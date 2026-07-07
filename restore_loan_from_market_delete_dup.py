#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从 repair_loan_app_no_market 的 REPAIR 日志恢复 delete_dup 误删行。

误删场景（旧 delete_dup）:
  删掉了 loan_no 正确、application_no 错误的那行
  留下 loan_no 错误、application_no 正确的那行

恢复策略（只 UPDATE，不 DELETE）:
  1. 若 correct loan_no 行还在且 app=bad → UPDATE application_no 为 good
  2. 若 good application_no 行在错误 loan_no 上 → UPDATE loan_no 为 correct
  3. 若已是 correct loan_no + good app → skip

Usage:
  python3 restore_loan_from_market_delete_dup.py --env ./ng_migration.env \\
    --repair-log /tmp/repair_loan_app_no_market_20260707_035300.csv --dry-run

  python3 restore_loan_from_market_delete_dup.py --env ./ng_migration.env \\
    --repair-log /tmp/repair_loan_app_no_market_20260707_035300.csv \\
    --sql-out /tmp/restore_delete_dup.sql

  python3 restore_loan_from_market_delete_dup.py --env ./ng_migration.env \\
    --repair-log /tmp/repair_loan_app_no_market_20260707_035300.csv --apply
"""
import argparse
import csv
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pymysql
from pymysql.cursors import DictCursor

from repair_loan_no_from_audit import CommitTracker, exec_with_retry

HERE = Path(__file__).resolve().parent
RESTORE_ACTIONS = frozenset({"delete_dup"})
WORKER_REPAIR_RE = re.compile(r"\.w\d+\.csv$", re.I)


def load_env(path: Path) -> Dict[str, str]:
    cfg: Dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        cfg[k.strip()] = v.strip().strip("'\"")
    return cfg


def connect_target(cfg: Dict[str, str]):
    return pymysql.connect(
        host=cfg["TARGET_HOST"],
        port=int(cfg.get("TARGET_PORT", "3306")),
        user=cfg["TARGET_USER"],
        password=cfg["TARGET_PASSWORD"],
        database=cfg.get("TARGET_DB", "ng"),
        charset="utf8mb4",
        cursorclass=DictCursor,
        read_timeout=3600,
        write_timeout=3600,
        autocommit=False,
    )


def _sql_escape(s: str) -> str:
    return str(s).replace("\\", "\\\\").replace("'", "''")


def discover_repair_logs(repair_log: str) -> List[str]:
    p = Path(repair_log)
    out: List[str] = []
    if p.is_file():
        out.append(str(p))
    suffix = p.suffix or ".csv"
    for f in sorted(p.parent.glob("%s.w*%s" % (p.stem, suffix))):
        if WORKER_REPAIR_RE.search(f.name):
            out.append(str(f))
    return out


def parse_market_repair_line(line: str) -> Optional[dict]:
    line = line.strip()
    if not line or line.startswith("ts,"):
        return None
    if line.startswith("REPAIR "):
        line = line[len("REPAIR ") :]
    row = next(csv.reader([line]))
    if len(row) < 8:
        return None
    return {
        "ts": row[0].strip(),
        "action": row[1].strip(),
        "loan_no": row[2].strip(),
        "bad_application_no": row[3].strip(),
        "good_application_no": row[4].strip(),
        "market_no": row[5].strip(),
        "app_id": row[6].strip(),
        "result": row[7].strip(),
    }


def load_delete_dup_plan(paths: List[str]) -> Tuple[List[dict], Dict[str, int]]:
    """按 loan_no 去重，保留时间最新的一条 delete_dup。"""
    by_loan: Dict[str, dict] = {}
    stats: Dict[str, int] = {"files": 0, "lines": 0}
    for path in paths:
        p = Path(path)
        if not p.is_file():
            stats["missing_files"] = stats.get("missing_files", 0) + 1
            continue
        stats["files"] += 1
        with p.open("r", encoding="utf-8") as fp:
            for raw in fp:
                rec = parse_market_repair_line(raw)
                if not rec:
                    continue
                stats["lines"] += 1
                action = rec["action"]
                stats[action] = stats.get(action, 0) + 1
                if action not in RESTORE_ACTIONS:
                    continue
                if not rec["loan_no"] or not rec["good_application_no"]:
                    stats["skip_incomplete"] = stats.get("skip_incomplete", 0) + 1
                    continue
                prev = by_loan.get(rec["loan_no"])
                if not prev or rec["ts"] >= prev["ts"]:
                    by_loan[rec["loan_no"]] = rec
    plan = sorted(by_loan.values(), key=lambda r: r["loan_no"])
    stats["delete_dup_unique"] = len(plan)
    return plan, stats


def fetch_loan_by_loan_no(tgt, loan_no: str) -> Optional[dict]:
    with tgt.cursor() as cur:
        cur.execute(
            """
            SELECT loan_no, application_no, period, roll_sequence
            FROM loan WHERE loan_no=%s LIMIT 1
            """,
            (loan_no,),
        )
        return cur.fetchone()


def fetch_loans_by_application_no(tgt, app_no: str) -> List[dict]:
    with tgt.cursor() as cur:
        cur.execute(
            """
            SELECT loan_no, application_no, period, roll_sequence
            FROM loan WHERE application_no=%s
            """,
            (app_no,),
        )
        return list(cur.fetchall())


def decide_recovery(rec: dict, row_at_correct: Optional[dict], rows_at_good: List[dict]) -> Tuple[str, dict]:
    correct_ln = rec["loan_no"]
    good = rec["good_application_no"]
    bad = rec["bad_application_no"]

    if row_at_correct:
        cur_app = str(row_at_correct["application_no"])
        if cur_app == good:
            return "already_ok", {}
        if cur_app == bad:
            return "fix_app_no", {
                "loan_no": correct_ln,
                "from_app": bad,
                "to_app": good,
            }

    if len(rows_at_good) == 1:
        g = rows_at_good[0]
        wrong_ln = str(g["loan_no"])
        if wrong_ln == correct_ln:
            return "already_ok", {}
        occupant = row_at_correct
        if occupant and str(occupant["application_no"]) != good:
            return "blocked_loan_no_taken", {
                "correct_loan_no": correct_ln,
                "occupant_app": str(occupant["application_no"]),
                "good_app_row": wrong_ln,
            }
        return "fix_loan_no", {
            "from_loan_no": wrong_ln,
            "to_loan_no": correct_ln,
            "application_no": good,
            "period": g.get("period", 1),
            "roll_sequence": g.get("roll_sequence", 0),
        }

    if len(rows_at_good) > 1:
        return "ambiguous_good_app", {"count": len(rows_at_good)}

    if row_at_correct:
        return "unexpected_state", {
            "loan_no": correct_ln,
            "application_no": str(row_at_correct["application_no"]),
        }

    return "missing_both", {}


def apply_recovery(
    tgt, action: str, payload: dict, dry_run: bool, tracker: Optional[CommitTracker]
) -> str:
    if action in ("already_ok", "blocked_loan_no_taken", "ambiguous_good_app", "missing_both", "unexpected_state"):
        return action

    if action == "fix_app_no":
        sql = (
            "UPDATE loan SET application_no=%s "
            "WHERE loan_no=%s AND application_no=%s"
        )
        params = (payload["to_app"], payload["loan_no"], payload["from_app"])
    elif action == "fix_loan_no":
        sql = (
            "UPDATE loan SET loan_no=%s "
            "WHERE loan_no=%s AND application_no=%s "
            "AND period=%s AND roll_sequence=%s"
        )
        params = (
            payload["to_loan_no"],
            payload["from_loan_no"],
            payload["application_no"],
            payload["period"],
            payload["roll_sequence"],
        )
    else:
        return "skip_unknown_action"

    if dry_run:
        return action

    with tgt.cursor() as cur:
        cur.execute(sql, params)
        if not cur.rowcount:
            return "skip_no_row"
    if tracker:
        tracker.note_write()
    else:
        tgt.commit()
    return action


def write_sql_out(plan_actions: List[Tuple[dict, str, dict]], out_path: Path) -> None:
    lines: List[str] = ["-- restore delete_dup from repair_loan_app_no_market REPAIR log", ""]
    for rec, action, payload in plan_actions:
        if action == "fix_app_no":
            lines.append(
                "UPDATE loan SET application_no='%s' WHERE loan_no='%s' AND application_no='%s';"
                % (
                    _sql_escape(payload["to_app"]),
                    _sql_escape(payload["loan_no"]),
                    _sql_escape(payload["from_app"]),
                )
            )
        elif action == "fix_loan_no":
            lines.append(
                "UPDATE loan SET loan_no='%s' WHERE loan_no='%s' AND application_no='%s' "
                "AND period=%s AND roll_sequence=%s;"
                % (
                    _sql_escape(payload["to_loan_no"]),
                    _sql_escape(payload["from_loan_no"]),
                    _sql_escape(payload["application_no"]),
                    int(payload["period"]),
                    int(payload["roll_sequence"]),
                )
            )
        else:
            lines.append(
                "-- skip %s loan_no=%s good=%s reason=%s payload=%s"
                % (
                    action,
                    rec["loan_no"],
                    rec["good_application_no"],
                    action,
                    payload,
                )
            )
        lines.append("")
    out_path.write_text("\n".join(lines), encoding="utf-8")


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Restore loan rows wrongly deleted by repair_loan_app_no_market delete_dup"
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--repair-log", required=True, help="repair_loan_app_no_market REPAIR csv")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--sql-out", default="", help="导出 SQL 到文件（IDEA 执行）")
    p.add_argument("--commit-every", type=int, default=20)
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    dry_run = not args.apply

    paths = discover_repair_logs(args.repair_log)
    if not paths:
        print("no repair log files found for %s" % args.repair_log, flush=True)
        return 1
    print("repair_logs=%s" % paths, flush=True)

    plan, stats = load_delete_dup_plan(paths)
    print("load_stats=%s delete_dup_unique=%s" % (stats, len(plan)), flush=True)
    if not plan:
        print("no delete_dup rows in logs", flush=True)
        return 0

    cfg = load_env(Path(args.env))
    tgt = connect_target(cfg)
    plan_actions: List[Tuple[dict, str, dict]] = []
    counts: Dict[str, int] = {}
    try:
        for rec in plan:
            row_at_correct = exec_with_retry(
                tgt,
                lambda ln=rec["loan_no"]: fetch_loan_by_loan_no(tgt, ln),
                "fetch loan_no=%s" % rec["loan_no"],
            )
            rows_at_good = exec_with_retry(
                tgt,
                lambda g=rec["good_application_no"]: fetch_loans_by_application_no(tgt, g),
                "fetch app=%s" % rec["good_application_no"],
            )
            action, payload = decide_recovery(rec, row_at_correct, rows_at_good)
            plan_actions.append((rec, action, payload))
            counts[action] = counts.get(action, 0) + 1
            print(
                "  %s action=%s correct_ln=%s good=%s payload=%s"
                % (
                    rec["loan_no"],
                    action,
                    rec["loan_no"],
                    rec["good_application_no"],
                    payload or "-",
                ),
                flush=True,
            )
    finally:
        tgt.close()

    print("diagnose=%s" % counts, flush=True)

    if args.sql_out:
        write_sql_out(plan_actions, Path(args.sql_out))
        print("wrote sql -> %s" % args.sql_out, flush=True)
        if dry_run and not args.apply:
            return 0

    if dry_run:
        print("dry-run done (use --apply or --sql-out)", flush=True)
        return 0

    tgt = connect_target(cfg)
    tracker = CommitTracker(tgt, args.commit_every, dry_run=False)
    ok = skip = 0
    try:
        for rec, action, payload in plan_actions:
            if action in (
                "already_ok",
                "blocked_loan_no_taken",
                "ambiguous_good_app",
                "missing_both",
                "unexpected_state",
                "skip_no_row",
                "skip_unknown_action",
            ):
                skip += 1
                continue
            st = exec_with_retry(
                tgt,
                lambda a=action, pl=payload: apply_recovery(
                    tgt, a, pl, False, tracker
                ),
                "restore %s %s" % (rec["loan_no"], action),
            )
            if st in ("fix_app_no", "fix_loan_no"):
                ok += 1
                print(
                    "RESTORE ok %s %s -> %s"
                    % (rec["loan_no"], action, payload),
                    flush=True,
                )
            else:
                skip += 1
        tracker.flush()
    finally:
        tgt.close()

    print("done ok=%s skip=%s" % (ok, skip), flush=True)
    if counts.get("blocked_loan_no_taken") or counts.get("missing_both"):
        print(
            "manual_review: blocked_loan_no_taken=%s missing_both=%s"
            % (
                counts.get("blocked_loan_no_taken", 0),
                counts.get("missing_both", 0),
            ),
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
