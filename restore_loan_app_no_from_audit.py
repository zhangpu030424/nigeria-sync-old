#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从 repair_loan_status20 审计 CSV 恢复 loan.application_no。

repair_loan_status20 误走 delete_long 时：删了 application_no 正确的长号行，
留下 loan_no 已对、application_no 错的短号行。本脚本读审计里记录的
correct_loan_no + application_no，写回目标库。

优先读主日志:
  /tmp/repair_loan_status20_YYYYMMDD_HHMMSS.csv
  action in (drop_long, rekey_long, rekey_keep_app_no)
  → correct_loan_no + application_no

补充读 deleted 侧车:
  *.deleted.csv  action=delete_long

Usage:
  python3 restore_loan_app_no_from_audit.py \\
    --env ./ng_migration.env \\
    --audit /tmp/repair_loan_status20_20260706_093302.csv \\
    --dry-run

  python3 restore_loan_app_no_from_audit.py \\
    --env ./ng_migration.env \\
    --audit /tmp/repair_loan_status20_20260706_093302.csv \\
    --apply --commit-every 50
"""
import argparse
import csv
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pymysql
from pymysql.cursors import DictCursor

from repair_loan_no_from_audit import CommitTracker, exec_with_retry

HERE = Path(__file__).resolve().parent
REPAIR_HEADER = (
    "ts",
    "action",
    "wrong_loan_no",
    "correct_loan_no",
    "legacy_loan_no",
    "application_no",
    "app_id",
    "result",
)
DELETED_HEADER = ("ts", "action", "loan_no", "application_no", "row_json")
RESTORE_ACTIONS = frozenset(
    {
        "drop_long",
        "rekey_long",
        "rekey_keep_app_no",
        "would_drop_long",
        "would_rekey_long",
        "would_rekey_keep_app_no",
    }
)
APP_SUFFIX_RE = re.compile(r"^ng\d+-(.+)$", re.I)


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


def deleted_sidecar(audit_path: str) -> str:
    p = Path(audit_path)
    return str(p.with_name("%s.deleted%s" % (p.stem, p.suffix or ".csv")))


def market_suffix(application_no: str) -> str:
    m = APP_SUFFIX_RE.match(str(application_no or "").strip())
    return m.group(1).strip() if m else ""


def load_plan_from_repair_csv(audit_path: str) -> Tuple[Dict[str, str], Dict[str, int]]:
    """loan_no -> want application_no"""
    plan: Dict[str, str] = {}
    stats: Dict[str, int] = {"lines": 0}
    path = Path(audit_path)
    if not path.is_file():
        raise FileNotFoundError("audit not found: %s" % audit_path)
    with path.open("r", encoding="utf-8", newline="") as fp:
        for raw in fp:
            line = raw.strip()
            if not line:
                continue
            if line.startswith("REPAIR_AUDIT "):
                line = line[len("REPAIR_AUDIT ") :]
            row = next(csv.reader([line]))
            if row[0] == "ts":
                continue
            if len(row) < 8:
                continue
            stats["lines"] += 1
            rec = dict(zip(REPAIR_HEADER, row[:8]))
            action = rec.get("action", "").strip()
            stats[action] = stats.get(action, 0) + 1
            if action not in RESTORE_ACTIONS:
                continue
            loan_no = str(rec.get("correct_loan_no") or "").strip()
            app_no = str(rec.get("application_no") or "").strip()
            if not loan_no or not app_no:
                stats["skip_incomplete"] = stats.get("skip_incomplete", 0) + 1
                continue
            plan[loan_no] = app_no
    return plan, stats


def load_plan_from_deleted_csv(deleted_path: str) -> Dict[str, str]:
    """market_suffix -> application_no（delete_long 被删长号行上的正确 app_no）"""
    by_market: Dict[str, str] = {}
    path = Path(deleted_path)
    if not path.is_file():
        return by_market
    with path.open("r", encoding="utf-8", newline="") as fp:
        for raw in fp:
            line = raw.strip()
            if not line:
                continue
            if line.startswith("ROW_DELETED "):
                line = line[len("ROW_DELETED ") :]
            row = next(csv.reader([line]))
            if row[0] == "ts":
                continue
            if len(row) < 5:
                continue
            rec = dict(zip(DELETED_HEADER, row[:5]))
            if rec.get("action") != "delete_long":
                continue
            app_no = str(rec.get("application_no") or "").strip()
            suffix = market_suffix(app_no)
            if app_no and suffix:
                by_market[suffix] = app_no
    return by_market


def enrich_plan_from_deleted(
    tgt, by_market: Dict[str, str], plan: Dict[str, str]
) -> int:
    """对主日志未覆盖的短号行，按 market 后缀从 deleted 审计补全。"""
    if not by_market:
        return 0
    added = 0
    suffixes = sorted(by_market.keys())
    for i in range(0, len(suffixes), 200):
        part = suffixes[i : i + 200]
        cond = " OR ".join(
            ["application_no LIKE %s"] * len(part)
        )
        params = ["ng%%-%s" % s for s in part]
        with tgt.cursor() as cur:
            cur.execute(
                """
                SELECT loan_no, application_no
                FROM loan
                WHERE (%s)
                  AND loan_no REGEXP '^[Nn][Gg]-[0-9]+-[0-9]{5}$'
                """
                % cond,
                params,
            )
            rows = list(cur.fetchall())
        for row in rows:
            ln = str(row["loan_no"])
            if ln in plan:
                continue
            suffix = market_suffix(str(row["application_no"]))
            want = by_market.get(suffix, "")
            if want and want != str(row["application_no"]):
                plan[ln] = want
                added += 1
    return added


def fetch_current_app_no(tgt, loan_no: str) -> str:
    with tgt.cursor() as cur:
        cur.execute(
            "SELECT application_no FROM loan WHERE loan_no=%s LIMIT 1",
            (loan_no,),
        )
        row = cur.fetchone()
    return str(row["application_no"]).strip() if row and row.get("application_no") else ""


def loan_pk_exists(tgt, app_no: str, loan_no: str) -> bool:
    with tgt.cursor() as cur:
        cur.execute(
            """
            SELECT period, roll_sequence FROM loan WHERE loan_no=%s LIMIT 1
            """,
            (loan_no,),
        )
        row = cur.fetchone()
    if not row:
        return False
    with tgt.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM loan
            WHERE application_no=%s AND period=%s AND roll_sequence=%s
              AND loan_no <> %s
            LIMIT 1
            """,
            (app_no, row["period"], row["roll_sequence"], loan_no),
        )
        return cur.fetchone() is not None


def apply_one(
    tgt, loan_no: str, want_app: str, dry_run: bool, tracker: Optional[CommitTracker]
) -> str:
    current = fetch_current_app_no(tgt, loan_no)
    if not current:
        return "skip_missing"
    if current == want_app:
        return "skip_ok"
    if loan_pk_exists(tgt, want_app, loan_no):
        print(
            "skip pk_conflict loan_no=%s want=%s current=%s"
            % (loan_no, want_app, current),
            flush=True,
        )
        return "skip_pk"
    if dry_run:
        print(
            "would_restore loan_no=%s  %s -> %s"
            % (loan_no, current, want_app),
            flush=True,
        )
        return "ok"
    with tgt.cursor() as cur:
        cur.execute(
            """
            UPDATE loan SET application_no=%s
            WHERE loan_no=%s AND application_no=%s
            """,
            (want_app, loan_no, current),
        )
        if not cur.rowcount:
            return "skip_no_row"
    if tracker:
        tracker.note_write()
    else:
        tgt.commit()
    return "ok"


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Restore loan.application_no from repair_loan_status20 audit CSV"
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--audit", required=True, help="repair_loan_status20 主审计 CSV 路径")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--use-deleted",
        action="store_true",
        help="主日志未命中时，用 *.deleted.csv 的 delete_long 按 market 后缀补全",
    )
    p.add_argument("--commit-every", type=int, default=50)
    p.add_argument("--log-every", type=int, default=100)
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    dry_run = not args.apply

    plan, stats = load_plan_from_repair_csv(args.audit)
    print("audit=%s repair_stats=%s plan_from_repair=%s" % (args.audit, stats, len(plan)), flush=True)

    cfg = load_env(Path(args.env))
    tgt = connect_target(cfg)
    try:
        if args.use_deleted:
            deleted = deleted_sidecar(args.audit)
            by_market = load_plan_from_deleted_csv(deleted)
            added = enrich_plan_from_deleted(tgt, by_market, plan)
            print(
                "deleted_sidecar=%s market_map=%s plan_added=%s total_plan=%s"
                % (deleted, len(by_market), added, len(plan)),
                flush=True,
            )
        items = sorted(plan.items())
        print("restore_plan=%s dry_run=%s" % (len(items), dry_run), flush=True)
        for loan_no, app_no in items[:20]:
            print("  %s -> %s" % (loan_no, app_no), flush=True)
        if len(items) > 20:
            print("  ... and %s more" % (len(items) - 20), flush=True)
        if not items:
            print("nothing to restore", flush=True)
            return 0

        ok = skip = 0
        tracker = CommitTracker(tgt, args.commit_every, dry_run)
        for i, (loan_no, want_app) in enumerate(items, 1):
            st = exec_with_retry(
                tgt,
                lambda ln=loan_no, wa=want_app: apply_one(
                    tgt, ln, wa, dry_run, tracker
                ),
                "restore %s" % loan_no,
            )
            if st == "ok":
                ok += 1
            else:
                skip += 1
            if i % max(1, args.log_every) == 0:
                print(
                    "progress ok=%s skip=%s last=%s" % (ok, skip, loan_no),
                    flush=True,
                )
        tracker.flush()
        print("done ok=%s skip=%s" % (ok, skip), flush=True)
        return 0
    finally:
        tgt.close()


if __name__ == "__main__":
    raise SystemExit(main())
