#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从 repair_loan_status20 审计 CSV 恢复 loan.application_no。

并行 --workers N 时主 CSV 常为空，真实记录在:
  /tmp/repair_loan_status20_xxx.w0.csv ... .w9.csv
  /tmp/repair_loan_status20_xxx.w0.deleted.csv ...（delete_long 含正确 application_no）

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
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pymysql
from pymysql.cursors import DictCursor

from repair_loan_no_from_audit import CommitTracker, exec_with_retry

HERE = Path(__file__).resolve().parent
LOAN_NO_RE = re.compile(r"^[Nn][Gg]-(\d+)-(\d{5})$")
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
TS_LEN = 19  # YYYY-MM-DD HH:MM:SS


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


def discover_repair_logs(audit_path: str) -> List[str]:
    p = Path(audit_path)
    out: List[str] = []
    if p.is_file():
        out.append(str(p))
    suffix = p.suffix or ".csv"
    for f in sorted(p.parent.glob("%s.w*%s" % (p.stem, suffix))):
        out.append(str(f))
    return out


def discover_deleted_logs(audit_path: str) -> List[str]:
    p = Path(audit_path)
    suffix = p.suffix or ".csv"
    patterns = [
        "%s.deleted%s" % (p.stem, suffix),
        "%s.w*.deleted%s" % (p.stem, suffix),
    ]
    out: List[str] = []
    seen = set()
    for pat in patterns:
        for f in sorted(p.parent.glob(pat)):
            s = str(f)
            if s not in seen:
                seen.add(s)
                out.append(s)
    return out


def market_suffix(application_no: str) -> str:
    m = APP_SUFFIX_RE.match(str(application_no or "").strip())
    return m.group(1).strip() if m else ""


def is_short_loan_no(loan_no: str, min_sn_len: int = 15) -> bool:
    m = LOAN_NO_RE.match(str(loan_no or "").strip())
    return bool(m and len(m.group(1)) < min_sn_len)


def parse_repair_line(line: str) -> Optional[dict]:
    line = line.strip()
    if not line or line.startswith("ts,"):
        return None
    if line.startswith("REPAIR_AUDIT "):
        line = line[len("REPAIR_AUDIT ") :]
    row = next(csv.reader([line]))
    if len(row) < 8:
        return None
    return {
        "ts": row[0],
        "action": row[1].strip(),
        "wrong_loan_no": row[2].strip(),
        "correct_loan_no": row[3].strip(),
        "legacy_loan_no": row[4].strip(),
        "application_no": row[5].strip(),
        "app_id": row[6].strip(),
        "result": ",".join(row[7:]).strip(),
    }


def parse_deleted_line(line: str) -> Optional[dict]:
    line = line.strip()
    if not line or line.startswith("ts,"):
        return None
    if line.startswith("ROW_DELETED "):
        line = line[len("ROW_DELETED ") :]
    json_start = line.find("{")
    if json_start < 0:
        return None
    prefix = line[:json_start].rstrip(",")
    row_json = line[json_start:]
    parts = prefix.split(",")
    if len(parts) < 4:
        if len(line) > TS_LEN and line[TS_LEN] == ",":
            ts = line[:TS_LEN]
            rest = line[TS_LEN + 1 :]
            json_start = rest.find("{")
            if json_start < 0:
                return None
            parts = rest[:json_start].rstrip(",").split(",")
            if len(parts) < 3:
                return None
            return {
                "ts": ts,
                "action": parts[0].strip(),
                "loan_no": parts[1].strip(),
                "application_no": parts[2].strip(),
                "row_json": rest[json_start:],
            }
        return None
    return {
        "ts": parts[0].strip(),
        "action": parts[1].strip(),
        "loan_no": parts[2].strip(),
        "application_no": parts[3].strip(),
        "row_json": row_json,
    }


def load_plan_from_repair_files(paths: List[str]) -> Tuple[Dict[str, str], Dict[str, int]]:
    plan: Dict[str, str] = {}
    stats: Dict[str, int] = {"files": 0, "lines": 0}
    for path in paths:
        p = Path(path)
        if not p.is_file():
            stats["missing_files"] = stats.get("missing_files", 0) + 1
            continue
        stats["files"] += 1
        with p.open("r", encoding="utf-8") as fp:
            for raw in fp:
                rec = parse_repair_line(raw)
                if not rec:
                    continue
                stats["lines"] += 1
                action = rec["action"]
                stats[action] = stats.get(action, 0) + 1
                if action not in RESTORE_ACTIONS:
                    continue
                loan_no = rec["correct_loan_no"]
                app_no = rec["application_no"]
                if not loan_no or not app_no:
                    stats["skip_incomplete"] = stats.get("skip_incomplete", 0) + 1
                    continue
                plan[loan_no] = app_no
    return plan, stats


def load_market_map_from_deleted_files(paths: List[str]) -> Tuple[Dict[str, str], Dict[str, int]]:
    """market_suffix -> 正确 application_no（来自 delete_long 被删长号行）。"""
    by_market: Dict[str, str] = {}
    stats: Dict[str, int] = {"files": 0, "lines": 0, "delete_long": 0}
    for path in paths:
        p = Path(path)
        if not p.is_file():
            continue
        stats["files"] += 1
        with p.open("r", encoding="utf-8") as fp:
            for raw in fp:
                rec = parse_deleted_line(raw)
                if not rec:
                    continue
                stats["lines"] += 1
                if rec["action"] != "delete_long":
                    continue
                stats["delete_long"] += 1
                app_no = rec["application_no"]
                suffix = market_suffix(app_no)
                if app_no and suffix:
                    by_market[suffix] = app_no
    return by_market, stats


def enrich_plan_from_market_map(
    tgt, by_market: Dict[str, str], plan: Dict[str, str], min_sn_len: int
) -> int:
    if not by_market:
        return 0
    added = 0
    suffixes = sorted(by_market.keys())
    for i in range(0, len(suffixes), 200):
        part = suffixes[i : i + 200]
        cond = " OR ".join(["application_no LIKE %s"] * len(part))
        params = ["ng%%-%s" % s for s in part]
        with tgt.cursor() as cur:
            cur.execute(
                """
                SELECT loan_no, application_no
                FROM loan
                WHERE (%s)
                """
                % cond,
                params,
            )
            rows = list(cur.fetchall())
        for row in rows:
            ln = str(row["loan_no"])
            if not is_short_loan_no(ln, min_sn_len):
                continue
            if ln in plan:
                continue
            suffix = market_suffix(str(row["application_no"]))
            want = by_market.get(suffix, "")
            cur_app = str(row["application_no"])
            if want and want != cur_app:
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
            "SELECT period, roll_sequence FROM loan WHERE loan_no=%s LIMIT 1",
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
    p.add_argument("--audit", required=True, help="repair 主审计路径（自动找 .w0..wN 与 deleted）")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--no-deleted",
        action="store_true",
        help="不读 *.deleted.csv（默认会读 worker deleted 侧车）",
    )
    p.add_argument("--min-sn-len", type=int, default=15)
    p.add_argument("--commit-every", type=int, default=50)
    p.add_argument("--log-every", type=int, default=100)
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    dry_run = not args.apply

    repair_files = discover_repair_logs(args.audit)
    deleted_files = [] if args.no_deleted else discover_deleted_logs(args.audit)
    print("repair_files=%s" % repair_files, flush=True)
    print("deleted_files=%s" % deleted_files, flush=True)

    plan, repair_stats = load_plan_from_repair_files(repair_files)
    print(
        "repair_stats=%s plan_from_repair=%s"
        % (repair_stats, len(plan)),
        flush=True,
    )

    cfg = load_env(Path(args.env))
    tgt = connect_target(cfg)
    try:
        if deleted_files:
            by_market, del_stats = load_market_map_from_deleted_files(deleted_files)
            added = enrich_plan_from_market_map(
                tgt, by_market, plan, args.min_sn_len
            )
            print(
                "deleted_stats=%s market_map=%s plan_added=%s total_plan=%s"
                % (del_stats, len(by_market), added, len(plan)),
                flush=True,
            )

        items = sorted(plan.items())
        print("restore_plan=%s dry_run=%s" % (len(items), dry_run), flush=True)
        for loan_no, app_no in items[:20]:
            print("  %s -> %s" % (loan_no, app_no), flush=True)
        if len(items) > 20:
            print("  ... and %s more" % (len(items) - 20), flush=True)
        if not items:
            print(
                "nothing to restore; check: ls -la %s*"
                % Path(args.audit).with_suffix(""),
                flush=True,
            )
            return 1

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
