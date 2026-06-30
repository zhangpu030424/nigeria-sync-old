#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Target-only backfill for application_no / loan_no (no source DB).

New format (suffix = old numeric application_no):
  application_no = ng{app_id:04d}-{old_application_no}
  loan_no        = ng-{old_application_no}-01000

Use this when SQL batch UPDATE hits proxy timeout (~60s on :8001).
Each row is one short UPDATE + commit. Paginate by primary key (no full-table COUNT).

Usage:
  python3 backfill_order_nos_target_only.py --env ./ng_migration.env --dry-run --tables loan
  python3 backfill_order_nos_target_only.py --env ./ng_migration.env --apply --tables loan
  python3 backfill_order_nos_target_only.py --env ./ng_migration.env --apply --tables application
"""
import argparse
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pymysql
from pymysql.cursors import DictCursor

HERE = Path(__file__).resolve().parent


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


def new_application_no(app_id, old_no: str) -> str:
    return "ng%04d-%s" % (int(app_id), old_no)


def new_loan_no(old_app_no: str) -> str:
    return "ng-%s-01000" % old_app_no


def needs_backfill_app_no(app_no: str) -> bool:
    return bool(app_no) and not str(app_no).startswith("ng")


def exec_with_retry(tgt, fn, what: str):
    for attempt in range(5):
        try:
            return fn()
        except pymysql.Error as exc:
            try:
                tgt.rollback()
            except Exception:
                pass
            if attempt >= 4:
                raise
            print("%s retry err=%s" % (what, exc), flush=True)
            tgt.ping(reconnect=True)
            time.sleep(2)
    return None


def scan_loan_rows(tgt, after_loan_no: str, scan_limit: int) -> List[dict]:
    """Scan loan by PK; filter old-format rows in Python (uses loan_no index)."""
    sql = """
        SELECT l.loan_no, l.application_no, a.app_id
        FROM loan l
        INNER JOIN application a ON l.application_no = a.application_no
        WHERE l.loan_no > %s AND a.app_id IS NOT NULL
        ORDER BY l.loan_no ASC
        LIMIT %s
    """
    with tgt.cursor() as cur:
        cur.execute(sql, (after_loan_no or "", scan_limit))
        return list(cur.fetchall())


def scan_application_rows(tgt, after_app_no: str, scan_limit: int) -> List[dict]:
    sql = """
        SELECT application_no, app_id
        FROM application
        WHERE application_no > %s AND app_id IS NOT NULL
        ORDER BY application_no ASC
        LIMIT %s
    """
    with tgt.cursor() as cur:
        cur.execute(sql, (after_app_no or "", scan_limit))
        return list(cur.fetchall())


def loan_exists(tgt, loan_no: str) -> bool:
    with tgt.cursor() as cur:
        cur.execute("SELECT 1 FROM loan WHERE loan_no=%s LIMIT 1", (loan_no,))
        return cur.fetchone() is not None


def application_exists(tgt, app_no: str) -> bool:
    with tgt.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM application WHERE application_no=%s LIMIT 1", (app_no,)
        )
        return cur.fetchone() is not None


def update_one_loan(tgt, row: dict, dry_run: bool) -> str:
    old_loan_no = str(row["loan_no"])
    old_app_no = str(row["application_no"])
    new_app = new_application_no(row["app_id"], old_app_no)
    new_loan = new_loan_no(old_app_no)
    if new_app == old_app_no and new_loan == old_loan_no:
        return "skip"
    if loan_exists(tgt, new_loan) and new_loan != old_loan_no:
        return "conflict_loan_no"
    if dry_run:
        return "ok"
    with tgt.cursor() as cur:
        cur.execute(
            "UPDATE loan SET loan_no=%s, application_no=%s WHERE loan_no=%s",
            (new_loan, new_app, old_loan_no),
        )
        if not cur.rowcount:
            return "missing"
    tgt.commit()
    return "ok"


def update_one_application(tgt, row: dict, dry_run: bool) -> str:
    old_no = str(row["application_no"])
    new_no = new_application_no(row["app_id"], old_no)
    if new_no == old_no:
        return "skip"
    if application_exists(tgt, new_no):
        return "conflict_app_no"
    if dry_run:
        return "ok"
    with tgt.cursor() as cur:
        cur.execute(
            "UPDATE application SET application_no=%s WHERE application_no=%s",
            (new_no, old_no),
        )
        if not cur.rowcount:
            return "missing"
    tgt.commit()
    return "ok"


def run_loans(
    tgt, dry_run: bool, scan_size: int, work_limit: int, log_every: int
) -> Tuple[int, int]:
    ok = skip = scanned = 0
    after = ""
    batch_no = 0
    while True:
        batch_no += 1
        rows = exec_with_retry(
            tgt,
            lambda: scan_loan_rows(tgt, after, scan_size),
            "loan scan after=%s" % after,
        )
        if not rows:
            break
        after = str(rows[-1]["loan_no"])
        scanned += len(rows)
        todo = [r for r in rows if needs_backfill_app_no(r["application_no"])]
        for row in todo:
            loan_no = str(row["loan_no"])
            status = exec_with_retry(
                tgt,
                lambda r=row: update_one_loan(tgt, r, dry_run),
                "loan update loan_no=%s" % loan_no,
            )
            if status == "ok":
                ok += 1
            else:
                skip += 1
            if work_limit and ok >= work_limit:
                print("loan stop work_limit=%s" % work_limit, flush=True)
                return ok, skip
            if (ok + skip) and (ok + skip) % log_every == 0:
                print(
                    "loan progress ok=%s skip=%s scanned=%s last_loan_no=%s"
                    % (ok, skip, scanned, loan_no),
                    flush=True,
                )
        print(
            "loan scan_batch=%s scanned=%s todo=%s ok=%s skip=%s after=%s"
            % (batch_no, len(rows), len(todo), ok, skip, after),
            flush=True,
        )
        if len(rows) < scan_size:
            break
    return ok, skip


def run_applications(
    tgt, dry_run: bool, scan_size: int, work_limit: int, log_every: int
) -> Tuple[int, int]:
    ok = skip = scanned = 0
    after = ""
    batch_no = 0
    while True:
        batch_no += 1
        rows = exec_with_retry(
            tgt,
            lambda: scan_application_rows(tgt, after, scan_size),
            "application scan after=%s" % after,
        )
        if not rows:
            break
        after = str(rows[-1]["application_no"])
        scanned += len(rows)
        todo = [r for r in rows if needs_backfill_app_no(r["application_no"])]
        for row in todo:
            app_no = str(row["application_no"])
            status = exec_with_retry(
                tgt,
                lambda r=row: update_one_application(tgt, r, dry_run),
                "application update app_no=%s" % app_no,
            )
            if status == "ok":
                ok += 1
            else:
                skip += 1
            if work_limit and ok >= work_limit:
                print("application stop work_limit=%s" % work_limit, flush=True)
                return ok, skip
            if (ok + skip) and (ok + skip) % log_every == 0:
                print(
                    "application progress ok=%s skip=%s scanned=%s last_app_no=%s"
                    % (ok, skip, scanned, app_no),
                    flush=True,
                )
        print(
            "application scan_batch=%s scanned=%s todo=%s ok=%s skip=%s after=%s"
            % (batch_no, len(rows), len(todo), ok, skip, after),
            flush=True,
        )
        if len(rows) < scan_size:
            break
    return ok, skip


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Target-only order no backfill (row by row)")
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true", help="write changes (default: dry-run)")
    p.add_argument("--dry-run", action="store_true", help="preview only (default)")
    p.add_argument(
        "--tables",
        choices=["loan", "application", "all"],
        default="all",
        help="loan must run before application when using all",
    )
    p.add_argument(
        "--scan-size",
        type=int,
        default=500,
        help="rows scanned per batch via PK pagination (default 500)",
    )
    p.add_argument(
        "--work-limit",
        type=int,
        default=0,
        help="stop after N successful updates (0 = no limit; dry-run test)",
    )
    p.add_argument("--log-every", type=int, default=100)
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run, not both")
    dry_run = not args.apply

    cfg = load_env(Path(args.env))
    tgt = connect_target(cfg)
    try:
        print(
            "start dry_run=%s tables=%s scan_size=%s work_limit=%s (no full-table COUNT)"
            % (dry_run, args.tables, args.scan_size, args.work_limit),
            flush=True,
        )
        if args.tables in ("loan", "all"):
            ok, skip = run_loans(
                tgt, dry_run, args.scan_size, args.work_limit, args.log_every
            )
            print("loan done ok=%s skip=%s" % (ok, skip), flush=True)
        if args.tables in ("application", "all"):
            ok, skip = run_applications(
                tgt, dry_run, args.scan_size, args.work_limit, args.log_every
            )
            print("application done ok=%s skip=%s" % (ok, skip), flush=True)
        print("finished", flush=True)
        return 0
    finally:
        tgt.close()


if __name__ == "__main__":
    raise SystemExit(main())
