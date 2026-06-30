#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Target-only backfill for application_no / loan_no (no source DB).

New format (suffix = old numeric application_no):
  application_no = ng{app_id:04d}-{old_application_no}
  loan_no        = ng-{old_application_no}-01000

Use this when SQL batch UPDATE hits proxy timeout (~60s on :8001).
Each row is one short UPDATE + commit.

Usage:
  python3 backfill_order_nos_target_only.py --env ./ng_migration.env --dry-run
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


def count_old_loans(tgt) -> int:
    with tgt.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS c FROM loan WHERE application_no NOT LIKE 'ng%%'"
        )
        return int(cur.fetchone()["c"])


def count_old_applications(tgt) -> int:
    with tgt.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS c FROM application
            WHERE application_no NOT LIKE 'ng%%' AND app_id IS NOT NULL
            """
        )
        return int(cur.fetchone()["c"])


def fetch_loan_rows(tgt, limit: int, after_loan_no: str = "") -> List[dict]:
    sql = """
        SELECT l.loan_no, l.application_no, a.app_id
        FROM loan l
        INNER JOIN application a ON l.application_no = a.application_no
        WHERE l.application_no NOT LIKE 'ng%%'
          AND a.app_id IS NOT NULL
    """
    params: List = []
    if after_loan_no:
        sql += " AND l.loan_no > %s"
        params.append(after_loan_no)
    sql += " ORDER BY l.loan_no ASC LIMIT %s"
    params.append(limit)
    with tgt.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def fetch_application_rows(tgt, limit: int, after_app_no: str = "") -> List[dict]:
    sql = """
        SELECT application_no, app_id
        FROM application
        WHERE application_no NOT LIKE 'ng%%'
          AND app_id IS NOT NULL
    """
    params: List = []
    if after_app_no:
        sql += " AND application_no > %s"
        params.append(after_app_no)
    sql += " ORDER BY application_no ASC LIMIT %s"
    params.append(limit)
    with tgt.cursor() as cur:
        cur.execute(sql, params)
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


def run_loans(tgt, dry_run: bool, fetch_size: int, log_every: int) -> Tuple[int, int]:
    ok = skip = 0
    after = ""
    batch_no = 0
    while True:
        batch_no += 1
        rows = fetch_loan_rows(tgt, fetch_size, after)
        if not rows:
            break
        for row in rows:
            after = str(row["loan_no"])
            for attempt in range(5):
                try:
                    status = update_one_loan(tgt, row, dry_run)
                    break
                except pymysql.Error as exc:
                    tgt.rollback()
                    if attempt >= 4:
                        raise
                    print("loan retry loan_no=%s err=%s" % (after, exc), flush=True)
                    tgt.ping(reconnect=True)
                    time.sleep(2)
            if status == "ok":
                ok += 1
            else:
                skip += 1
            if (ok + skip) % log_every == 0:
                print(
                    "loan progress ok=%s skip=%s last=%s remaining~=%s"
                    % (ok, skip, after, count_old_loans(tgt)),
                    flush=True,
                )
        print(
            "loan batch=%s fetched=%s ok=%s skip=%s after=%s"
            % (batch_no, len(rows), ok, skip, after),
            flush=True,
        )
    return ok, skip


def run_applications(
    tgt, dry_run: bool, fetch_size: int, log_every: int
) -> Tuple[int, int]:
    ok = skip = 0
    after = ""
    batch_no = 0
    while True:
        batch_no += 1
        rows = fetch_application_rows(tgt, fetch_size, after)
        if not rows:
            break
        for row in rows:
            after = str(row["application_no"])
            for attempt in range(5):
                try:
                    status = update_one_application(tgt, row, dry_run)
                    break
                except pymysql.Error as exc:
                    tgt.rollback()
                    if attempt >= 4:
                        raise
                    print(
                        "application retry app_no=%s err=%s" % (after, exc), flush=True
                    )
                    tgt.ping(reconnect=True)
                    time.sleep(2)
            if status == "ok":
                ok += 1
            else:
                skip += 1
            if (ok + skip) % log_every == 0:
                print(
                    "application progress ok=%s skip=%s last=%s remaining~=%s"
                    % (ok, skip, after, count_old_applications(tgt)),
                    flush=True,
                )
        print(
            "application batch=%s fetched=%s ok=%s skip=%s after=%s"
            % (batch_no, len(rows), ok, skip, after),
            flush=True,
        )
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
    p.add_argument("--fetch-size", type=int, default=200)
    p.add_argument("--log-every", type=int, default=100)
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run, not both")
    dry_run = not args.apply

    cfg = load_env(Path(args.env))
    tgt = connect_target(cfg)
    try:
        print(
            "start dry_run=%s tables=%s old_loan=%s old_app=%s"
            % (
                dry_run,
                args.tables,
                count_old_loans(tgt),
                count_old_applications(tgt),
            ),
            flush=True,
        )
        if args.tables in ("loan", "all"):
            ok, skip = run_loans(tgt, dry_run, args.fetch_size, args.log_every)
            print("loan done ok=%s skip=%s remaining=%s" % (ok, skip, count_old_loans(tgt)))
        if args.tables in ("application", "all"):
            ok, skip = run_applications(tgt, dry_run, args.fetch_size, args.log_every)
            print(
                "application done ok=%s skip=%s remaining=%s"
                % (ok, skip, count_old_applications(tgt))
            )
        print(
            "finished old_loan=%s old_app=%s"
            % (count_old_loans(tgt), count_old_applications(tgt))
        )
        return 0
    finally:
        tgt.close()


if __name__ == "__main__":
    raise SystemExit(main())
