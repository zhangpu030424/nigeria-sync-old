#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按 loan 条件将对应 application.is_test 设为 1（JOIN 一次更新，无大 IN）。

等价:
  UPDATE application a
  INNER JOIN (
    SELECT DISTINCT application_no FROM loan
    WHERE due_date < '2026-07-05' AND status = 20
  ) l ON a.application_no = l.application_no
  SET a.is_test = 1;

Usage:
  python3 mark_application_is_test.py --env ./ng_migration.env --dry-run \\
    --due-before 2026-07-05 --status 20

  python3 mark_application_is_test.py --env ./ng_migration.env --apply \\
    --due-before 2026-07-05 --status 20
"""
import argparse
import time
from pathlib import Path
from typing import Dict, Optional

import pymysql
from pymysql.cursors import DictCursor

from repair_loan_no_from_audit import exec_with_retry

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


def count_plan(tgt, due_before: str, status: str, only_not_test: bool) -> Dict[str, int]:
    join_sql = """
        FROM application a
        INNER JOIN (
            SELECT DISTINCT application_no
            FROM loan
            WHERE due_date < %s AND status = %s
              AND application_no IS NOT NULL AND application_no <> ''
        ) l ON a.application_no = l.application_no
    """
    params = (due_before, status)
    out: Dict[str, int] = {}
    with tgt.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c " + join_sql, params)
        out["match_application"] = int(cur.fetchone()["c"])
        cur.execute(
            "SELECT COUNT(*) AS c "
            + join_sql
            + (" WHERE a.is_test IS NULL OR a.is_test <> 1" if only_not_test else ""),
            params,
        )
        out["would_update"] = int(cur.fetchone()["c"])
        cur.execute(
            """
            SELECT COUNT(DISTINCT application_no) AS c FROM loan
            WHERE due_date < %s AND status = %s
              AND application_no IS NOT NULL AND application_no <> ''
            """,
            params,
        )
        out["distinct_loan_app_no"] = int(cur.fetchone()["c"])
    out["missing_application"] = (
        out["distinct_loan_app_no"] - out["match_application"]
    )
    return out


def apply_update(tgt, due_before: str, status: str, only_not_test: bool) -> int:
    sql = """
        UPDATE application a
        INNER JOIN (
            SELECT DISTINCT application_no
            FROM loan
            WHERE due_date < %s AND status = %s
              AND application_no IS NOT NULL AND application_no <> ''
        ) l ON a.application_no = l.application_no
        SET a.is_test = 1
    """
    if only_not_test:
        sql += " WHERE a.is_test IS NULL OR a.is_test <> 1"
    with tgt.cursor() as cur:
        cur.execute(sql, (due_before, status))
        n = int(cur.rowcount or 0)
    tgt.commit()
    return n


def sample_app_nos(tgt, due_before: str, status: str, limit: int = 20):
    with tgt.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT application_no
            FROM loan
            WHERE due_date < %s AND status = %s
              AND application_no IS NOT NULL AND application_no <> ''
            ORDER BY application_no ASC
            LIMIT %s
            """,
            (due_before, status, limit),
        )
        return [str(r["application_no"]) for r in cur.fetchall()]


def main(argv: Optional[list] = None) -> int:
    p = argparse.ArgumentParser(description="Set application.is_test=1 via loan JOIN")
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--due-before", default="2026-07-05")
    p.add_argument("--status", default="20")
    p.add_argument(
        "--force",
        action="store_true",
        help="已是 is_test=1 的也计入（默认只更新非 1 的行）",
    )
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    dry_run = not args.apply
    only_not_test = not args.force

    cfg = load_env(Path(args.env))
    tgt = connect_target(cfg)
    t0 = time.time()
    try:
        stats = exec_with_retry(
            tgt,
            lambda: count_plan(tgt, args.due_before, args.status, only_not_test),
            "count plan",
        )
        print(
            "due_before=%s status=%s dry_run=%s stats=%s"
            % (args.due_before, args.status, dry_run, stats),
            flush=True,
        )
        for no in sample_app_nos(tgt, args.due_before, args.status):
            print("  sample %s" % no, flush=True)
        if dry_run:
            print(
                "would_update is_test=1 rows=%s" % stats.get("would_update", 0),
                flush=True,
            )
            return 0
        n = exec_with_retry(
            tgt,
            lambda: apply_update(
                tgt, args.due_before, args.status, only_not_test
            ),
            "update is_test",
        )
        print("done updated=%s elapsed=%.1fs" % (n, time.time() - t0), flush=True)
        return 0
    finally:
        tgt.close()


if __name__ == "__main__":
    raise SystemExit(main())
