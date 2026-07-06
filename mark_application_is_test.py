#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按 loan 条件将对应 application.is_test 设为 1。

示例:
  SELECT application_no FROM loan
  WHERE due_date < '2026-07-05' AND status = 20;

  → UPDATE application SET is_test=1 WHERE application_no IN (...)

Usage:
  python3 mark_application_is_test.py --env ./ng_migration.env --dry-run \\
    --due-before 2026-07-05 --status 20

  python3 mark_application_is_test.py --env ./ng_migration.env --apply \\
    --due-before 2026-07-05 --status 20 --commit-every 100
"""
import argparse
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

import pymysql
from pymysql.cursors import DictCursor

from repair_loan_no_from_audit import CommitTracker, exec_with_retry

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


def load_application_nos_from_loans(
    tgt, due_before: str, status: str
) -> List[str]:
    sql = """
        SELECT DISTINCT application_no
        FROM loan
        WHERE due_date < %s AND status = %s
          AND application_no IS NOT NULL AND application_no <> ''
        ORDER BY application_no ASC
    """
    with tgt.cursor() as cur:
        cur.execute(sql, (due_before, status))
        return [str(r["application_no"]) for r in cur.fetchall()]


def count_already_test(tgt, app_nos: List[str]) -> int:
    if not app_nos:
        return 0
    n = 0
    for i in range(0, len(app_nos), 2000):
        part = app_nos[i : i + 2000]
        ph = ",".join(["%s"] * len(part))
        with tgt.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*) AS c FROM application
                WHERE application_no IN ({ph}) AND is_test = 1
                """,
                part,
            )
            n += int(cur.fetchone()["c"])
    return n


def count_missing_application(tgt, app_nos: List[str]) -> int:
    if not app_nos:
        return 0
    found: Set[str] = set()
    for i in range(0, len(app_nos), 2000):
        part = app_nos[i : i + 2000]
        ph = ",".join(["%s"] * len(part))
        with tgt.cursor() as cur:
            cur.execute(
                f"SELECT application_no FROM application WHERE application_no IN ({ph})",
                part,
            )
            for row in cur.fetchall():
                found.add(str(row["application_no"]))
    return len(app_nos) - len(found)


def apply_batch(
    tgt,
    app_nos: List[str],
    dry_run: bool,
    tracker: Optional[CommitTracker],
) -> int:
    if not app_nos:
        return 0
    if dry_run:
        return len(app_nos)
    with tgt.cursor() as cur:
        ph = ",".join(["%s"] * len(app_nos))
        cur.execute(
            f"""
            UPDATE application SET is_test = 1
            WHERE application_no IN ({ph}) AND (is_test IS NULL OR is_test <> 1)
            """,
            app_nos,
        )
        n = int(cur.rowcount or 0)
    if tracker:
        tracker.note_write()
    else:
        tgt.commit()
    return n


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Set application.is_test=1 from loan filter")
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--due-before", default="2026-07-05")
    p.add_argument("--status", default="20")
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--commit-every", type=int, default=100)
    p.add_argument("--log-every", type=int, default=1)
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    dry_run = not args.apply

    cfg = load_env(Path(args.env))
    tgt = connect_target(cfg)
    t0 = time.time()
    try:
        app_nos = load_application_nos_from_loans(
            tgt, args.due_before, args.status
        )
        already = count_already_test(tgt, app_nos)
        missing = count_missing_application(tgt, app_nos)
        print(
            "loan_filter due_before=%s status=%s distinct_application_no=%s "
            "already_is_test=%s missing_application=%s dry_run=%s"
            % (
                args.due_before,
                args.status,
                len(app_nos),
                already,
                missing,
                dry_run,
            ),
            flush=True,
        )
        for no in app_nos[:20]:
            print("  %s" % no, flush=True)
        if len(app_nos) > 20:
            print("  ... and %s more" % (len(app_nos) - 20), flush=True)
        if not app_nos:
            return 0

        tracker = CommitTracker(tgt, args.commit_every, dry_run)
        updated = 0
        batches = 0
        for i in range(0, len(app_nos), args.batch_size):
            part = app_nos[i : i + args.batch_size]
            batches += 1
            n = exec_with_retry(
                tgt,
                lambda batch=part: apply_batch(tgt, batch, dry_run, tracker),
                "update is_test batch=%s" % batches,
            )
            updated += n
            if batches % max(1, args.log_every) == 0:
                print(
                    "progress batches=%s updated=%s last=%s"
                    % (batches, updated, part[-1]),
                    flush=True,
                )
        tracker.flush()
        print(
            "done updated=%s batches=%s elapsed=%.1fs"
            % (updated, batches, time.time() - t0),
            flush=True,
        )
        return 0
    finally:
        tgt.close()


if __name__ == "__main__":
    raise SystemExit(main())
