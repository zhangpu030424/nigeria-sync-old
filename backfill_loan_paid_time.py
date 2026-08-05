#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
历史刷数：cms.loan.paid_time = ng_loan_market.application.paidTime * 100

与 Flink loan_incr_bundle_lookup / ng_migration_run._build_loan_row 口径一致。

用法（在 nigeria-sync-old 目录，需 ng_migration.env）:
  python3 backfill_loan_paid_time.py --dry-run --limit 100
  python3 backfill_loan_paid_time.py --workers 8 --batch 2000
"""
from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import ng_migration_run as mig


def _fetch_batch(src, last_id: int, batch: int) -> List[dict]:
    m, c = "ng_loan_market", "ng_loan_core"
    sql = f"""
        SELECT
          ma.id AS src_id,
          ma.`appId` AS app_id,
          ma.applicationNo AS application_no_raw,
          CAST(IFNULL(ma.paidTime, 0) AS SIGNED) * 100 AS paid_time
        FROM {m}.application ma
        INNER JOIN {c}.application ca ON ca.ext_sn = ma.applicationNo
        WHERE ma.id > %s
          AND ma.disburseTime > 0
        ORDER BY ma.id
        LIMIT %s
    """
    with src.cursor() as cur:
        cur.execute(sql, (last_id, batch))
        return list(cur.fetchall())


def _update_target(tgt, rows: List[Tuple[Optional[int], str]], dry_run: bool) -> int:
    """rows: [(paid_time_or_None, application_no), ...]"""
    if not rows:
        return 0
    if dry_run:
        return len(rows)
    sql = """
        UPDATE loan
        SET paid_time = %s
        WHERE application_no = %s
          AND `period` = 1
          AND roll_sequence = 0
    """
    with tgt.cursor() as cur:
        cur.executemany(sql, rows)
    tgt.commit()
    return len(rows)


def run(args: argparse.Namespace) -> int:
    cfg = mig.load_env()
    src = mig.connect_source(cfg)
    tgt = mig.connect_target(cfg)
    last_id = int(args.after_id or 0)
    total_src = 0
    total_upd = 0
    t0 = time.time()
    try:
        while True:
            batch = _fetch_batch(src, last_id, args.batch)
            if not batch:
                break
            total_src += len(batch)
            last_id = int(batch[-1]["src_id"])
            pairs: List[Tuple[Optional[int], str]] = []
            for r in batch:
                app_no = mig.format_application_no(r.get("app_id"), r.get("application_no_raw"))
                if not app_no:
                    continue
                pt = int(r.get("paid_time") or 0)
                pairs.append((pt if pt > 0 else None, app_no))
            n = _update_target(tgt, pairs, args.dry_run)
            total_upd += n
            if total_src % max(args.batch * 5, 1) < args.batch or len(batch) < args.batch:
                print(
                    f"[backfill] src={total_src} upd={total_upd} last_id={last_id} "
                    f"elapsed={int(time.time() - t0)}s dry_run={int(args.dry_run)}",
                    flush=True,
                )
            if args.limit and total_src >= args.limit:
                break
            if len(batch) < args.batch:
                break
    finally:
        mig._close_mysql_conn(src)
        mig._close_mysql_conn(tgt)
    print(
        f"[backfill] DONE src={total_src} upd={total_upd} last_id={last_id} "
        f"elapsed={int(time.time() - t0)}s",
        flush=True,
    )
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="刷 loan.paid_time = application.paidTime * 100")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--batch", type=int, default=2000)
    p.add_argument("--limit", type=int, default=0, help="最多处理源 application 行数，0=不限")
    p.add_argument("--after-id", type=int, default=0, help="从 application.id > N 继续")
    p.add_argument("--workers", type=int, default=1, help="预留；当前单线程刷数")
    args = p.parse_args()
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
