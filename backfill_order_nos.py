#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Backfill target application_no / loan_no to new order-no format.

Old target rows typically use:
  application.application_no = market applicationNo (e.g. 178225557412028023)
  loan.loan_no                 = NG-{plan_sn}
  loan.application_no          = market applicationNo

New format (see ng_migration_run.format_*):
  application_no = ng{appId:04d}-{core_sn}
  loan_no        = ng-{core_sn}-01000

Usage:
  python3 backfill_order_nos.py --env ./ng_migration.env --dry-run
  python3 backfill_order_nos.py --env ./ng_migration.env --apply --batch-size 5000
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

import pymysql
from pymysql.cursors import DictCursor

import ng_migration_run as mig

HERE = Path(__file__).resolve().parent
NEW_APP_NO_RE = re.compile(r"^ng\d{4}-.+$")


def load_env(path: Path) -> Dict[str, str]:
    cfg: Dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        cfg[k.strip()] = v.strip().strip("'\"")
    return cfg


def connect_source(cfg: Dict[str, str]):
    return pymysql.connect(
        host=cfg["SOURCE_HOST"],
        port=int(cfg.get("SOURCE_PORT", "3306")),
        user=cfg["SOURCE_USER"],
        password=cfg["SOURCE_PASSWORD"],
        charset="utf8mb4",
        cursorclass=DictCursor,
        read_timeout=3600,
        write_timeout=3600,
    )


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
    )


def fetch_mapping_for_old_nos(src, old_nos: List[str]) -> Dict[str, Tuple[str, str]]:
    """old market applicationNo -> (new_application_no, new_loan_no)."""
    out: Dict[str, Tuple[str, str]] = {}
    if not old_nos:
        return out
    m, c = "ng_loan_market", "ng_loan_core"
    for i in range(0, len(old_nos), 2000):
        part = old_nos[i:i + 2000]
        ph = ",".join(["%s"] * len(part))
        with src.cursor() as cur:
            cur.execute(
                f"""
                SELECT a.applicationNo AS old_no, a.`appId` AS app_id, ca.sn AS core_sn
                FROM {m}.application a
                INNER JOIN {c}.application ca ON ca.ext_sn = a.applicationNo
                WHERE a.applicationNo IN ({ph})
                  AND ca.sn IS NOT NULL AND ca.sn <> ''
                """,
                part,
            )
            rows = list(cur.fetchall())
        for row in rows:
            old_no = str(row["old_no"])
            new_app = mig.format_application_no(row.get("app_id"), row.get("core_sn"))
            new_loan = mig.format_loan_no(row.get("core_sn"), 1, 0)
            if new_app and new_loan:
                out[old_no] = (new_app, new_loan)
    return out


def list_old_application_nos(tgt, limit: int, after: str = "") -> List[str]:
    sql = """
        SELECT application_no
        FROM application
        WHERE application_no IS NOT NULL AND application_no <> ''
          AND application_no NOT REGEXP '^ng[0-9]{4}-'
    """
    params: List[str] = []
    if after:
        sql += " AND application_no > %s"
        params.append(after)
    sql += " ORDER BY application_no ASC LIMIT %s"
    params.append(limit)
    with tgt.cursor() as cur:
        cur.execute(sql, params)
        return [str(r["application_no"]) for r in cur.fetchall()]


def count_old_rows(tgt) -> Tuple[int, int]:
    with tgt.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS c FROM application
            WHERE application_no IS NOT NULL AND application_no <> ''
              AND application_no NOT REGEXP '^ng[0-9]{4}-'
            """
        )
        apps = int(cur.fetchone()["c"])
        cur.execute(
            """
            SELECT COUNT(*) AS c FROM loan
            WHERE application_no IS NOT NULL AND application_no <> ''
              AND application_no NOT REGEXP '^ng[0-9]{4}-'
            """
        )
        loans = int(cur.fetchone()["c"])
    return apps, loans


def apply_application_updates(tgt, mapping: Dict[str, Tuple[str, str]], dry_run: bool) -> Tuple[int, int]:
    ok = skip = 0
    with tgt.cursor() as cur:
        for old_no, (new_no, _) in mapping.items():
            cur.execute("SELECT 1 FROM application WHERE application_no=%s LIMIT 1", (new_no,))
            if cur.fetchone():
                skip += 1
                continue
            cur.execute("SELECT 1 FROM application WHERE application_no=%s LIMIT 1", (old_no,))
            if not cur.fetchone():
                skip += 1
                continue
            if dry_run:
                ok += 1
                continue
            cur.execute(
                "UPDATE application SET application_no=%s WHERE application_no=%s",
                (new_no, old_no),
            )
            ok += int(cur.rowcount or 0)
    if not dry_run:
        tgt.commit()
    return ok, skip


def apply_loan_updates(tgt, mapping: Dict[str, Tuple[str, str]], dry_run: bool) -> Tuple[int, int]:
    ok = skip = 0
    with tgt.cursor() as cur:
        for old_app_no, (new_app_no, new_loan_no) in mapping.items():
            cur.execute("SELECT 1 FROM loan WHERE loan_no=%s LIMIT 1", (new_loan_no,))
            if cur.fetchone():
                # loan_no already exists; still try application_no-only rows
                pass
            cur.execute(
                "SELECT loan_no FROM loan WHERE application_no=%s",
                (old_app_no,),
            )
            rows = list(cur.fetchall())
            if not rows:
                skip += 1
                continue
            for row in rows:
                old_loan_no = str(row["loan_no"])
                if old_loan_no == new_loan_no and old_app_no == new_app_no:
                    skip += 1
                    continue
                cur.execute("SELECT 1 FROM loan WHERE loan_no=%s LIMIT 1", (new_loan_no,))
                if cur.fetchone() and old_loan_no != new_loan_no:
                    skip += 1
                    continue
                if dry_run:
                    ok += 1
                    continue
                cur.execute(
                    "UPDATE loan SET loan_no=%s, application_no=%s WHERE loan_no=%s",
                    (new_loan_no, new_app_no, old_loan_no),
                )
                ok += int(cur.rowcount or 0)
    if not dry_run:
        tgt.commit()
    return ok, skip


def main(argv: List[str] = None) -> int:
    p = argparse.ArgumentParser(description="Backfill target order numbers to ng new format")
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true", help="write changes (default dry-run)")
    p.add_argument("--batch-size", type=int, default=5000)
    p.add_argument("--max-batches", type=int, default=0, help="0 = no limit")
    args = p.parse_args(argv)
    dry_run = not args.apply

    cfg = load_env(Path(args.env))
    src = connect_source(cfg)
    tgt = connect_target(cfg)
    try:
        old_apps, old_loans = count_old_rows(tgt)
        print(f"target old_format application={old_apps} loan={old_loans} dry_run={dry_run}")
        if old_apps == 0 and old_loans == 0:
            print("nothing to backfill")
            return 0

        total_app_ok = total_app_skip = 0
        total_loan_ok = total_loan_skip = 0
        after = ""
        batch_no = 0
        while True:
            batch_no += 1
            if args.max_batches and batch_no > args.max_batches:
                break
            old_nos = list_old_application_nos(tgt, args.batch_size, after=after)
            if not old_nos:
                break
            after = old_nos[-1]
            t0 = time.time()
            mapping = fetch_mapping_for_old_nos(src, old_nos)
            miss = len(old_nos) - len(mapping)
            app_ok, app_skip = apply_application_updates(tgt, mapping, dry_run)
            loan_ok, loan_skip = apply_loan_updates(tgt, mapping, dry_run)
            total_app_ok += app_ok
            total_app_skip += app_skip
            total_loan_ok += loan_ok
            total_loan_skip += loan_skip
            print(
                f"batch={batch_no} old_nos={len(old_nos)} mapped={len(mapping)} miss_core_sn={miss} "
                f"app_ok={app_ok} app_skip={app_skip} loan_ok={loan_ok} loan_skip={loan_skip} "
                f"elapsed={time.time()-t0:.1f}s after={after}"
            )

        old_apps, old_loans = count_old_rows(tgt)
        print(
            f"done app_updated={total_app_ok} app_skip={total_app_skip} "
            f"loan_updated={total_loan_ok} loan_skip={total_loan_skip} "
            f"remaining_old application={old_apps} loan={old_loans}"
        )
        return 0
    finally:
        src.close()
        tgt.close()


if __name__ == "__main__":
    raise SystemExit(main())
