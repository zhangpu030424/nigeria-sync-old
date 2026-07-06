#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""修复目标库 loan_no 中间段仍为 market 长号（15~18 位）的残留行。

正确格式:
  loan_no        = ng-{core_sn}-01000          （短号，约 12 位）
  application_no = ng{appId}-{marketNo}        （长号）

错误格式（本脚本处理）:
  loan_no        = ng-167034212912015986-01000  （中间 18 位 = market 号）
  application_no = ng0502-167034212912015986    （长号，往往已对）

逻辑:
  1. 分页扫 loan（无大 IN）
  2. loan_no 中间段长度 >= --min-sn-len
  3. 按 application_no 查 application.sn（core sn）
  4. 改成 ng-{sn}-01000；若正确行已存在则删长号行
  5. 打印 REPAIR_AUDIT + .deleted.csv / .modified.csv

Usage:
  python3 repair_loan_long_sn.py --env ./ng_migration.env --dry-run --min-sn-len 15
  python3 repair_loan_long_sn.py --env ./ng_migration.env --apply --min-sn-len 15 --commit-every 20
"""
import argparse
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pymysql
from pymysql.cursors import DictCursor

import ng_migration_run as mig

# 复用 repair 脚本的审计与写入逻辑
from repair_loan_no_from_audit import (
    CommitTracker,
    RepairAuditLog,
    RowChangeAuditLog,
    cols_sql,
    exec_with_retry,
    fetch_loan_row,
    insert_row,
    loan_exists,
    repair_one_loan,
)

HERE = Path(__file__).resolve().parent
LOAN_COLS = mig.LOAN_INSERT_COLS
LOAN_NO_RE = re.compile(r"^[Nn][Gg]-(\d+)-(\d{5})$")


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


def parse_loan_middle(loan_no: str) -> Optional[Tuple[str, int]]:
    m = LOAN_NO_RE.match(str(loan_no or "").strip())
    if not m:
        return None
    return m.group(1), len(m.group(1))


def _long_loan_no_regexp(min_sn_len: int) -> str:
    n = max(1, int(min_sn_len))
    return r"^[Nn][Gg]-[0-9]{%d,}-[0-9]{5}$" % n


def scan_loan_batch(tgt, after: str, limit: int, min_sn_len: int) -> List[dict]:
    """只扫 loan_no 中间段 >= min_sn_len 的候选行，并 JOIN application 取 core sn。"""
    sql = """
        SELECT l.loan_no, l.application_no, a.sn AS core_sn
        FROM loan l
        LEFT JOIN application a ON a.application_no = l.application_no
        WHERE l.loan_no > %s
          AND l.loan_no REGEXP %s
        ORDER BY l.loan_no ASC
        LIMIT %s
    """
    with tgt.cursor() as cur:
        cur.execute(sql, (after, _long_loan_no_regexp(min_sn_len), limit))
        return list(cur.fetchall())


def build_plan_from_scan(
    tgt,
    min_sn_len: int,
    scan_size: int,
    work_limit: int,
) -> List[dict]:
    plan = []
    after = ""
    batches = 0
    while True:
        try:
            tgt.ping(reconnect=True)
        except Exception:
            pass
        rows = exec_with_retry(
            tgt,
            lambda a=after: scan_loan_batch(tgt, a, scan_size, min_sn_len),
            "scan loan batch after=%s" % (after or "(start)"),
        )
        batches += 1
        if not rows:
            break
        after = str(rows[-1]["loan_no"])
        for row in rows:
            parsed = parse_loan_middle(row["loan_no"])
            if not parsed:
                continue
            middle, mlen = parsed
            if mlen < min_sn_len:
                continue
            app_no = str(row["application_no"] or "").strip()
            core_sn = str(row.get("core_sn") or "").strip()
            if not core_sn:
                print(
                    "skip no_core_sn loan_no=%s application_no=%s"
                    % (row["loan_no"], app_no),
                    flush=True,
                )
                continue
            correct = mig.format_loan_no(core_sn, 1, 0)
            if not correct or correct == str(row["loan_no"]):
                continue
            plan.append(
                {
                    "wrong_loan_no": str(row["loan_no"]),
                    "correct_loan_no": correct,
                    "legacy_loan_no": "NG-%s" % core_sn,
                    "application_no": app_no,
                    "app_id": "",
                    "core_sn": core_sn,
                }
            )
            if work_limit and len(plan) >= work_limit:
                return plan
        if batches % 10 == 0:
            print(
                "scan progress batches=%s plan=%s last_loan_no=%s"
                % (batches, len(plan), after),
                flush=True,
            )
        if len(rows) < scan_size:
            break
    return plan


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Fix loan_no with long market sn in middle")
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--min-sn-len", type=int, default=15, help="仅处理中间段 >= 该长度")
    p.add_argument("--scan-size", type=int, default=500)
    p.add_argument("--commit-every", type=int, default=20)
    p.add_argument("--work-limit", type=int, default=0)
    p.add_argument("--log-every", type=int, default=100)
    p.add_argument("--repair-log", default="")
    p.add_argument("--no-repair-log", action="store_true")
    p.add_argument("--plan-only", action="store_true")
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    dry_run = not args.apply

    cfg = load_env(Path(args.env))
    tgt = connect_target(cfg)
    try:
        print(
            "scan long loan_no min_sn_len=%s scan_size=%s dry_run=%s"
            % (args.min_sn_len, args.scan_size, dry_run),
            flush=True,
        )
        plan = build_plan_from_scan(
            tgt, args.min_sn_len, args.scan_size, args.work_limit
        )
        print("repair_plan=%s" % len(plan), flush=True)
        for row in plan[:15]:
            print(
                "  %s -> %s app=%s core_sn=%s"
                % (
                    row["wrong_loan_no"],
                    row["correct_loan_no"],
                    row["application_no"],
                    row["core_sn"],
                ),
                flush=True,
            )
        if len(plan) > 15:
            print("  ... and %s more" % (len(plan) - 15), flush=True)
        if args.plan_only:
            return 0 if plan else 1
        if not plan:
            print("no long loan_no rows to fix", flush=True)
            return 0
    finally:
        tgt.close()

    repair_log = args.repair_log or (
        "/tmp/repair_loan_long_sn_%s.csv"
        % datetime.now().strftime("%Y%m%d_%H%M%S")
    )
    audit = RepairAuditLog(repair_log if not args.no_repair_log else None, enabled=not args.no_repair_log)
    row_audit = RowChangeAuditLog(repair_log, enabled=not args.no_repair_log)

    tgt = connect_target(cfg)
    try:
        ok = skip = 0
        tracker = CommitTracker(tgt, args.commit_every, dry_run)
        for i, row in enumerate(plan, 1):
            status = exec_with_retry(
                tgt,
                lambda r=row: repair_one_loan(
                    tgt, r, dry_run, "update", audit, row_audit, tracker
                ),
                "repair %s" % row["wrong_loan_no"],
            )
            if status == "ok":
                ok += 1
            else:
                skip += 1
            if i % max(1, args.log_every) == 0:
                print("progress ok=%s skip=%s last=%s" % (ok, skip, row["wrong_loan_no"]), flush=True)
        tracker.flush()
        print("done ok=%s skip=%s repair_log=%s" % (ok, skip, repair_log), flush=True)
        return 0
    finally:
        audit.close()
        row_audit.close()
        tgt.close()


if __name__ == "__main__":
    raise SystemExit(main())
