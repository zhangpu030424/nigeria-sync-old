#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""将 application.status 与 loan.status 对齐（无大表 JOIN，避免 2013）。

流程:
  1. 分页扫 loan（due_date <= 截止日）→ 内存 application_no -> loan.status
  2. 分批查 application.status（默认只取 status=20）
  3. 内存比对，逐条 UPDATE application

Usage:
  python3 sync_application_status_from_loan.py --env ./ng_migration.env --dry-run
  python3 sync_application_status_from_loan.py --env ./ng_migration.env --apply
  python3 sync_application_status_from_loan.py --env ./ng_migration.env --apply \\
    --due-before 2026-07-05 --app-status 20 --commit-every 50
"""
import argparse
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

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


def parse_status_list(raw: str) -> Optional[List[str]]:
    if not raw.strip():
        return None
    return [x.strip() for x in raw.split(",") if x.strip()]


def _row_rank(row: dict) -> Tuple:
    due = row.get("due_date")
    return (
        str(due) if due is not None else "",
        int(row.get("period") or 0),
        int(row.get("roll_sequence") or 0),
    )


def load_loan_status_map(
    tgt, due_before: str, scan_chunk: int, loan_statuses: Optional[Sequence[str]]
) -> Dict[str, int]:
    """分页扫 loan，同一 application_no 取 due_date/period/roll 最大的一行。"""
    best: Dict[str, dict] = {}
    after = ""
    total_rows = 0
    while True:
        sql = """
            SELECT loan_no, application_no, status, due_date, period, roll_sequence
            FROM loan
            WHERE due_date <= %s
              AND application_no IS NOT NULL AND application_no <> ''
              AND loan_no > %s
            ORDER BY loan_no ASC
            LIMIT %s
        """
        with tgt.cursor() as cur:
            cur.execute(sql, (due_before, after, scan_chunk))
            rows = list(cur.fetchall())
        if not rows:
            break
        total_rows += len(rows)
        after = str(rows[-1]["loan_no"])
        for row in rows:
            app_no = str(row["application_no"]).strip()
            if not app_no:
                continue
            st = int(row["status"])
            if loan_statuses and str(st) not in loan_statuses:
                continue
            prev = best.get(app_no)
            if prev is None or _row_rank(row) >= _row_rank(prev):
                best[app_no] = row
        print(
            "loan scan rows=%s apps=%s last=%s"
            % (total_rows, len(best), after),
            flush=True,
        )
        if len(rows) < scan_chunk:
            break
    return {k: int(v["status"]) for k, v in best.items()}


def fetch_application_status(
    tgt, app_nos: Sequence[str]
) -> Dict[str, int]:
    if not app_nos:
        return {}
    ph = ",".join(["%s"] * len(app_nos))
    with tgt.cursor() as cur:
        cur.execute(
            "SELECT application_no, status FROM application WHERE application_no IN (%s)"
            % ph,
            tuple(app_nos),
        )
        return {
            str(r["application_no"]): int(r["status"]) for r in cur.fetchall()
        }


def build_plan(
    tgt,
    loan_map: Dict[str, int],
    app_status_filter: Optional[str],
    lookup_chunk: int,
) -> Tuple[List[dict], Dict[int, int]]:
    app_nos = sorted(loan_map.keys())
    plan: List[dict] = []
    by_loan: Dict[int, int] = defaultdict(int)
    for i in range(0, len(app_nos), lookup_chunk):
        part = app_nos[i : i + lookup_chunk]
        app_status = exec_with_retry(
            tgt,
            lambda p=part: fetch_application_status(tgt, p),
            "fetch application status",
        )
        for app_no in part:
            loan_st = loan_map[app_no]
            app_st = app_status.get(app_no)
            if app_st is None:
                continue
            if app_status_filter is not None and str(app_st) != app_status_filter:
                continue
            if app_st == loan_st:
                continue
            plan.append(
                {
                    "application_no": app_no,
                    "app_status": app_st,
                    "loan_status": loan_st,
                }
            )
            by_loan[loan_st] += 1
        print(
            "lookup %s/%s plan=%s"
            % (min(i + lookup_chunk, len(app_nos)), len(app_nos), len(plan)),
            flush=True,
        )
    return plan, dict(by_loan)


def apply_plan(
    tgt,
    plan: List[dict],
    dry_run: bool,
    commit_every: int,
) -> Tuple[int, int]:
    tracker = CommitTracker(tgt, commit_every, dry_run)
    ok = skip = 0
    for row in plan:
        app_no = row["application_no"]
        new_st = row["loan_status"]
        old_st = row["app_status"]
        if dry_run:
            ok += 1
            continue
        with tgt.cursor() as cur:
            cur.execute(
                """
                UPDATE application
                SET status=%s
                WHERE application_no=%s AND status=%s
                """,
                (new_st, app_no, old_st),
            )
            n = int(cur.rowcount or 0)
        if n:
            ok += 1
            tracker.note_write()
        else:
            skip += 1
    tracker.flush()
    if not dry_run:
        tgt.commit()
    return ok, skip


def main(argv: Optional[list] = None) -> int:
    p = argparse.ArgumentParser(
        description="Sync application.status from loan.status (no big JOIN)"
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--due-before", default="2026-07-05")
    p.add_argument(
        "--app-status",
        default="20",
        help="只处理 application 当前为该 status 的行；传空字符串=全部不一致",
    )
    p.add_argument(
        "--loan-status",
        default="",
        help="只取这些 loan.status，逗号分隔，如 23,27；空=全部",
    )
    p.add_argument("--scan-chunk", type=int, default=500, help="loan 分页大小")
    p.add_argument("--lookup-chunk", type=int, default=100, help="application IN 批量")
    p.add_argument("--commit-every", type=int, default=50)
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    dry_run = not args.apply
    loan_statuses = parse_status_list(args.loan_status)
    app_status_filter = args.app_status.strip() if args.app_status.strip() else None

    cfg = load_env(Path(args.env))
    tgt = connect_target(cfg)
    t0 = time.time()
    try:
        print(
            "due_before=%s app_status=%s loan_status=%s dry_run=%s"
            % (
                args.due_before,
                app_status_filter or "ANY",
                loan_statuses or "ALL",
                dry_run,
            ),
            flush=True,
        )
        loan_map = exec_with_retry(
            tgt,
            lambda: load_loan_status_map(
                tgt, args.due_before, args.scan_chunk, loan_statuses
            ),
            "scan loan",
        )
        print("loan_map apps=%s" % len(loan_map), flush=True)
        plan, by_loan = exec_with_retry(
            tgt,
            lambda: build_plan(
                tgt, loan_map, app_status_filter, args.lookup_chunk
            ),
            "build plan",
        )
        print("by_loan_status=%s would_update=%s" % (by_loan, len(plan)), flush=True)
        for row in plan[:15]:
            print(
                "  sample %s app=%s -> loan=%s"
                % (row["application_no"], row["app_status"], row["loan_status"]),
                flush=True,
            )
        if dry_run:
            print("dry-run done would_update=%s" % len(plan), flush=True)
            return 0
        ok, skip = apply_plan(tgt, plan, dry_run=False, commit_every=args.commit_every)
        print(
            "done updated=%s skip=%s elapsed=%.1fs"
            % (ok, skip, time.time() - t0),
            flush=True,
        )
        return 0
    finally:
        tgt.close()


if __name__ == "__main__":
    raise SystemExit(main())
