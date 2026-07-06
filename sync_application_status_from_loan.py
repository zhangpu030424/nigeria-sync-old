#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""将 application.status 与 loan.status 对齐（无大表 JOIN，避免 2013）。

流程（默认从 application 出发，不扫全表 loan）:
  1. 分页扫 application（默认 status=20）
  2. 每批 application_no 查 loan.status（due_date <= 截止日）
  3. 内存比对，逐条 UPDATE

Usage:
  python3 sync_application_status_from_loan.py --env ./ng_migration.env --dry-run
  python3 sync_application_status_from_loan.py --env ./ng_migration.env --apply
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


def fetch_application_page(
    tgt,
    after: str,
    limit: int,
    app_status: Optional[str],
) -> List[dict]:
    sql = """
        SELECT application_no, status
        FROM application
        WHERE application_no > %s
    """
    params: List = [after]
    if app_status is not None:
        sql += " AND status = %s"
        params.append(int(app_status))
    sql += " ORDER BY application_no ASC LIMIT %s"
    params.append(limit)
    with tgt.cursor() as cur:
        cur.execute(sql, tuple(params))
        return list(cur.fetchall())


def fetch_loan_status_for_apps(
    tgt,
    app_nos: Sequence[str],
    due_before: str,
    loan_statuses: Optional[Sequence[str]],
) -> Dict[str, int]:
    if not app_nos:
        return {}
    ph = ",".join(["%s"] * len(app_nos))
    params = tuple(app_nos) + (due_before,)
    with tgt.cursor() as cur:
        cur.execute(
            """
            SELECT application_no, status, due_date, period, roll_sequence
            FROM loan
            WHERE application_no IN (%s)
              AND due_date <= %%s
              AND application_no IS NOT NULL AND application_no <> ''
            """
            % ph,
            params,
        )
        rows = list(cur.fetchall())
    best: Dict[str, dict] = {}
    for row in rows:
        app_no = str(row["application_no"]).strip()
        st = int(row["status"])
        if loan_statuses and str(st) not in loan_statuses:
            continue
        prev = best.get(app_no)
        if prev is None or _row_rank(row) >= _row_rank(prev):
            best[app_no] = row
    return {k: int(v["status"]) for k, v in best.items()}


def build_plan(
    tgt,
    due_before: str,
    scan_chunk: int,
    app_status_filter: Optional[str],
    loan_statuses: Optional[Sequence[str]],
) -> Tuple[List[dict], Dict[int, int]]:
    plan: List[dict] = []
    by_loan: Dict[int, int] = defaultdict(int)
    after = ""
    total_apps = 0
    no_loan = 0
    while True:
        apps = exec_with_retry(
            tgt,
            lambda a=after: fetch_application_page(
                tgt, a, scan_chunk, app_status_filter
            ),
            "scan application",
        )
        if not apps:
            break
        after = str(apps[-1]["application_no"])
        total_apps += len(apps)
        app_nos = [str(a["application_no"]) for a in apps]
        loan_map = exec_with_retry(
            tgt,
            lambda nos=app_nos: fetch_loan_status_for_apps(
                tgt, nos, due_before, loan_statuses
            ),
            "fetch loan status",
        )
        for app in apps:
            app_no = str(app["application_no"])
            app_st = int(app["status"])
            loan_st = loan_map.get(app_no)
            if loan_st is None:
                no_loan += 1
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
            "app scan total=%s plan=%s no_loan_in_range=%s last=%s"
            % (total_apps, len(plan), no_loan, after[:40]),
            flush=True,
        )
        if len(apps) < scan_chunk:
            break
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
        help="只扫 application 中该 status；传空字符串=扫全表 application",
    )
    p.add_argument(
        "--loan-status",
        default="",
        help="只取这些 loan.status，逗号分隔，如 23,27；空=全部",
    )
    p.add_argument("--scan-chunk", type=int, default=500, help="application 分页大小")
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
                app_status_filter if app_status_filter is not None else "ANY",
                loan_statuses or "ALL",
                dry_run,
            ),
            flush=True,
        )
        plan, by_loan = exec_with_retry(
            tgt,
            lambda: build_plan(
                tgt,
                args.due_before,
                args.scan_chunk,
                app_status_filter,
                loan_statuses,
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
