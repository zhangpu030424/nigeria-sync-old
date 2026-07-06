#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""将 application.status 与 loan.status 对齐。

查不一致（约 4290 条）:
  SELECT l.application_no, l.status, a.status
  FROM loan l JOIN application a ON a.application_no = l.application_no
  WHERE l.due_date <= '2026-07-05' AND a.status = 20 AND l.status <> a.status;

写入: 单连接逐条 UPDATE（经 8001 代理勿开多进程/批量 JOIN UPDATE）

Usage:
  python3 sync_application_status_from_loan.py --env ./ng_migration.env --dry-run \\
    --plan-file /tmp/sync_app_status_plan.json

  python3 sync_application_status_from_loan.py --env ./ng_migration.env --apply \\
    --plan-file /tmp/sync_app_status_plan.json
"""
import argparse
import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

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


def save_plan(path: Path, plan: List[dict]) -> None:
    path.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")


def load_plan(path: Path) -> List[dict]:
    return json.loads(path.read_text(encoding="utf-8"))


def fetch_mismatch_page(
    tgt,
    due_before: str,
    after: str,
    limit: int,
    app_status: Optional[str],
    loan_statuses: Optional[Sequence[str]],
) -> List[dict]:
    sql = """
        SELECT l.loan_no, l.application_no,
               l.status AS loan_status, a.status AS app_status,
               l.due_date, l.period, l.roll_sequence
        FROM loan l
        INNER JOIN application a ON a.application_no = l.application_no
        WHERE l.due_date <= %s
          AND l.status <> a.status
          AND l.loan_no > %s
    """
    params: List = [due_before, after]
    if app_status is not None:
        sql += " AND a.status = %s"
        params.append(int(app_status))
    if loan_statuses:
        ph = ",".join(["%s"] * len(loan_statuses))
        sql += " AND l.status IN (%s)" % ph
        params.extend(loan_statuses)
    sql += " ORDER BY l.loan_no ASC LIMIT %s"
    params.append(limit)
    with tgt.cursor() as cur:
        cur.execute(sql, tuple(params))
        return list(cur.fetchall())


def build_plan(
    tgt,
    due_before: str,
    scan_chunk: int,
    app_status_filter: Optional[str],
    loan_statuses: Optional[Sequence[str]],
) -> Tuple[List[dict], Dict[int, int]]:
    plan_by_app: Dict[str, dict] = {}
    after = ""
    total_rows = 0
    while True:
        rows = exec_with_retry(
            tgt,
            lambda a=after: fetch_mismatch_page(
                tgt, due_before, a, scan_chunk, app_status_filter, loan_statuses
            ),
            "fetch mismatch page",
        )
        if not rows:
            break
        after = str(rows[-1]["loan_no"])
        total_rows += len(rows)
        for row in rows:
            app_no = str(row["application_no"]).strip()
            cur = {
                "application_no": app_no,
                "app_status": int(row["app_status"]),
                "loan_status": int(row["loan_status"]),
                "due_date": row.get("due_date"),
                "period": row.get("period"),
                "roll_sequence": row.get("roll_sequence"),
            }
            prev = plan_by_app.get(app_no)
            if prev is None or _row_rank(cur) >= _row_rank(prev):
                plan_by_app[app_no] = cur
        print(
            "mismatch page rows=%s plan=%s last=%s"
            % (total_rows, len(plan_by_app), after[-30:]),
            flush=True,
        )
        if len(rows) < scan_chunk:
            break
    by_loan: Dict[int, int] = defaultdict(int)
    for st in plan_by_app.values():
        by_loan[int(st["loan_status"])] += 1
    plan = [
        {
            "application_no": v["application_no"],
            "app_status": v["app_status"],
            "loan_status": v["loan_status"],
        }
        for v in plan_by_app.values()
    ]
    return plan, dict(by_loan)


def apply_one(tgt, row: dict) -> int:
    with tgt.cursor() as cur:
        cur.execute(
            """
            UPDATE application
            SET status=%s
            WHERE application_no=%s AND status=%s
            """,
            (row["loan_status"], row["application_no"], row["app_status"]),
        )
        return int(cur.rowcount or 0)


def run_apply(
    cfg: Dict[str, str],
    plan: List[dict],
    commit_every: int,
    log_every: int,
    sleep_ms: int,
) -> Tuple[int, int]:
    tgt = connect_target(cfg)
    ok = skip = 0
    pending = 0
    try:
        for i, row in enumerate(plan, 1):
            n = 0
            for attempt in range(8):
                try:
                    n = apply_one(tgt, row)
                    break
                except pymysql.Error as exc:
                    try:
                        tgt.rollback()
                    except Exception:
                        pass
                    try:
                        tgt.close()
                    except Exception:
                        pass
                    if attempt >= 7:
                        raise
                    wait = 2 + attempt * 2
                    print(
                        "update %s retry err=%s wait=%ss"
                        % (row["application_no"], exc, wait),
                        flush=True,
                    )
                    time.sleep(wait)
                    tgt = connect_target(cfg)
            if n:
                ok += 1
                pending += 1
            else:
                skip += 1
            if pending >= commit_every:
                tgt.commit()
                pending = 0
            if sleep_ms > 0:
                time.sleep(sleep_ms / 1000.0)
            if i % max(1, log_every) == 0 or i == len(plan):
                print(
                    "progress ok=%s skip=%s done=%s/%s last=%s"
                    % (ok, skip, i, len(plan), row["application_no"]),
                    flush=True,
                )
        if pending:
            tgt.commit()
        elif ok or skip:
            tgt.commit()
    finally:
        tgt.close()
    return ok, skip


def main(argv: Optional[list] = None) -> int:
    p = argparse.ArgumentParser(
        description="Sync application.status from loan.status"
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--due-before", default="2026-07-05")
    p.add_argument(
        "--app-status",
        default="20",
        help="application 侧 status 条件，默认 20；空=不限",
    )
    p.add_argument(
        "--loan-status",
        default="",
        help="只取这些 loan.status，逗号分隔；空=全部",
    )
    p.add_argument("--scan-chunk", type=int, default=500)
    p.add_argument(
        "--plan-file",
        default="",
        help="dry-run 写出 plan；apply 优先从此文件读（加 --rebuild-plan 强制重扫）",
    )
    p.add_argument("--rebuild-plan", action="store_true")
    p.add_argument("--commit-every", type=int, default=20)
    p.add_argument("--log-every", type=int, default=100)
    p.add_argument(
        "--sleep-ms",
        type=int,
        default=50,
        help="每条 UPDATE 后休眠毫秒数，减轻代理压力",
    )
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    dry_run = not args.apply
    loan_statuses = parse_status_list(args.loan_status)
    app_status_filter = args.app_status.strip() if args.app_status.strip() else None
    plan_path = Path(args.plan_file) if args.plan_file.strip() else None

    cfg = load_env(Path(args.env))
    t0 = time.time()
    tgt = connect_target(cfg)
    try:
        plan: List[dict] = []
        by_loan: Dict[int, int] = {}
        use_cached = (
            plan_path is not None
            and plan_path.is_file()
            and not args.rebuild_plan
            and not dry_run
        )
        if use_cached:
            plan = load_plan(plan_path)
            for row in plan:
                by_loan[int(row["loan_status"])] = (
                    by_loan.get(int(row["loan_status"]), 0) + 1
                )
            print("loaded plan from %s rows=%s" % (plan_path, len(plan)), flush=True)
        else:
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
            if plan_path is not None:
                save_plan(plan_path, plan)
                print("saved plan to %s" % plan_path, flush=True)

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
    finally:
        tgt.close()

    ok, skip = run_apply(
        cfg, plan, args.commit_every, args.log_every, args.sleep_ms
    )
    print(
        "done updated=%s skip=%s elapsed=%.1fs"
        % (ok, skip, time.time() - t0),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
