#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""将 application.status 与 loan.status 对齐。

直接分页查「不一致」行（约 4561 条），不扫 12 万 loan:

  SELECT l.application_no, l.status, a.status
  FROM loan l
  JOIN application a ON a.application_no = l.application_no
  WHERE l.due_date <= '2026-07-05'
    AND a.status = 20
    AND l.status <> a.status;

Usage:
  python3 sync_application_status_from_loan.py --env ./ng_migration.env --dry-run
  python3 sync_application_status_from_loan.py --env ./ng_migration.env --apply --workers 8
"""
import argparse
import hashlib
import multiprocessing
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


def apply_batch(tgt, rows: List[dict]) -> int:
    if not rows:
        return 0
    parts = []
    params: List = []
    for r in rows:
        parts.append("SELECT %s AS application_no, %s AS old_status, %s AS new_status")
        params.extend([r["application_no"], r["app_status"], r["loan_status"]])
    sql = (
        """
        UPDATE application a
        INNER JOIN (
        """
        + " UNION ALL ".join(parts)
        + """
        ) x ON a.application_no = x.application_no AND a.status = x.old_status
        SET a.status = x.new_status
        """
    )
    with tgt.cursor() as cur:
        cur.execute(sql, tuple(params))
        return int(cur.rowcount or 0)


def run_apply_chunk(
    tgt,
    chunk: List[dict],
    commit_every: int,
    log_every: int,
    batch_size: int,
    prefix: str = "",
) -> Tuple[int, int]:
    ok = skip = 0
    pending = 0
    for i in range(0, len(chunk), batch_size):
        part = chunk[i : i + batch_size]
        n = exec_with_retry(
            tgt,
            lambda p=part: apply_batch(tgt, p),
            "%sbatch update" % prefix,
        )
        ok += n
        skip += len(part) - n
        pending += n
        if pending >= commit_every:
            tgt.commit()
            pending = 0
        done = min(i + batch_size, len(chunk))
        if done % max(1, log_every) == 0 or done == len(chunk):
            print(
                "%sprogress ok=%s skip=%s done=%s/%s"
                % (prefix, ok, skip, done, len(chunk)),
                flush=True,
            )
    if pending:
        tgt.commit()
    elif ok or skip:
        tgt.commit()
    return ok, skip


def split_chunks(rows: List[dict], workers: int) -> List[List[dict]]:
    n = max(1, int(workers))
    chunks: List[List[dict]] = [[] for _ in range(n)]
    for row in rows:
        key = str(row.get("application_no") or "")
        idx = int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16) % n
        chunks[idx].append(row)
    return [c for c in chunks if c]


def worker_run(spec: dict) -> Tuple[int, int]:
    label = "[%s/%s] " % (spec["worker_id"], spec["workers"])
    chunk = spec.get("plan_chunk") or []
    if not chunk:
        return 0, 0
    time.sleep(spec["worker_id"] * 2)
    cfg = load_env(Path(spec["env"]))
    tgt = connect_target(cfg)
    try:
        print(
            "%sstart rows=%s first=%s last=%s"
            % (
                label,
                len(chunk),
                chunk[0]["application_no"],
                chunk[-1]["application_no"],
            ),
            flush=True,
        )
        ok, skip = run_apply_chunk(
            tgt,
            chunk,
            spec["commit_every"],
            spec["log_every"],
            spec["batch_size"],
            label,
        )
        print("%sdone ok=%s skip=%s" % (label, ok, skip), flush=True)
        return ok, skip
    finally:
        tgt.close()


def run_parallel(
    plan: List[dict],
    workers: int,
    env_path: str,
    commit_every: int,
    log_every: int,
    batch_size: int,
) -> Tuple[int, int]:
    workers = min(max(1, workers), 4)
    chunks = split_chunks(plan, workers)
    specs = []
    for i, chunk in enumerate(chunks):
        specs.append(
            {
                "worker_id": i,
                "workers": workers,
                "env": env_path,
                "commit_every": commit_every,
                "log_every": log_every,
                "batch_size": batch_size,
                "plan_chunk": chunk,
            }
        )
    print("parallel workers=%s chunks=%s rows=%s" % (workers, len(specs), len(plan)), flush=True)
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(processes=len(specs)) as pool:
        results = pool.map(worker_run, specs)
    ok = sum(r[0] for r in results)
    skip = sum(r[1] for r in results)
    return ok, skip


def apply_plan(
    tgt,
    plan: List[dict],
    workers: int,
    env_path: str,
    commit_every: int,
    log_every: int,
    batch_size: int,
) -> Tuple[int, int]:
    if workers <= 1:
        return run_apply_chunk(
            tgt, plan, commit_every, log_every, batch_size
        )
    tgt.close()
    return run_parallel(
        plan, workers, env_path, commit_every, log_every, batch_size
    )


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
        help="application 侧 status 条件，默认 20；空=不限",
    )
    p.add_argument(
        "--loan-status",
        default="",
        help="只取这些 loan.status，逗号分隔，如 23,27；空=全部",
    )
    p.add_argument("--scan-chunk", type=int, default=500, help="分页 LIMIT 大小")
    p.add_argument("--workers", type=int, default=1, help="并发进程数，建议 1~2（经代理易 2013）")
    p.add_argument("--batch-size", type=int, default=25, help="每条 SQL 批量 UPDATE 行数")
    p.add_argument("--commit-every", type=int, default=100)
    p.add_argument("--log-every", type=int, default=500)
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    dry_run = not args.apply
    loan_statuses = parse_status_list(args.loan_status)
    app_status_filter = args.app_status.strip() if args.app_status.strip() else None

    cfg = load_env(Path(args.env))
    tgt = connect_target(cfg)
    t0 = time.time()
    env_path = str(Path(args.env))
    try:
        print(
            "due_before=%s app_status=%s loan_status=%s workers=%s dry_run=%s"
            % (
                args.due_before,
                app_status_filter if app_status_filter is not None else "ANY",
                loan_statuses or "ALL",
                args.workers,
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
        if args.workers > 1:
            tgt.close()
            tgt = None
            ok, skip = run_parallel(
                plan,
                args.workers,
                env_path,
                args.commit_every,
                args.log_every,
                args.batch_size,
            )
        else:
            ok, skip = run_apply_chunk(
                tgt,
                plan,
                args.commit_every,
                args.log_every,
                args.batch_size,
            )
        print(
            "done updated=%s skip=%s elapsed=%.1fs"
            % (ok, skip, time.time() - t0),
            flush=True,
        )
        return 0
    finally:
        if tgt is not None:
            tgt.close()


if __name__ == "__main__":
    raise SystemExit(main())
