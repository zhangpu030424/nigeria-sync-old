#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""将 application.status 与 loan.status 对齐。

查不一致:
  SELECT l.application_no, l.status, a.status
  FROM loan l JOIN application a ON a.application_no = l.application_no
  WHERE l.due_date <= '2026-07-05' AND a.status = 20 AND l.status <> a.status;

经 8001 代理 pymysql UPDATE 易卡死 → 推荐 --sql-out 生成 SQL，用 mysql 客户端执行。

Usage:
  # 1. 建 plan（或已有 json 跳过）
  python3 sync_application_status_from_loan.py --env ./ng_migration.env --dry-run \\
    --plan-file /tmp/sync_app_status_plan.json

  # 2a. 导出 SQL（不连库写）
  python3 sync_application_status_from_loan.py \\
    --plan-file /tmp/sync_app_status_plan.json --sql-out /tmp/sync_app_status.sql

  # 2b. mysql 客户端执行（可直连目标库时）
  mysql -h... -u... -p ng < /tmp/sync_app_status.sql

  # 2c. 或仍用 pymysql（短超时）
  python3 sync_application_status_from_loan.py --env ./ng_migration.env --apply-only \\
    --plan-file /tmp/sync_app_status_plan.json
"""
import argparse
import json
import subprocess
import sys
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


def connect_target(cfg: Dict[str, str], for_apply: bool = False):
    return pymysql.connect(
        host=cfg["TARGET_HOST"],
        port=int(cfg.get("TARGET_PORT", "3306")),
        user=cfg["TARGET_USER"],
        password=cfg["TARGET_PASSWORD"],
        database=cfg.get("TARGET_DB", "ng"),
        charset="utf8mb4",
        cursorclass=DictCursor,
        connect_timeout=10,
        read_timeout=30 if for_apply else 3600,
        write_timeout=30 if for_apply else 3600,
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


def _sql_escape(s: str) -> str:
    return str(s).replace("\\", "\\\\").replace("'", "''")


def save_plan(path: Path, plan: List[dict]) -> None:
    path.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")


def load_plan(path: Path) -> List[dict]:
    return json.loads(path.read_text(encoding="utf-8"))


def plan_stats(plan: List[dict]) -> Dict[int, int]:
    by_loan: Dict[int, int] = defaultdict(int)
    for row in plan:
        by_loan[int(row["loan_status"])] += 1
    return dict(by_loan)


def write_sql_file(path: Path, plan: List[dict], sql_batch: int, db: str) -> None:
    lines = ["-- sync application.status from loan, rows=%s" % len(plan)]
    if db:
        lines.append("USE `%s`;" % _sql_escape(db))
    for i in range(0, len(plan), sql_batch):
        part = plan[i : i + sql_batch]
        lines.append("START TRANSACTION;")
        for row in part:
            app_no = _sql_escape(row["application_no"])
            lines.append(
                "UPDATE application SET status=%s "
                "WHERE application_no='%s' AND status=%s;"
                % (int(row["loan_status"]), app_no, int(row["app_status"]))
            )
        lines.append("COMMIT;")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_mysql_client(cfg: Dict[str, str], sql_path: Path) -> int:
    cmd = [
        "mysql",
        "-h",
        cfg["TARGET_HOST"],
        "-P",
        str(cfg.get("TARGET_PORT", "3306")),
        "-u",
        cfg["TARGET_USER"],
        f"-p{cfg['TARGET_PASSWORD']}",
        cfg.get("TARGET_DB", "ng"),
    ]
    print("exec mysql client: %s < %s" % (" ".join(cmd[:6]), sql_path), flush=True)
    with sql_path.open("rb") as f:
        proc = subprocess.run(cmd, stdin=f, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.stdout:
        sys.stdout.buffer.write(proc.stdout)
    if proc.stderr:
        sys.stderr.buffer.write(proc.stderr)
    return int(proc.returncode)


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
) -> List[dict]:
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
            }
            prev = plan_by_app.get(app_no)
            item = dict(cur)
            item["_rank"] = _row_rank(row)
            if prev is None or item["_rank"] >= prev.get("_rank", ()):
                plan_by_app[app_no] = item
        print(
            "mismatch page rows=%s plan=%s last=%s"
            % (total_rows, len(plan_by_app), after[-30:]),
            flush=True,
        )
        if len(rows) < scan_chunk:
            break
    return [
        {
            "application_no": v["application_no"],
            "app_status": v["app_status"],
            "loan_status": v["loan_status"],
        }
        for v in plan_by_app.values()
    ]


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
    start_offset: int = 0,
) -> Tuple[int, int]:
    if start_offset:
        plan = plan[start_offset:]
    sys.stdout.flush()
    print(
        "apply start rows=%s commit_every=%s sleep_ms=%s"
        % (len(plan), commit_every, sleep_ms),
        flush=True,
    )
    if not plan:
        return 0, 0
    print(
        "apply first=%s app=%s -> loan=%s"
        % (plan[0]["application_no"], plan[0]["app_status"], plan[0]["loan_status"]),
        flush=True,
    )
    print("connecting target for apply (timeout 30s) ...", flush=True)
    tgt = connect_target(cfg, for_apply=True)
    print("connected, updating ...", flush=True)
    ok = skip = 0
    pending = 0
    try:
        for i, row in enumerate(plan, 1):
            n = 0
            for attempt in range(5):
                try:
                    n = apply_one(tgt, row)
                    if i == 1:
                        print("first row rowcount=%s" % n, flush=True)
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
                    if attempt >= 4:
                        raise
                    wait = 3 + attempt * 3
                    print(
                        "update %s retry err=%s wait=%ss"
                        % (row["application_no"], exc, wait),
                        flush=True,
                    )
                    time.sleep(wait)
                    tgt = connect_target(cfg, for_apply=True)
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
            if i <= 5 or i % max(1, log_every) == 0 or i == len(plan):
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


def resolve_plan(args, cfg) -> List[dict]:
    plan_path = Path(args.plan_file) if args.plan_file.strip() else None
    skip_scan = args.apply_only or args.sql_out or (
        plan_path is not None and plan_path.is_file() and not args.rebuild_plan
    )
    if skip_scan and plan_path is not None and plan_path.is_file():
        plan = load_plan(plan_path)
        print("loaded plan from %s rows=%s" % (plan_path, len(plan)), flush=True)
        return plan
    if args.apply_only or args.sql_out:
        raise SystemExit("plan file missing: %s" % plan_path)
    tgt = connect_target(cfg)
    try:
        print(
            "scan mismatch due_before=%s app_status=%s"
            % (args.due_before, args.app_status or "ANY"),
            flush=True,
        )
        loan_statuses = parse_status_list(args.loan_status)
        app_status_filter = (
            args.app_status.strip() if args.app_status.strip() else None
        )
        plan = exec_with_retry(
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
        return plan
    finally:
        tgt.close()


def main(argv: Optional[list] = None) -> int:
    p = argparse.ArgumentParser(description="Sync application.status from loan.status")
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument("--apply-only", action="store_true", help="只读 plan 文件并写入")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--due-before", default="2026-07-05")
    p.add_argument("--app-status", default="20")
    p.add_argument("--loan-status", default="")
    p.add_argument("--scan-chunk", type=int, default=500)
    p.add_argument("--plan-file", default="")
    p.add_argument("--rebuild-plan", action="store_true")
    p.add_argument("--sql-out", default="", help="从 plan 生成 SQL 文件，不写库")
    p.add_argument("--sql-batch", type=int, default=50, help="SQL 文件每批 COMMIT 条数")
    p.add_argument(
        "--exec-mysql",
        action="store_true",
        help="生成 SQL 后用本机 mysql 客户端执行（需已安装 mysql）",
    )
    p.add_argument("--commit-every", type=int, default=20)
    p.add_argument("--log-every", type=int, default=10)
    p.add_argument("--start-offset", type=int, default=0)
    p.add_argument("--sleep-ms", type=int, default=50)
    args = p.parse_args(argv)
    if sum([args.apply, args.apply_only, args.dry_run, bool(args.sql_out)]) > 1:
        p.error("use only one of --apply / --apply-only / --dry-run / --sql-out")
    if args.apply_only and not args.plan_file.strip():
        p.error("--apply-only requires --plan-file")
    if args.sql_out and not args.plan_file.strip():
        p.error("--sql-out requires --plan-file")

    cfg = load_env(Path(args.env))
    t0 = time.time()
    plan = resolve_plan(args, cfg)
    by_loan = plan_stats(plan)
    print("by_loan_status=%s would_update=%s" % (by_loan, len(plan)), flush=True)
    for row in plan[:5]:
        print(
            "  sample %s app=%s -> loan=%s"
            % (row["application_no"], row["app_status"], row["loan_status"]),
            flush=True,
        )

    if args.sql_out:
        out = Path(args.sql_out)
        write_sql_file(out, plan, args.sql_batch, cfg.get("TARGET_DB", "ng"))
        print("wrote sql rows=%s file=%s" % (len(plan), out), flush=True)
        if args.exec_mysql:
            rc = run_mysql_client(cfg, out)
            if rc != 0:
                return rc
            print("mysql client done elapsed=%.1fs" % (time.time() - t0), flush=True)
        return 0

    if args.dry_run:
        print("dry-run done would_update=%s" % len(plan), flush=True)
        return 0

    if not args.apply and not args.apply_only:
        return 0

    ok, skip = run_apply(
        cfg,
        plan,
        args.commit_every,
        args.log_every,
        args.sleep_ms,
        args.start_offset,
    )
    print(
        "done updated=%s skip=%s elapsed=%.1fs" % (ok, skip, time.time() - t0),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
