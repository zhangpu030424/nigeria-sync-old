#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""将 application.status 与 loan.status 对齐（目标库 JOIN 一次 UPDATE）。

适用：repair_loan_status20 已把 loan 同步到 23/24/27，但 application 仍停在 20。

等价:
  UPDATE application a
  INNER JOIN loan l ON a.application_no = l.application_no
  SET a.status = l.status
  WHERE l.due_date <= '2026-07-05'
    AND a.status <> l.status;

Usage:
  python3 sync_application_status_from_loan.py --env ./ng_migration.env --dry-run
  python3 sync_application_status_from_loan.py --env ./ng_migration.env --apply
  python3 sync_application_status_from_loan.py --env ./ng_migration.env --apply \\
    --due-before 2026-07-05 --loan-status 23,27
"""
import argparse
import time
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


def _scope_where(loan_statuses: Optional[Sequence[str]]) -> Tuple[str, Tuple]:
    sql = """
        l.due_date <= %s
        AND l.application_no IS NOT NULL AND l.application_no <> ''
        AND a.status <> l.status
    """
    params: List = []
    if loan_statuses:
        ph = ",".join(["%s"] * len(loan_statuses))
        sql += " AND l.status IN (%s)" % ph
    return sql, tuple(params)


def count_plan(
    tgt, due_before: str, loan_statuses: Optional[Sequence[str]]
) -> Dict[str, int]:
    scope, extra_params = _scope_where(loan_statuses)
    params = (due_before,) + extra_params
    out: Dict[str, int] = {}
    with tgt.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM application a
            INNER JOIN loan l ON a.application_no = l.application_no
            WHERE """
            + scope,
            params,
        )
        out["would_update"] = int(cur.fetchone()["c"])
        cur.execute(
            """
            SELECT l.status AS loan_status, COUNT(*) AS c
            FROM application a
            INNER JOIN loan l ON a.application_no = l.application_no
            WHERE """
            + scope
            + " GROUP BY l.status ORDER BY c DESC",
            params,
        )
        out["by_loan_status"] = {
            int(r["loan_status"]): int(r["c"]) for r in cur.fetchall()
        }
        cur.execute(
            """
            SELECT COUNT(DISTINCT a.application_no) AS c
            FROM application a
            INNER JOIN loan l ON a.application_no = l.application_no
            WHERE """
            + scope,
            params,
        )
        out["distinct_application"] = int(cur.fetchone()["c"])
    return out


def sample_rows(
    tgt, due_before: str, loan_statuses: Optional[Sequence[str]], limit: int = 15
):
    scope, extra_params = _scope_where(loan_statuses)
    params = (due_before,) + extra_params + (limit,)
    with tgt.cursor() as cur:
        cur.execute(
            """
            SELECT a.application_no, a.status AS app_status, l.status AS loan_status
            FROM application a
            INNER JOIN loan l ON a.application_no = l.application_no
            WHERE """
            + scope
            + " ORDER BY l.status DESC, a.application_no ASC LIMIT %s",
            params,
        )
        return list(cur.fetchall())


def apply_update(
    tgt, due_before: str, loan_statuses: Optional[Sequence[str]]
) -> int:
    scope, extra_params = _scope_where(loan_statuses)
    params = (due_before,) + extra_params
    sql = (
        """
        UPDATE application a
        INNER JOIN loan l ON a.application_no = l.application_no
        SET a.status = l.status
        WHERE """
        + scope
    )
    with tgt.cursor() as cur:
        cur.execute(sql, params)
        n = int(cur.rowcount or 0)
    tgt.commit()
    return n


def main(argv: Optional[list] = None) -> int:
    p = argparse.ArgumentParser(
        description="Sync application.status from loan.status on target DB"
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--due-before", default="2026-07-05")
    p.add_argument(
        "--loan-status",
        default="",
        help="只处理这些 loan.status，逗号分隔，如 23,27；空=全部",
    )
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    dry_run = not args.apply
    loan_statuses = parse_status_list(args.loan_status)

    cfg = load_env(Path(args.env))
    tgt = connect_target(cfg)
    t0 = time.time()
    try:
        stats = exec_with_retry(
            tgt,
            lambda: count_plan(tgt, args.due_before, loan_statuses),
            "count plan",
        )
        print(
            "due_before=%s loan_status=%s dry_run=%s"
            % (args.due_before, loan_statuses or "ALL", dry_run),
            flush=True,
        )
        print("by_loan_status=%s" % stats.get("by_loan_status", {}), flush=True)
        print(
            "would_update=%s distinct_application=%s"
            % (stats.get("would_update", 0), stats.get("distinct_application", 0)),
            flush=True,
        )
        for row in sample_rows(tgt, args.due_before, loan_statuses):
            print(
                "  sample %s app=%s -> loan=%s"
                % (
                    row["application_no"],
                    row["app_status"],
                    row["loan_status"],
                ),
                flush=True,
            )
        if dry_run:
            return 0
        n = exec_with_retry(
            tgt,
            lambda: apply_update(tgt, args.due_before, loan_statuses),
            "sync application.status",
        )
        print("done updated=%s elapsed=%.1fs" % (n, time.time() - t0), flush=True)
        return 0
    finally:
        tgt.close()


if __name__ == "__main__":
    raise SystemExit(main())
