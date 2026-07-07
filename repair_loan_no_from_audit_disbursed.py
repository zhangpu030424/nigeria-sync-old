#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按 audit_loan_disbursed 的 fix_plan 批量修复 loan_no（market 长号 → core sn）。

输入（二选一）:
  --plan-file /tmp/loan_audit_fix_plan.json
    audit_loan_disbursed 生成的 JSON，字段:
      application_no, from_loan_no, to_loan_no, period, roll_sequence

  --issues-csv /tmp/loan_audit_issues_core_sn.csv
    仅处理 issue=wrong_loan_no 且 loan_count=1 的行

修复逻辑:
  UPDATE loan SET loan_no = to_loan_no
  WHERE loan_no = from_loan_no AND application_no = ? AND period = ? AND roll_sequence = ?
  不改 application_no；主键冲突时 fallback 逐条（正确行已存在则删错误行，不覆盖）。

Usage:
  # 试跑 100 条
  python3 repair_loan_no_from_audit_disbursed.py --env ./ng_migration.env \\
    --plan-file /tmp/loan_audit_fix_plan.json --dry-run --work-limit 100

  # 全量（多进程批更新）
  python3 repair_loan_no_from_audit_disbursed.py --env ./ng_migration.env \\
    --apply-only --plan-file /tmp/loan_audit_fix_plan.json \\
    --workers 8 --batch-size 200

  # 导出 SQL
  python3 repair_loan_no_from_audit_disbursed.py \\
    --plan-file /tmp/loan_audit_fix_plan.json --sql-out /tmp/repair_loan_no_core_sn.sql
"""
import argparse
import csv
import hashlib
import json
import multiprocessing
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pymysql
from pymysql.cursors import DictCursor

from repair_loan_no_from_audit import (
    CommitTracker,
    RepairAuditLog,
    RowChangeAuditLog,
    exec_with_retry,
    fetch_loan_row,
    loan_exists,
    repair_one_loan,
)

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
        read_timeout=120 if for_apply else 3600,
        write_timeout=120 if for_apply else 3600,
        autocommit=False,
    )


def _sql_escape(s: str) -> str:
    return str(s).replace("\\", "\\\\").replace("'", "''")


def normalize_plan_row(row: dict) -> Optional[dict]:
    app_no = str(
        row.get("application_no") or row.get("actual_application_no") or ""
    ).strip()
    from_ln = str(
        row.get("from_loan_no") or row.get("actual_loan_no") or ""
    ).strip()
    to_ln = str(
        row.get("to_loan_no") or row.get("expected_loan_no") or ""
    ).strip()
    if not from_ln or not to_ln or from_ln == to_ln:
        return None
    if not app_no:
        return None
    return {
        "application_no": app_no,
        "from_loan_no": from_ln,
        "to_loan_no": to_ln,
        "period": int(row.get("period") or row.get("actual_period") or 1),
        "roll_sequence": int(
            row.get("roll_sequence") or row.get("actual_roll_sequence") or 0
        ),
    }


def load_plan_json(path: Path) -> List[dict]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("plan json must be a list")
    return raw


def load_plan_issues_csv(path: Path) -> List[dict]:
    out: List[dict] = []
    with path.open(encoding="utf-8", newline="") as fp:
        for row in csv.DictReader(fp):
            if str(row.get("issue") or "").strip() != "wrong_loan_no":
                continue
            if int(row.get("loan_count") or 0) != 1:
                continue
            norm = normalize_plan_row(row)
            if norm:
                out.append(norm)
    return out


def dedupe_plan(rows: List[dict]) -> Tuple[List[dict], Dict[str, int]]:
    skipped: Dict[str, int] = {}
    out: List[dict] = []
    seen_from = set()
    for row in rows:
        norm = normalize_plan_row(row)
        if not norm:
            skipped["bad_row"] = skipped.get("bad_row", 0) + 1
            continue
        key = (
            norm["from_loan_no"],
            norm["application_no"],
            norm["period"],
            norm["roll_sequence"],
        )
        if key in seen_from:
            skipped["dup_from"] = skipped.get("dup_from", 0) + 1
            continue
        seen_from.add(key)
        out.append(norm)
    out.sort(key=lambda r: r["from_loan_no"])
    return out, skipped


def write_sql_file(path: Path, plan: List[dict], batch: int) -> None:
    lines = ["-- repair_loan_no_from_audit_disbursed rows=%s" % len(plan)]
    for i in range(0, len(plan), max(1, batch)):
        part = plan[i : i + batch]
        lines.append("START TRANSACTION;")
        for row in part:
            lines.append(
                "UPDATE loan SET loan_no='%s' "
                "WHERE loan_no='%s' AND application_no='%s' "
                "AND period=%s AND roll_sequence=%s;"
                % (
                    _sql_escape(row["to_loan_no"]),
                    _sql_escape(row["from_loan_no"]),
                    _sql_escape(row["application_no"]),
                    int(row["period"]),
                    int(row["roll_sequence"]),
                )
            )
        lines.append("COMMIT;")
        lines.append("")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def apply_batch_update(tgt, rows: List[dict]) -> int:
    if not rows:
        return 0
    parts: List[str] = []
    params: List = []
    for r in rows:
        parts.append(
            "SELECT %s AS from_ln, %s AS to_ln, %s AS app_no, %s AS period, %s AS roll"
        )
        params.extend(
            [
                r["from_loan_no"],
                r["to_loan_no"],
                r["application_no"],
                int(r["period"]),
                int(r["roll_sequence"]),
            ]
        )
    sql = (
        """
        UPDATE loan l
        INNER JOIN (
        """
        + " UNION ALL ".join(parts)
        + """
        ) x ON l.loan_no = x.from_ln
           AND l.application_no = x.app_no
           AND l.period = x.period
           AND l.roll_sequence = x.roll
        SET l.loan_no = x.to_ln
        """
    )
    with tgt.cursor() as cur:
        cur.execute(sql, tuple(params))
        return int(cur.rowcount or 0)


def apply_one_row(
    tgt,
    row: dict,
    dry_run: bool,
    tracker: Optional[CommitTracker],
    audit: RepairAuditLog,
    row_audit: Optional[RowChangeAuditLog],
) -> str:
    repair_row = {
        "wrong_loan_no": row["from_loan_no"],
        "correct_loan_no": row["to_loan_no"],
        "application_no": row["application_no"],
        "legacy_loan_no": "",
        "app_id": "",
    }
    from_ln = row["from_loan_no"]
    to_ln = row["to_loan_no"]
    app_no = row["application_no"]
    period = int(row["period"])
    roll = int(row["roll_sequence"])

    if not loan_exists(tgt, from_ln):
        if loan_exists(tgt, to_ln):
            audit.record("skip_done", repair_row, "from_missing_to_exists")
            return "skip_done"
        audit.record("skip", repair_row, "from_missing")
        return "skip_missing"

    before = fetch_loan_row(tgt, from_ln)
    if before:
        if str(before.get("application_no") or "").strip() != app_no:
            audit.record("skip", repair_row, "app_mismatch")
            return "skip_app_mismatch"
        if int(before.get("period") or 0) != period:
            audit.record("skip", repair_row, "period_mismatch")
            return "skip_period_mismatch"
        if int(before.get("roll_sequence") or 0) != roll:
            audit.record("skip", repair_row, "roll_mismatch")
            return "skip_roll_mismatch"

    if loan_exists(tgt, to_ln):
        return repair_one_loan(
            tgt,
            {
                "wrong_loan_no": from_ln,
                "correct_loan_no": to_ln,
                "application_no": "",
                "legacy_loan_no": "",
                "app_id": "",
            },
            dry_run,
            "update",
            audit,
            row_audit,
            tracker,
        )

    if dry_run:
        if before and row_audit:
            after = dict(before)
            after["loan_no"] = to_ln
            row_audit.record_modified("would_update", before, after)
        audit.record("would_update", repair_row, "update_loan_no")
        return "ok"

    with tgt.cursor() as cur:
        cur.execute(
            """
            UPDATE loan SET loan_no=%s
            WHERE loan_no=%s AND application_no=%s
              AND period=%s AND roll_sequence=%s
            """,
            (to_ln, from_ln, app_no, period, roll),
        )
        if not cur.rowcount:
            audit.record("skip", repair_row, "update_no_row")
            return "skip_no_row"
    after = fetch_loan_row(tgt, to_ln) or dict(before or {}, loan_no=to_ln)
    if row_audit and before:
        row_audit.record_modified("update", before, after)
    audit.record("update", repair_row, "update_loan_no")
    if tracker:
        tracker.note_write()
    else:
        tgt.commit()
    return "ok"


def run_batch_chunk(
    cfg: Dict[str, str],
    plan: List[dict],
    batch_size: int,
    dry_run: bool,
    repair_log: str,
    prefix: str = "",
) -> Tuple[int, int]:
    if batch_size <= 0:
        batch_size = 200
    tgt = connect_target(cfg, for_apply=True)
    audit = RepairAuditLog(repair_log or None, enabled=bool(repair_log))
    row_audit = RowChangeAuditLog(repair_log or "", enabled=bool(repair_log))
    ok = skip = 0
    total_batches = (len(plan) + batch_size - 1) // batch_size if plan else 0
    try:
        for bi in range(0, len(plan), batch_size):
            part = plan[bi : bi + batch_size]
            bno = bi // batch_size + 1
            if dry_run:
                ok += len(part)
                print(
                    "%sbatch %s/%s would_update=%s"
                    % (prefix, bno, total_batches, len(part)),
                    flush=True,
                )
                continue
            try:
                n = exec_with_retry(
                    tgt,
                    lambda p=part: apply_batch_update(tgt, p),
                    "%sbatch %s" % (prefix, bno),
                )
                tgt.commit()
                ok += n
                skip += len(part) - n
                print(
                    "%sbatch %s/%s updated=%s batch_rows=%s total_ok=%s skip=%s"
                    % (prefix, bno, total_batches, n, len(part), ok, skip),
                    flush=True,
                )
            except pymysql.err.IntegrityError as exc:
                tgt.rollback()
                print(
                    "%sbatch %s/%s integrity_error=%s fallback row-by-row"
                    % (prefix, bno, total_batches, exc),
                    flush=True,
                )
                tracker = CommitTracker(tgt, 20, dry_run=False)
                for row in part:
                    st = exec_with_retry(
                        tgt,
                        lambda r=row: apply_one_row(
                            tgt, r, False, tracker, audit, row_audit
                        ),
                        "row %s" % row["from_loan_no"],
                    )
                    if st == "ok":
                        ok += 1
                    else:
                        skip += 1
                tracker.flush()
    finally:
        audit.close()
        row_audit.close()
        tgt.close()
    return ok, skip


def split_chunks(rows: List[dict], workers: int) -> List[List[dict]]:
    n = max(1, int(workers))
    chunks: List[List[dict]] = [[] for _ in range(n)]
    for row in rows:
        key = str(row.get("from_loan_no") or "")
        idx = int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16) % n
        chunks[idx].append(row)
    return [c for c in chunks if c]


def _worker_repair_log(base: str, worker_id: int) -> str:
    if not base:
        return ""
    p = Path(base)
    return str(p.with_name("%s.w%s%s" % (p.stem, worker_id, p.suffix or ".csv")))


def batch_worker_run(spec: dict) -> Tuple[int, int]:
    label = "[%s/%s] " % (spec["worker_id"], spec["workers"])
    chunk = spec.get("plan_chunk") or []
    if not chunk:
        return 0, 0
    cfg = load_env(Path(spec["env"]))
    print(
        "%sstart rows=%s batch_size=%s first=%s"
        % (label, len(chunk), spec["batch_size"], chunk[0]["from_loan_no"]),
        flush=True,
    )
    ok, skip = run_batch_chunk(
        cfg,
        chunk,
        spec["batch_size"],
        spec["dry_run"],
        spec.get("repair_log") or "",
        label,
    )
    print("%sdone ok=%s skip=%s" % (label, ok, skip), flush=True)
    return ok, skip


def run_parallel_batch(
    plan: List[dict],
    workers: int,
    env_path: str,
    batch_size: int,
    dry_run: bool,
    repair_log: str,
) -> Tuple[int, int]:
    workers = min(max(1, int(workers)), 16)
    chunks = split_chunks(plan, workers)
    specs = []
    for i, chunk in enumerate(chunks):
        if not chunk:
            continue
        specs.append(
            {
                "worker_id": i,
                "workers": workers,
                "env": env_path,
                "dry_run": dry_run,
                "batch_size": batch_size,
                "plan_chunk": chunk,
                "repair_log": _worker_repair_log(repair_log, i),
            }
        )
    print(
        "parallel_batch workers=%s rows=%s batch_size=%s"
        % (len(specs), len(plan), batch_size),
        flush=True,
    )
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(processes=len(specs)) as pool:
        results = pool.map(batch_worker_run, specs)
    ok = sum(r[0] for r in results)
    skip = sum(r[1] for r in results)
    print("parallel_batch done ok=%s skip=%s" % (ok, skip), flush=True)
    return ok, skip


def print_plan_preview(plan: List[dict], limit: int) -> None:
    n = min(max(1, int(limit)), len(plan))
    print("preview %s/%s rows:" % (n, len(plan)), flush=True)
    print(
        "application_no | from_loan_no -> to_loan_no | period | roll",
        flush=True,
    )
    print("-" * 100, flush=True)
    for row in plan[:n]:
        print(
            "%s | %s -> %s | %s | %s"
            % (
                row["application_no"],
                row["from_loan_no"],
                row["to_loan_no"],
                row["period"],
                row["roll_sequence"],
            ),
            flush=True,
        )


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Fix loan_no from audit_loan_disbursed fix_plan (core sn)"
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument(
        "--apply-only",
        action="store_true",
        help="只读 plan 并写库，不再从 CSV 重建",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--plan-file",
        default="/tmp/loan_audit_fix_plan.json",
        help="audit 生成的 fix_plan JSON",
    )
    p.add_argument(
        "--issues-csv",
        default="",
        help="或从 issues CSV 读取 wrong_loan_no",
    )
    p.add_argument("--sql-out", default="", help="导出 UPDATE SQL")
    p.add_argument("--sql-batch", type=int, default=50)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--batch-size", type=int, default=200)
    p.add_argument("--work-limit", type=int, default=0)
    p.add_argument(
        "--preview",
        type=int,
        default=0,
        metavar="N",
        help="只打印前 N 条修复计划，不连库、不写文件",
    )
    p.add_argument(
        "--repair-log",
        default="",
        help="REPAIR_AUDIT 日志（默认 /tmp/repair_loan_no_core_sn_*.csv）",
    )
    args = p.parse_args(argv)

    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run, not both")
    if args.apply_only:
        args.apply = True
    dry_run = not args.apply

    raw_rows: List[dict] = []
    if args.issues_csv:
        raw_rows = load_plan_issues_csv(Path(args.issues_csv))
        print("loaded issues_csv=%s rows=%s" % (args.issues_csv, len(raw_rows)), flush=True)
    elif Path(args.plan_file).exists():
        raw_rows = load_plan_json(Path(args.plan_file))
        print("loaded plan_file=%s rows=%s" % (args.plan_file, len(raw_rows)), flush=True)
    else:
        print("plan not found: %s (use --issues-csv or run audit first)" % args.plan_file)
        return 1

    plan, skipped = dedupe_plan(raw_rows)
    if skipped:
        print(
            "plan_skipped %s"
            % " ".join("%s=%s" % (k, v) for k, v in sorted(skipped.items())),
            flush=True,
        )
    if args.work_limit > 0:
        plan = plan[: args.work_limit]
    print("repair_plan=%s" % len(plan), flush=True)

    if args.preview > 0:
        print_plan_preview(plan, args.preview)
        return 0

    for row in plan[:5]:
        print(
            "  %s -> %s app=%s"
            % (row["from_loan_no"], row["to_loan_no"], row["application_no"]),
            flush=True,
        )
    if len(plan) > 5:
        print("  ... and %s more" % (len(plan) - 5), flush=True)

    if args.sql_out:
        write_sql_file(Path(args.sql_out), plan, args.sql_batch)
        print("wrote sql_out=%s rows=%s" % (args.sql_out, len(plan)), flush=True)

    if not plan:
        return 1

    if not (dry_run or args.apply or args.apply_only):
        return 0

    repair_log = args.repair_log
    if not repair_log and (args.apply or args.apply_only):
        repair_log = "/tmp/repair_loan_no_core_sn_%s.csv" % datetime.now().strftime(
            "%Y%m%d_%H%M%S"
        )
    print(
        "start mode=%s workers=%s batch_size=%s repair_log=%s"
        % (
            "DRY_RUN" if dry_run else "APPLY",
            args.workers,
            args.batch_size,
            repair_log or "(off)",
        ),
        flush=True,
    )
    if args.workers > 1:
        ok, skip = run_parallel_batch(
            plan,
            args.workers,
            args.env,
            args.batch_size,
            dry_run,
            repair_log,
        )
    else:
        cfg = load_env(Path(args.env))
        ok, skip = run_batch_chunk(
            cfg,
            plan,
            args.batch_size,
            dry_run,
            repair_log,
        )
    print("finished ok=%s skip=%s" % (ok, skip), flush=True)
    return 0 if ok or skip else 1


if __name__ == "__main__":
    raise SystemExit(main())
