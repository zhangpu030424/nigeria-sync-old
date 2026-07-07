#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""用目标库 application.sn 批量修复 loan.application_no 不一致。

适用：loan_no 已是短号，但 application_no 仍为 ng20626817-... 等异常前缀，
而 application 表上同 sn 的行已是 ng0515-... 正确格式。

等价 SQL:
  UPDATE loan l
  INNER JOIN application a
    ON a.sn = SUBSTRING_INDEX(SUBSTRING_INDEX(l.loan_no, '-', 2), '-', -1)
  SET l.application_no = a.application_no
  WHERE l.application_no <> a.application_no
    AND l.application_no REGEXP '^ng[0-9]{5,}-';

Usage:
  python3 repair_loan_app_no_from_application.py --env ./ng_migration.env --dry-run
  python3 repair_loan_app_no_from_application.py --env ./ng_migration.env --dry-run \\
    --plan-file /tmp/fix_app_no_plan.json --sql-out /tmp/fix_app_no.sql

  python3 repair_loan_app_no_from_application.py --env ./ng_migration.env --apply-only \\
    --plan-file /tmp/fix_app_no_plan.json --batch-size 200 --workers 4

  # 内存批更新（默认 workers=4）：plan 已在 json，每 worker 多批 JOIN UPDATE
"""
import argparse
import hashlib
import json
import multiprocessing
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pymysql
from pymysql.cursors import DictCursor

from repair_loan_no_from_audit import CommitTracker, exec_with_retry

HERE = Path(__file__).resolve().parent
LOAN_NO_RE = re.compile(r"^[Nn][Gg]-(\d+)-(\d{5})$")
BAD_APP_PREFIX_RE = re.compile(r"^ng\d{5,}-", re.I)


def _sql_escape(s: str) -> str:
    return str(s).replace("\\", "\\\\").replace("'", "''")


def save_plan(path: Path, plan: List[dict]) -> None:
    path.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")


def load_plan(path: Path) -> List[dict]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_sql_file(path: Path, plan: List[dict], sql_batch: int) -> None:
    lines = ["-- fix loan.application_no from application.sn, rows=%s" % len(plan)]
    for i in range(0, len(plan), sql_batch):
        part = plan[i : i + sql_batch]
        lines.append("START TRANSACTION;")
        for row in part:
            good = _sql_escape(row["good_application_no"])
            bad = _sql_escape(row["bad_application_no"])
            loan_no = _sql_escape(row["loan_no"])
            lines.append(
                "UPDATE loan SET application_no='%s' "
                "WHERE loan_no='%s' AND application_no='%s';"
                % (good, loan_no, bad)
            )
        lines.append("COMMIT;")
        lines.append("")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


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


def count_mismatch(tgt, bad_prefix_only: bool) -> int:
    sql = """
        SELECT COUNT(*) AS c
        FROM loan l
        INNER JOIN application a
          ON a.sn = SUBSTRING_INDEX(SUBSTRING_INDEX(l.loan_no, '-', 2), '-', -1)
        WHERE l.application_no <> a.application_no
          AND l.loan_no REGEXP '^[Nn][Gg]-[0-9]+-[0-9]{5}$'
    """
    if bad_prefix_only:
        sql += " AND l.application_no REGEXP '^ng[0-9]{5,}-'"
    with tgt.cursor() as cur:
        cur.execute(sql)
        return int(cur.fetchone()["c"])


def scan_mismatch_batch(
    tgt, after_loan_no: str, limit: int, bad_prefix_only: bool
) -> List[dict]:
    sql = """
        SELECT
            l.loan_no,
            l.application_no AS bad_application_no,
            l.period,
            l.roll_sequence,
            a.application_no AS good_application_no
        FROM loan l
        INNER JOIN application a
          ON a.sn = SUBSTRING_INDEX(SUBSTRING_INDEX(l.loan_no, '-', 2), '-', -1)
        WHERE l.loan_no > %s
          AND l.application_no <> a.application_no
          AND l.loan_no REGEXP '^[Nn][Gg]-[0-9]+-[0-9]{5}$'
    """
    params: List = [after_loan_no or ""]
    if bad_prefix_only:
        sql += " AND l.application_no REGEXP '^ng[0-9]{5,}-'"
    sql += " ORDER BY l.loan_no ASC LIMIT %s"
    params.append(limit)
    with tgt.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def fetch_all_mismatch(tgt, bad_prefix_only: bool) -> List[dict]:
    """一次查询拉全量到内存（约几千行）。"""
    sql = """
        SELECT
            l.loan_no,
            l.application_no AS bad_application_no,
            a.application_no AS good_application_no
        FROM loan l
        INNER JOIN application a
          ON a.sn = SUBSTRING_INDEX(SUBSTRING_INDEX(l.loan_no, '-', 2), '-', -1)
        WHERE l.application_no <> a.application_no
          AND l.loan_no REGEXP '^[Nn][Gg]-[0-9]+-[0-9]{5}$'
    """
    if bad_prefix_only:
        sql += " AND l.application_no REGEXP '^ng[0-9]{5,}-'"
    sql += " ORDER BY l.loan_no ASC"
    with tgt.cursor() as cur:
        cur.execute(sql)
        rows = list(cur.fetchall())
    plan: List[dict] = []
    for row in rows:
        bad = str(row["bad_application_no"] or "").strip()
        good = str(row["good_application_no"] or "").strip()
        if good and bad != good:
            plan.append(
                {
                    "loan_no": str(row["loan_no"]),
                    "bad_application_no": bad,
                    "good_application_no": good,
                }
            )
    return plan


def build_plan(
    tgt, scan_size: int, work_limit: int, bad_prefix_only: bool
) -> Tuple[List[dict], Dict[str, int]]:
    plan: List[dict] = []
    stats: Dict[str, int] = {"batches": 0}
    after = ""
    while True:
        try:
            tgt.ping(reconnect=True)
        except Exception:
            pass
        rows = exec_with_retry(
            tgt,
            lambda a=after: scan_mismatch_batch(tgt, a, scan_size, bad_prefix_only),
            "scan after=%s" % (after or "(start)"),
        )
        stats["batches"] += 1
        if not rows:
            break
        after = str(rows[-1]["loan_no"])
        for row in rows:
            bad = str(row["bad_application_no"] or "").strip()
            good = str(row["good_application_no"] or "").strip()
            if not good or bad == good:
                continue
            plan.append(
                {
                    "loan_no": str(row["loan_no"]),
                    "bad_application_no": bad,
                    "good_application_no": good,
                    "period": row.get("period", 1),
                    "roll_sequence": row.get("roll_sequence", 0),
                }
            )
            if work_limit and len(plan) >= work_limit:
                return plan, stats
        if stats["batches"] % 20 == 0:
            print(
                "scan batches=%s plan=%s last=%s"
                % (stats["batches"], len(plan), after),
                flush=True,
            )
        if len(rows) < scan_size:
            break
    return plan, stats


def loan_pk_exists(tgt, app_no: str, loan_no: str) -> bool:
    with tgt.cursor() as cur:
        cur.execute(
            "SELECT period, roll_sequence FROM loan WHERE loan_no=%s LIMIT 1",
            (loan_no,),
        )
        row = cur.fetchone()
    if not row:
        return False
    with tgt.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM loan
            WHERE application_no=%s AND period=%s AND roll_sequence=%s
              AND loan_no <> %s LIMIT 1
            """,
            (app_no, row["period"], row["roll_sequence"], loan_no),
        )
        return cur.fetchone() is not None


def apply_batch_update(tgt, rows: List[dict]) -> int:
    """一批行合成一条 UPDATE … JOIN (UNION ALL …)。"""
    if not rows:
        return 0
    parts: List[str] = []
    params: List = []
    for r in rows:
        parts.append("SELECT %s AS loan_no, %s AS bad_app, %s AS good_app")
        params.extend(
            [r["loan_no"], r["bad_application_no"], r["good_application_no"]]
        )
    sql = (
        """
        UPDATE loan l
        INNER JOIN (
        """
        + " UNION ALL ".join(parts)
        + """
        ) x ON l.loan_no = x.loan_no AND l.application_no = x.bad_app
        SET l.application_no = x.good_app
        """
    )
    with tgt.cursor() as cur:
        cur.execute(sql, tuple(params))
        return int(cur.rowcount or 0)


def run_apply_batch_chunk(
    cfg: Dict[str, str],
    plan: List[dict],
    batch_size: int,
    dry_run: bool,
    prefix: str = "",
) -> Tuple[int, int]:
    """单连接按批 UPDATE；plan 可为 worker 子集。"""
    if batch_size <= 0:
        batch_size = 200
    tgt = connect_target(cfg, for_apply=True)
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
                    "%sbatch update %s" % (prefix, bno),
                )
                tgt.commit()
                ok += n
                skip += len(part) - n
                print(
                    "%sbatch %s/%s updated=%s batch_rows=%s total_ok=%s"
                    % (prefix, bno, total_batches, n, len(part), ok),
                    flush=True,
                )
            except pymysql.err.IntegrityError:
                tgt.rollback()
                print(
                    "%sbatch %s/%s integrity_error fallback row-by-row"
                    % (prefix, bno, total_batches),
                    flush=True,
                )
                for row in part:
                    st, tgt = _apply_with_retry(
                        tgt, cfg, row, False, None, False
                    )
                    if st == "ok":
                        ok += 1
                    else:
                        skip += 1
                tgt.commit()
    finally:
        tgt.close()
    return ok, skip


def run_apply_batch(
    cfg: Dict[str, str],
    plan: List[dict],
    batch_size: int,
    dry_run: bool,
) -> Tuple[int, int]:
    return run_apply_batch_chunk(cfg, plan, batch_size, dry_run)


def batch_worker_run(spec: dict) -> Tuple[int, int]:
    label = "[%s/%s] " % (spec["worker_id"], spec["workers"])
    chunk = spec.get("plan_chunk") or []
    if not chunk:
        return 0, 0
    cfg = load_env(Path(spec["env"]))
    print(
        "%sstart rows=%s batch_size=%s"
        % (label, len(chunk), spec["batch_size"]),
        flush=True,
    )
    ok, skip = run_apply_batch_chunk(
        cfg,
        chunk,
        spec["batch_size"],
        spec["dry_run"],
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


def apply_one(
    tgt, row: dict, dry_run: bool, tracker: Optional[CommitTracker], check_pk: bool
) -> str:
    loan_no = row["loan_no"]
    good = row["good_application_no"]
    bad = row["bad_application_no"]
    if check_pk and loan_pk_exists(tgt, good, loan_no):
        return "skip_pk"
    if dry_run:
        return "ok"
    try:
        with tgt.cursor() as cur:
            cur.execute(
                """
                UPDATE loan SET application_no=%s
                WHERE loan_no=%s AND application_no=%s
                """,
                (good, loan_no, bad),
            )
            if not cur.rowcount:
                return "skip_no_row"
    except pymysql.err.IntegrityError:
        try:
            tgt.rollback()
        except Exception:
            pass
        return "skip_pk"
    if tracker:
        tracker.note_write()
    else:
        tgt.commit()
    return "ok"


def _apply_with_retry(
    tgt,
    cfg: Dict[str, str],
    row: dict,
    dry_run: bool,
    tracker: Optional[CommitTracker],
    check_pk: bool,
) -> Tuple[str, object]:
    conn = tgt
    for attempt in range(4):
        try:
            return apply_one(conn, row, dry_run, tracker, check_pk), conn
        except pymysql.Error:
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            if attempt >= 3:
                raise
            time.sleep(1 + attempt)
            conn = connect_target(cfg, for_apply=True)
    return "skip_no_row", conn


def run_apply_chunk(
    tgt,
    chunk: List[dict],
    cfg: Dict[str, str],
    dry_run: bool,
    commit_every: int,
    log_every: int,
    sleep_ms: int = 0,
    check_pk: bool = False,
    prefix: str = "",
) -> Tuple[int, int]:
    ok = skip = 0
    tracker = CommitTracker(tgt, commit_every, dry_run)
    conn = tgt
    for i, row in enumerate(chunk, 1):
        st, new_conn = _apply_with_retry(
            conn, cfg, row, dry_run, tracker, check_pk
        )
        if new_conn is not conn:
            conn = new_conn
            tracker.conn = conn
        if st == "ok":
            ok += 1
        else:
            skip += 1
        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)
        if i % max(1, log_every) == 0 or i == len(chunk):
            print(
                "%sprogress ok=%s skip=%s done=%s/%s last=%s"
                % (prefix, ok, skip, i, len(chunk), row["loan_no"]),
                flush=True,
            )
    tracker.flush()
    return ok, skip


def split_chunks(rows: List[dict], workers: int) -> List[List[dict]]:
    n = max(1, int(workers))
    chunks: List[List[dict]] = [[] for _ in range(n)]
    for row in rows:
        key = str(row.get("loan_no") or "")
        idx = int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16) % n
        chunks[idx].append(row)
    return [c for c in chunks if c]


def worker_run(spec: dict) -> Tuple[int, int]:
    label = "[%s/%s] " % (spec["worker_id"], spec["workers"])
    chunk = spec.get("plan_chunk") or []
    if not chunk:
        return 0, 0
    cfg = load_env(Path(spec["env"]))
    tgt = connect_target(cfg, for_apply=True)
    try:
        print("%sstart rows=%s" % (label, len(chunk)), flush=True)
        ok, skip = run_apply_chunk(
            tgt,
            chunk,
            cfg,
            spec["dry_run"],
            spec["commit_every"],
            spec["log_every"],
            spec.get("sleep_ms", 0),
            spec.get("check_pk", False),
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
    dry_run: bool,
    commit_every: int,
    log_every: int,
    sleep_ms: int,
    check_pk: bool,
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
                "commit_every": commit_every,
                "log_every": log_every,
                "sleep_ms": sleep_ms,
                "check_pk": check_pk,
                "plan_chunk": chunk,
            }
        )
    print("parallel workers=%s rows=%s" % (len(specs), len(plan)), flush=True)
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(processes=len(specs)) as pool:
        results = pool.map(worker_run, specs)
    ok = sum(r[0] for r in results)
    skip = sum(r[1] for r in results)
    print("parallel done ok=%s skip=%s" % (ok, skip), flush=True)
    return ok, skip


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Fix loan.application_no from target application.sn join"
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument(
        "--apply-only",
        action="store_true",
        help="只读 plan-file 并并发 UPDATE，不再扫库",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--all-mismatch",
        action="store_true",
        help="不限异常前缀，凡 loan<>application 都修",
    )
    p.add_argument("--scan-size", type=int, default=500)
    p.add_argument("--work-limit", type=int, default=0)
    p.add_argument("--workers", type=int, default=4, help="批/逐条模式并发进程数")
    p.add_argument("--batch-size", type=int, default=200, help="批更新每批行数，0=逐条")
    p.add_argument(
        "--row-by-row",
        action="store_true",
        help="逐条 UPDATE（慢），不用批 JOIN",
    )
    p.add_argument(
        "--load-all",
        action="store_true",
        help="扫 plan 时用一条大 SELECT 拉全量到内存",
    )
    p.add_argument("--commit-every", type=int, default=200)
    p.add_argument("--log-every", type=int, default=500)
    p.add_argument("--sleep-ms", type=int, default=0, help="每条 UPDATE 后休眠毫秒，0=不休眠")
    p.add_argument(
        "--check-pk",
        action="store_true",
        help="UPDATE 前查主键冲突（慢，默认关闭，冲突时靠 IntegrityError skip）",
    )
    p.add_argument(
        "--fast",
        action="store_true",
        help="batch-size>=500 workers=4",
    )
    p.add_argument("--plan-file", default="", help="保存/读取 plan json")
    p.add_argument("--rebuild-plan", action="store_true")
    p.add_argument("--sql-out", default="", help="导出逐条 UPDATE SQL（IDEA 分批执行）")
    p.add_argument("--sql-batch", type=int, default=50, help="每个事务几条 UPDATE")
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    if args.apply_only and not args.plan_file.strip():
        p.error("--apply-only requires --plan-file")
    if args.apply_only:
        args.apply = True
    if args.fast:
        args.batch_size = max(args.batch_size, 500)
        args.workers = max(args.workers, 4)
    if args.row_by_row:
        args.batch_size = 0
    dry_run = not args.apply
    bad_prefix_only = not args.all_mismatch
    plan_path = Path(args.plan_file) if args.plan_file.strip() else None

    cfg = load_env(Path(args.env))
    plan: List[dict] = []

    if args.apply_only:
        plan = load_plan(plan_path)
        print("apply-only loaded plan rows=%s" % len(plan), flush=True)
    else:
        use_cached = (
            plan_path is not None
            and plan_path.is_file()
            and not args.rebuild_plan
            and (args.sql_out or not dry_run)
        )
        tgt = connect_target(cfg)
        try:
            if use_cached:
                plan = load_plan(plan_path)
                print(
                    "loaded plan from %s rows=%s" % (plan_path, len(plan)),
                    flush=True,
                )
            else:
                if args.load_all:
                    print("load-all: one query fetch mismatch ...", flush=True)
                    plan = exec_with_retry(
                        tgt,
                        lambda: fetch_all_mismatch(tgt, bad_prefix_only),
                        "fetch all mismatch",
                    )
                    stats = {"batches": 1, "load_all": len(plan)}
                else:
                    cnt = count_mismatch(tgt, bad_prefix_only)
                    print(
                        "mismatch_count=%s bad_prefix_only=%s dry_run=%s"
                        % (cnt, bad_prefix_only, dry_run),
                        flush=True,
                    )
                    stats = {}
                    plan, stats = build_plan(
                        tgt, args.scan_size, args.work_limit, bad_prefix_only
                    )
                print("scan_stats=%s plan=%s" % (stats, len(plan)), flush=True)
                if plan_path is not None:
                    save_plan(plan_path, plan)
                    print("saved plan to %s" % plan_path, flush=True)
            for row in plan[:15]:
                print(
                    "  %s  %s -> %s"
                    % (
                        row["loan_no"],
                        row["bad_application_no"],
                        row["good_application_no"],
                    ),
                    flush=True,
                )
            if len(plan) > 15:
                print("  ... and %s more" % (len(plan) - 15), flush=True)
            if not plan:
                return 0
        finally:
            tgt.close()

    if not plan:
        return 0

    if args.sql_out:
        out = Path(args.sql_out)
        write_sql_file(out, plan, args.sql_batch)
        batches = (len(plan) + args.sql_batch - 1) // args.sql_batch
        print(
            "wrote sql rows=%s batches=%s file=%s"
            % (len(plan), batches, out),
            flush=True,
        )
        if not args.apply:
            return 0

    if dry_run:
        print("dry-run done plan=%s" % len(plan), flush=True)
        return 0

    env_path = str(Path(args.env).resolve())
    if args.batch_size > 0:
        if args.workers > 1:
            print(
                "apply parallel_batch workers=%s batch_size=%s rows=%s"
                % (args.workers, args.batch_size, len(plan)),
                flush=True,
            )
            ok, skip = run_parallel_batch(
                plan,
                args.workers,
                env_path,
                args.batch_size,
                dry_run,
            )
        else:
            print(
                "apply batch_mode batch_size=%s rows=%s"
                % (args.batch_size, len(plan)),
                flush=True,
            )
            ok, skip = run_apply_batch(cfg, plan, args.batch_size, dry_run)
    else:
        print(
            "apply row_mode workers=%s rows=%s commit_every=%s"
            % (args.workers, len(plan), args.commit_every),
            flush=True,
        )
        if args.workers > 1:
            ok, skip = run_parallel(
                plan,
                args.workers,
                env_path,
                dry_run,
                args.commit_every,
                args.log_every,
                args.sleep_ms,
                args.check_pk,
            )
        else:
            tgt = connect_target(cfg, for_apply=True)
            try:
                ok, skip = run_apply_chunk(
                    tgt,
                    plan,
                    cfg,
                    dry_run,
                    args.commit_every,
                    args.log_every,
                    args.sleep_ms,
                    args.check_pk,
                )
            finally:
                tgt.close()
    print("done ok=%s skip=%s" % (ok, skip), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
