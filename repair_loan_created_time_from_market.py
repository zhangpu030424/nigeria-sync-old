#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""修复补数 loan.created_time：占位值 → market.disburseTime（毫秒）。

流程（按用户要求）：
  1. 多进程从目标库拉 loan 主键行到内存（可写 cache）
  2. 多线程/分批查源库 ng_loan_market.application.disburseTime
  3. 生成逐行修复 plan（含 PK）
  4. 多进程并发 UPDATE 目标库

规则：
  new_created_time = disburseTime * 1000   # 源为 Unix 秒
  默认只修 created_time = 1785340800000

Usage:
  python3 repair_loan_created_time_from_market.py --env ./ng_migration.env --build-plan \\
    --cache-file /tmp/loan_marker_snapshot.jsonl \\
    --plan-file /tmp/fix_loan_created_time_plan.jsonl --load-workers 8 --source-workers 8

  python3 repair_loan_created_time_from_market.py --env ./ng_migration.env --apply \\
    --plan-file /tmp/fix_loan_created_time_plan.jsonl --workers 8 --batch-size 200

  # 已有 cache，只重建 plan
  python3 repair_loan_created_time_from_market.py --env ./ng_migration.env --build-plan \\
    --from-cache --cache-file /tmp/loan_marker_snapshot.jsonl \\
    --plan-file /tmp/fix_loan_created_time_plan.jsonl
"""
import argparse
import json
import multiprocessing
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pymysql
from pymysql.cursors import DictCursor

from repair_loan_no_from_audit import exec_with_retry

HERE = Path(__file__).resolve().parent
APP_NO_RE = re.compile(r"^ng(\d+)-(.+)$", re.I)
DEFAULT_OLD_CREATED_MS = 1785340800000


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
    timeout = 120 if for_apply else 3600
    return pymysql.connect(
        host=cfg["TARGET_HOST"],
        port=int(cfg.get("TARGET_PORT", "3306")),
        user=cfg["TARGET_USER"],
        password=cfg["TARGET_PASSWORD"],
        database=cfg.get("TARGET_DB", "ng"),
        charset="utf8mb4",
        cursorclass=DictCursor,
        connect_timeout=int(cfg.get("mysql_connect_timeout") or 60),
        read_timeout=timeout,
        write_timeout=timeout,
        autocommit=False,
    )


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
        autocommit=True,
    )


def disburse_seconds_to_ms(val: Any) -> int:
    if val in (None, ""):
        return 0
    try:
        n = int(val)
    except (TypeError, ValueError):
        return 0
    if n <= 0:
        return 0
    return n * 1000 if n < 10**12 else n


def parse_target_application_no(app_no: str) -> Optional[Tuple[int, str]]:
    m = APP_NO_RE.match(str(app_no or "").strip())
    if not m:
        return None
    return int(m.group(1)), m.group(2).strip()


def write_jsonl(path: Path, rows: List[dict]) -> None:
    with path.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")


def read_jsonl(path: Path) -> List[dict]:
    out: List[dict] = []
    with path.open(encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def _ping(conn) -> None:
    try:
        conn.ping(reconnect=True)
    except Exception:
        pass


def _reconnect_target(cfg: Dict[str, str], conn):
    try:
        conn.close()
    except Exception:
        pass
    return connect_target(cfg, for_apply=True)


# ---------------------------------------------------------------------------
# Phase 1: parallel load loan PK rows from target
# ---------------------------------------------------------------------------

def _load_loan_shard(spec: dict) -> Tuple[int, List[dict], Dict[str, int]]:
    worker_id = int(spec["worker_id"])
    workers = int(spec["workers"])
    old_ms = int(spec["old_created_ms"])
    env_path = spec["env"]
    page_size = max(1000, int(spec.get("page_size") or 20000))
    cfg = load_env(Path(env_path))
    stats = {"scanned": 0}
    rows: List[dict] = []
    label = "[%s/%s]" % (worker_id, workers)
    print("%s load start shard ..." % label, flush=True)
    t0 = time.time()
    tgt = connect_target(cfg)
    try:
        last_loan_no = ""
        while True:
            with tgt.cursor() as cur:
                cur.execute(
                    """
                    SELECT loan_no, application_no, period, roll_sequence, created_time
                    FROM loan
                    WHERE created_time = %s
                      AND MOD(CRC32(application_no), %s) = %s
                      AND loan_no > %s
                    ORDER BY loan_no ASC
                    LIMIT %s
                    """,
                    (old_ms, workers, worker_id - 1, last_loan_no, page_size),
                )
                batch = cur.fetchall()
            if not batch:
                break
            for row in batch:
                rows.append(
                    {
                        "loan_no": str(row["loan_no"]),
                        "application_no": str(row["application_no"]),
                        "period": int(row.get("period") or 1),
                        "roll_sequence": int(row.get("roll_sequence") or 0),
                        "created_time": int(row.get("created_time") or 0),
                    }
                )
                last_loan_no = str(row["loan_no"])
            stats["scanned"] = len(rows)
            if stats["scanned"] % 100000 < page_size:
                print(
                    "%s load progress rows=%s elapsed=%.1fs"
                    % (label, stats["scanned"], time.time() - t0),
                    flush=True,
                )
            if len(batch) < page_size:
                break
    finally:
        tgt.close()
    print(
        "%s load done rows=%s elapsed=%.1fs"
        % (label, len(rows), time.time() - t0),
        flush=True,
    )
    return worker_id, rows, stats


def parallel_load_loans(
    cfg: Dict[str, str],
    env_path: str,
    old_created_ms: int,
    load_workers: int,
    page_size: int,
) -> List[dict]:
    workers = max(1, min(int(load_workers), 32))
    specs = [
        {
            "worker_id": i + 1,
            "workers": workers,
            "old_created_ms": old_created_ms,
            "env": env_path,
            "page_size": page_size,
        }
        for i in range(workers)
    ]
    print(
        "parallel load workers=%s old_created_ms=%s ..."
        % (workers, old_created_ms),
        flush=True,
    )
    t0 = time.time()
    if workers == 1:
        _, rows, _ = _load_loan_shard(specs[0])
        merged = rows
    else:
        ctx = multiprocessing.get_context("spawn")
        with ctx.Pool(processes=workers) as pool:
            parts = pool.map(_load_loan_shard, specs)
        parts.sort(key=lambda x: x[0])
        merged = []
        for _, chunk, _ in parts:
            merged.extend(chunk)
    print(
        "parallel load done total_rows=%s elapsed=%.1fs"
        % (len(merged), time.time() - t0),
        flush=True,
    )
    return merged


# ---------------------------------------------------------------------------
# Phase 2: source disburseTime lookup (parallel threads)
# ---------------------------------------------------------------------------

def _fetch_market_chunk(cfg: Dict[str, str], keys: List[Tuple[int, str]]) -> Dict[str, int]:
    if not keys:
        return {}
    src = connect_source(cfg)
    out: Dict[str, int] = {}
    try:
        m = "ng_loan_market"
        holders = ",".join(["(%s,%s)"] * len(keys))
        params: List[Any] = []
        for aid, mno in keys:
            params.extend([int(aid), str(mno)])
        sql = f"""
            SELECT appId AS app_id, applicationNo AS market_no, disburseTime AS disburse_time
            FROM {m}.application
            WHERE (appId, applicationNo) IN ({holders})
        """
        with src.cursor() as cur:
            cur.execute(sql, params)
            for row in cur.fetchall():
                aid = int(row["app_id"])
                mno = str(row["market_no"]).strip()
                app_no = f"ng{aid:04d}-{mno}"
                ms = disburse_seconds_to_ms(row.get("disburse_time"))
                if ms > 0:
                    out[app_no] = ms
    finally:
        src.close()
    return out


def parallel_fetch_disburse_map(
    cfg: Dict[str, str],
    app_keys: List[Tuple[int, str]],
    source_workers: int,
    chunk_size: int,
) -> Dict[str, int]:
    uniq: Dict[Tuple[int, str], str] = {}
    for aid, mno in app_keys:
        uniq[(int(aid), str(mno))] = f"ng{int(aid):04d}-{mno}"
    keys = sorted(uniq.keys())
    if not keys:
        return {}
    workers = max(1, min(int(source_workers), 32))
    chunks: List[List[Tuple[int, str]]] = []
    step = max(50, int(chunk_size))
    for i in range(0, len(keys), step):
        chunks.append(keys[i : i + step])
    print(
        "source lookup keys=%s chunks=%s workers=%s ..."
        % (len(keys), len(chunks), workers),
        flush=True,
    )
    t0 = time.time()
    out: Dict[str, int] = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = [pool.submit(_fetch_market_chunk, cfg, c) for c in chunks]
        for fut in as_completed(futs):
            part = fut.result()
            out.update(part)
    print(
        "source lookup done hit=%s elapsed=%.1fs"
        % (len(out), time.time() - t0),
        flush=True,
    )
    return out


def fetch_target_disbursed_map(
    cfg: Dict[str, str], app_nos: List[str], chunk: int = 2000
) -> Dict[str, int]:
    if not app_nos:
        return {}
    tgt = connect_target(cfg)
    out: Dict[str, int] = {}
    try:
        for i in range(0, len(app_nos), chunk):
            part = app_nos[i : i + chunk]
            ph = ",".join(["%s"] * len(part))
            with tgt.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT application_no, disbursed_time
                    FROM application
                    WHERE application_no IN ({ph})
                      AND disbursed_time > 0
                    """,
                    part,
                )
                for row in cur.fetchall():
                    out[str(row["application_no"]).strip()] = int(row["disbursed_time"])
    finally:
        tgt.close()
    return out


# ---------------------------------------------------------------------------
# Phase 3: build row-level plan
# ---------------------------------------------------------------------------

def build_plan_rows(
    loans: List[dict],
    ms_by_app: Dict[str, int],
    old_created_ms: int,
) -> Tuple[List[dict], Dict[str, int]]:
    plan: List[dict] = []
    stats = {
        "total_loans": len(loans),
        "skip_bad_application_no": 0,
        "skip_no_disburse_time": 0,
        "skip_unchanged": 0,
        "plan_rows": 0,
    }
    for row in loans:
        app_no = str(row["application_no"])
        if not parse_target_application_no(app_no):
            stats["skip_bad_application_no"] += 1
            continue
        new_ms = int(ms_by_app.get(app_no) or 0)
        if new_ms <= 0:
            stats["skip_no_disburse_time"] += 1
            continue
        if new_ms == old_created_ms:
            stats["skip_unchanged"] += 1
            continue
        plan.append(
            {
                "loan_no": row["loan_no"],
                "application_no": app_no,
                "period": int(row["period"]),
                "roll_sequence": int(row["roll_sequence"]),
                "old_created_time": old_created_ms,
                "new_created_time": new_ms,
            }
        )
    stats["plan_rows"] = len(plan)
    return plan, stats


def build_plan_pipeline(
    cfg: Dict[str, str],
    env_path: str,
    old_created_ms: int,
    loans: List[dict],
    source_workers: int,
    source_chunk: int,
    fallback_target: bool,
) -> Tuple[List[dict], Dict[str, int]]:
    app_keys: List[Tuple[int, str]] = []
    seen: Set[str] = set()
    for row in loans:
        app_no = str(row["application_no"])
        if app_no in seen:
            continue
        parsed = parse_target_application_no(app_no)
        if not parsed:
            continue
        seen.add(app_no)
        app_keys.append(parsed)

    ms_by_app = parallel_fetch_disburse_map(
        cfg, app_keys, source_workers, source_chunk
    )
    stats_extra: Dict[str, int] = {"market_hit": len(ms_by_app)}

    if fallback_target:
        missing = sorted(seen - set(ms_by_app.keys()))
        print("fallback target disbursed_time missing=%s ..." % len(missing), flush=True)
        fb = fetch_target_disbursed_map(cfg, missing)
        ms_by_app.update(fb)
        stats_extra["fallback_hit"] = len(fb)

    plan, stats = build_plan_rows(loans, ms_by_app, old_created_ms)
    stats.update(stats_extra)
    return plan, stats


# ---------------------------------------------------------------------------
# Phase 4: parallel apply by PK
# ---------------------------------------------------------------------------

def split_chunks(rows: List[dict], workers: int) -> List[List[dict]]:
    n = max(1, int(workers))
    if n == 1:
        return [rows]
    chunks: List[List[dict]] = [[] for _ in range(n)]
    for i, row in enumerate(rows):
        chunks[i % n].append(row)
    return [c for c in chunks if c]


def apply_batch_pk(tgt, rows: List[dict]) -> Tuple[int, int, Dict[str, int]]:
    if not rows:
        return 0, 0, {}
    parts: List[str] = []
    params: List[Any] = []
    for r in rows:
        parts.append(
            "SELECT %s AS app_no, %s AS period, %s AS roll, "
            "%s AS new_ms, %s AS old_ms"
        )
        params.extend(
            [
                str(r["application_no"]),
                int(r["period"]),
                int(r["roll_sequence"]),
                int(r["new_created_time"]),
                int(r["old_created_time"]),
            ]
        )
    sql = (
        """
        UPDATE loan l
        INNER JOIN (
        """
        + " UNION ALL ".join(parts)
        + """
        ) x ON l.application_no = x.app_no
           AND l.period = x.period
           AND l.roll_sequence = x.roll
           AND l.created_time = x.old_ms
        SET l.created_time = x.new_ms
        """
    )

    def _run():
        _ping(tgt)
        with tgt.cursor() as cur:
            cur.execute(sql, tuple(params))
            return int(cur.rowcount or 0)

    try:
        ok = int(exec_with_retry(tgt, _run, "apply batch size=%s" % len(rows)) or 0)
        skip = max(0, len(rows) - ok)
        stats: Dict[str, int] = {}
        if skip:
            stats["skip_rowcount"] = skip
        return ok, skip, stats
    except pymysql.err.IntegrityError:
        try:
            tgt.rollback()
        except Exception:
            pass
        return apply_rows_fallback(tgt, rows)


def apply_one_pk(tgt, row: dict) -> str:
    def _run():
        _ping(tgt)
        with tgt.cursor() as cur:
            cur.execute(
                """
                UPDATE loan
                SET created_time=%s
                WHERE application_no=%s AND period=%s AND roll_sequence=%s
                  AND created_time=%s
                """,
                (
                    int(row["new_created_time"]),
                    str(row["application_no"]),
                    int(row["period"]),
                    int(row["roll_sequence"]),
                    int(row["old_created_time"]),
                ),
            )
            return "ok" if int(cur.rowcount or 0) > 0 else "skip_missing"

    return exec_with_retry(
        tgt,
        _run,
        "update %s p=%s r=%s"
        % (row["application_no"], row["period"], row["roll_sequence"]),
    )


def apply_rows_fallback(tgt, rows: List[dict]) -> Tuple[int, int, Dict[str, int]]:
    ok = skip = 0
    stats: Dict[str, int] = {"fallback_row": len(rows)}
    for r in rows:
        result = apply_one_pk(tgt, r)
        if result == "ok":
            ok += 1
            tgt.commit()
        else:
            skip += 1
            stats[result] = stats.get(result, 0) + 1
            try:
                tgt.rollback()
            except Exception:
                pass
    return ok, skip, stats


def _apply_worker(spec: dict) -> Tuple[int, int, Dict[str, int]]:
    label = "[%s/%s] " % (spec["worker_id"], spec["workers"])
    chunk = spec.get("plan_chunk") or []
    if not chunk:
        return 0, 0, {}
    cfg = load_env(Path(spec["env"]))
    batch_size = max(1, int(spec.get("batch_size") or 100))
    print("%sstart rows=%s batch_size=%s" % (label, len(chunk), batch_size), flush=True)
    tgt = connect_target(cfg, for_apply=True)
    ok = skip = 0
    stats: Dict[str, int] = {}
    try:
        total_batches = (len(chunk) + batch_size - 1) // batch_size
        for bi in range(0, len(chunk), batch_size):
            part = chunk[bi : bi + batch_size]
            bno = bi // batch_size + 1
            try:
                bok, bskip, bstats = apply_batch_pk(tgt, part)
                tgt.commit()
            except Exception as exc:
                print("%sbatch %s err=%s reconnect" % (label, bno, exc), flush=True)
                try:
                    tgt.rollback()
                except Exception:
                    pass
                tgt = _reconnect_target(cfg, tgt)
                bok, bskip, bstats = apply_batch_pk(tgt, part)
                tgt.commit()
            ok += bok
            skip += bskip
            for k, v in bstats.items():
                stats[k] = stats.get(k, 0) + v
            print(
                "%sbatch %s/%s updated=%s skip=%s total_ok=%s"
                % (label, bno, total_batches, bok, bskip, ok),
                flush=True,
            )
    finally:
        try:
            tgt.close()
        except Exception:
            pass
    print("%sdone ok=%s skip=%s stats=%s" % (label, ok, skip, stats), flush=True)
    return ok, skip, stats


def run_parallel_apply(
    plan: List[dict],
    workers: int,
    env_path: str,
    batch_size: int,
) -> Tuple[int, int, Dict[str, int]]:
    workers = min(max(1, int(workers)), 16)
    chunks = split_chunks(plan, workers)
    specs = []
    for i, chunk in enumerate(chunks):
        specs.append(
            {
                "worker_id": i + 1,
                "workers": len(chunks),
                "env": env_path,
                "batch_size": batch_size,
                "plan_chunk": chunk,
            }
        )
    print(
        "apply workers=%s plan_rows=%s batch_size=%s"
        % (len(specs), len(plan), batch_size),
        flush=True,
    )
    if len(specs) == 1:
        return _apply_worker(specs[0])
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(processes=len(specs)) as pool:
        results = pool.map(_apply_worker, specs)
    ok = sum(r[0] for r in results)
    skip = sum(r[1] for r in results)
    stats: Dict[str, int] = {}
    for _, _, s in results:
        for k, v in s.items():
            stats[k] = stats.get(k, 0) + v
    print("parallel apply done ok=%s skip=%s stats=%s" % (ok, skip, stats), flush=True)
    return ok, skip, stats


def default_workers() -> int:
    return min(8, os.cpu_count() or 4)


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Fix placeholder loan.created_time from market.disburseTime (parallel)"
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument("--apply-only", action="store_true")
    p.add_argument("--build-plan", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--from-cache", action="store_true", help="跳过拉取，从 cache-file 读 loan")
    p.add_argument(
        "--cache-file",
        default="/tmp/loan_marker_snapshot.jsonl",
        help="loan 主键快照（jsonl）",
    )
    p.add_argument(
        "--plan-file",
        default="/tmp/fix_loan_created_time_plan.jsonl",
    )
    p.add_argument("--old-created-ms", type=int, default=DEFAULT_OLD_CREATED_MS)
    p.add_argument("--page-size", type=int, default=20000, help="每 shard 分页大小")
    p.add_argument("--load-workers", type=int, default=default_workers())
    p.add_argument("--source-workers", type=int, default=default_workers())
    p.add_argument("--source-chunk", type=int, default=500)
    p.add_argument("--workers", type=int, default=default_workers(), help="apply 并行进程")
    p.add_argument("--batch-size", type=int, default=200)
    p.add_argument("--work-limit", type=int, default=0)
    p.add_argument("--fallback-target-disbursed", action="store_true")
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    if args.apply_only:
        args.apply = True
    dry_run = not args.apply

    env_path = str(Path(args.env).resolve())
    cfg = load_env(Path(args.env))
    cache_path = Path(args.cache_file)
    plan_path = Path(args.plan_file)
    old_ms = int(args.old_created_ms)

    if args.build_plan or args.dry_run:
        if args.from_cache:
            if not cache_path.is_file():
                p.error("cache not found: %s" % cache_path)
            print("loading cache %s ..." % cache_path, flush=True)
            t0 = time.time()
            loans = read_jsonl(cache_path)
            print(
                "loaded cache rows=%s elapsed=%.1fs" % (len(loans), time.time() - t0),
                flush=True,
            )
        else:
            loans = parallel_load_loans(
                cfg,
                env_path,
                old_ms,
                args.load_workers,
                max(1000, args.page_size),
            )
            cache_path.write_text("", encoding="utf-8")
            write_jsonl(cache_path, loans)
            print("wrote cache_file=%s rows=%s" % (cache_path, len(loans)), flush=True)

        if args.work_limit > 0:
            loans = loans[: args.work_limit]

        plan, stats = build_plan_pipeline(
            cfg,
            env_path,
            old_ms,
            loans,
            args.source_workers,
            max(50, args.source_chunk),
            bool(args.fallback_target_disbursed),
        )
        write_jsonl(plan_path, plan)
        print(
            "wrote plan_file=%s rows=%s stats=%s"
            % (plan_path, len(plan), stats),
            flush=True,
        )
        for row in plan[:10]:
            print(
                "  %s p=%s r=%s -> created_time=%s"
                % (
                    row["application_no"],
                    row["period"],
                    row["roll_sequence"],
                    row["new_created_time"],
                ),
                flush=True,
            )
        if len(plan) > 10:
            print("  ... and %s more" % (len(plan) - 10), flush=True)
        if dry_run:
            print("dry-run only (use --apply or --apply-only)", flush=True)
            return 0

    if args.apply:
        if not plan_path.is_file():
            p.error("plan not found: %s (run --build-plan first)" % plan_path)
        print("loading plan %s ..." % plan_path, flush=True)
        t0 = time.time()
        plan = read_jsonl(plan_path)
        print(
            "loaded plan rows=%s elapsed=%.1fs" % (len(plan), time.time() - t0),
            flush=True,
        )
        if not plan:
            print("plan empty", flush=True)
            return 0
        ok, skip, stats = run_parallel_apply(
            plan, args.workers, env_path, max(1, args.batch_size)
        )
        print("done ok=%s skip=%s stats=%s" % (ok, skip, stats), flush=True)
        return 0 if ok or skip else 1

    p.error("specify --build-plan, --dry-run, --apply, or --apply-only")


if __name__ == "__main__":
    raise SystemExit(main())
