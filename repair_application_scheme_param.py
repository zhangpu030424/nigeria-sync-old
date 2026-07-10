#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""将 application.product_scheme_param 批量改为 {}。

按 mobile 更新（同一 mobile 下所有申请一并改）。
从本地快照去重 mobile 后多进程执行，并打印跳过原因。

Usage:
  python3 repair_application_scheme_param.py --env ./ng_migration.env --apply \\
    --from-cache --cache-file /tmp/application_sn_snapshot_after.json \\
    --workers 20 --batch-size 100

  python3 repair_application_scheme_param.py --dry-run --from-cache \\
    --cache-file /tmp/application_sn_snapshot_after.json
"""
import argparse
import json
import multiprocessing
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pymysql
from pymysql.constants import CLIENT
from pymysql.cursors import DictCursor

HERE = Path(__file__).resolve().parent
TARGET_VALUE = "{}"


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
        connect_timeout=60,
        read_timeout=timeout,
        write_timeout=timeout,
        autocommit=False,
        client_flag=CLIENT.FOUND_ROWS,
    )


def load_snapshot_cache(path: Path) -> List[dict]:
    t0 = time.time()
    print("loading cache %s ..." % path, flush=True)
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".jsonl" or (text and text[:1] not in "[{"):
        rows = []
        for line in text.splitlines():
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    else:
        rows = json.loads(text)
    print(
        "loaded cache rows=%s elapsed=%.1fs" % (len(rows), time.time() - t0),
        flush=True,
    )
    return rows


def extract_mobiles(rows: List[dict], mobile_prefix: str = "") -> List[str]:
    """快照去重 mobile。"""
    seen = set()
    out: List[str] = []
    prefix = (mobile_prefix or "").strip()
    for r in rows:
        mobile = str(r.get("mobile") or "").strip()
        if not mobile or mobile in seen:
            continue
        if prefix and not mobile.startswith(prefix):
            continue
        seen.add(mobile)
        out.append(mobile)
    return out


def split_chunks(rows: List[str], workers: int) -> List[List[str]]:
    n = max(1, int(workers))
    if n == 1:
        return [rows]
    chunks: List[List[str]] = [[] for _ in range(n)]
    for i, row in enumerate(rows):
        chunks[i % n].append(row)
    return [c for c in chunks if c]


def _ping(conn) -> None:
    try:
        conn.ping(reconnect=True)
    except Exception:
        pass


def _reconnect(cfg: Dict[str, str], conn):
    try:
        conn.close()
    except Exception:
        pass
    return connect_target(cfg, for_apply=True)


def diagnose_mobile(tgt, mobile: str) -> Dict[str, int]:
    """查该 mobile 在库里的状态，用于解释 skip。"""
    with tgt.cursor() as cur:
        cur.execute(
            """
            SELECT
              COUNT(*) AS total,
              SUM(CASE
                    WHEN product_scheme_param IS NULL THEN 1
                    WHEN CAST(product_scheme_param AS CHAR) = '{}' THEN 1
                    WHEN CAST(product_scheme_param AS CHAR) = '' THEN 1
                    ELSE 0
                  END) AS already_ok,
              SUM(CASE
                    WHEN product_scheme_param IS NOT NULL
                     AND CAST(product_scheme_param AS CHAR) <> '{}'
                     AND CAST(product_scheme_param AS CHAR) <> ''
                    THEN 1 ELSE 0
                  END) AS need_fix,
              LEFT(MAX(CAST(product_scheme_param AS CHAR)), 80) AS sample_psp
            FROM application
            WHERE mobile = %s
            """,
            (mobile,),
        )
        row = cur.fetchone() or {}
    return {
        "total": int(row.get("total") or 0),
        "already_ok": int(row.get("already_ok") or 0),
        "need_fix": int(row.get("need_fix") or 0),
        "sample_psp": row.get("sample_psp"),
    }


def update_one_mobile(tgt, mobile: str) -> Tuple[str, int, Dict]:
    """
    按 mobile UPDATE。
    返回 (reason, affected, diag)
      reason: updated | already_ok | missing | error
    """
    diag = diagnose_mobile(tgt, mobile)
    if diag["total"] == 0:
        return "missing", 0, diag
    if diag["need_fix"] == 0:
        return "already_ok", 0, diag

    sql = """
        UPDATE application
        SET product_scheme_param = %s
        WHERE mobile = %s
          AND (
            product_scheme_param IS NULL
            OR CAST(product_scheme_param AS CHAR) <> '{}'
            OR CAST(product_scheme_param AS CHAR) = ''
          )
    """
    for attempt in range(6):
        try:
            _ping(tgt)
            with tgt.cursor() as cur:
                cur.execute(sql, (TARGET_VALUE, mobile))
                n = int(cur.rowcount or 0)
            if n > 0:
                return "updated", n, diag
            # rowcount=0 但诊断说有 need_fix：再查一次
            diag2 = diagnose_mobile(tgt, mobile)
            if diag2["need_fix"] == 0:
                return "already_ok", 0, diag2
            return "rowcount0_but_need_fix", 0, diag2
        except pymysql.err.OperationalError as e:
            errno = e.args[0] if e.args else 0
            try:
                tgt.rollback()
            except Exception:
                pass
            if errno in (1213, 1205, 2013, 2006) and attempt < 5:
                time.sleep(0.3 * (2 ** attempt))
                continue
            return "error:%s" % errno, 0, diag
    return "error:retry_exhausted", 0, diag


def apply_mobile_batch(
    tgt, mobiles: List[str], label: str, log_skips: bool
) -> Tuple[int, int, Dict[str, int]]:
    """返回 (updated_rows, skip_mobiles, reason_counts)。"""
    updated_rows = 0
    skip_mobiles = 0
    reasons: Dict[str, int] = defaultdict(int)
    skip_samples: List[str] = []

    for mobile in mobiles:
        reason, n, diag = update_one_mobile(tgt, mobile)
        reasons[reason] += 1
        if reason == "updated":
            updated_rows += n
        else:
            skip_mobiles += 1
            if log_skips and len(skip_samples) < 5:
                skip_samples.append(
                    "mobile=%s reason=%s total=%s already_ok=%s need_fix=%s psp=%r"
                    % (
                        mobile,
                        reason,
                        diag.get("total"),
                        diag.get("already_ok"),
                        diag.get("need_fix"),
                        diag.get("sample_psp"),
                    )
                )

    if log_skips and skip_samples:
        for line in skip_samples:
            print("%sskip_detail %s" % (label, line), flush=True)
    return updated_rows, skip_mobiles, dict(reasons)


def worker_run(spec: dict) -> Tuple[int, int, Dict[str, int]]:
    label = "[%s/%s] " % (spec["worker_id"], spec["workers"])
    chunk: List[str] = spec.get("plan_chunk") or []
    if not chunk:
        return 0, 0, {}
    cfg = load_env(Path(spec["env"]))
    batch_size = max(1, int(spec.get("batch_size") or 100))
    print(
        "%sstart mobiles=%s batch_size=%s"
        % (label, len(chunk), batch_size),
        flush=True,
    )
    tgt = connect_target(cfg, for_apply=True)
    ok = skip = 0
    reasons: Dict[str, int] = defaultdict(int)
    try:
        # 启动抽查
        for m in chunk[:3]:
            d = diagnose_mobile(tgt, m)
            print("%sprobe mobile=%s diag=%s" % (label, m, d), flush=True)

        total_batches = (len(chunk) + batch_size - 1) // batch_size
        for bi in range(0, len(chunk), batch_size):
            part = chunk[bi : bi + batch_size]
            bno = bi // batch_size + 1
            log_skips = bno <= 3 or bno % 50 == 0
            try:
                bok, bskip, breasons = apply_mobile_batch(
                    tgt, part, label, log_skips
                )
                tgt.commit()
            except Exception as exc:
                print("%sbatch %s err=%s reconnect" % (label, bno, exc), flush=True)
                try:
                    tgt.rollback()
                except Exception:
                    pass
                tgt = _reconnect(cfg, tgt)
                bok, bskip, breasons = apply_mobile_batch(
                    tgt, part, label, True
                )
                tgt.commit()
            ok += bok
            skip += bskip
            for k, v in breasons.items():
                reasons[k] += v
            if bno == 1 or bno % 20 == 0 or bno == total_batches:
                print(
                    "%sbatch %s/%s updated_rows=%s skip_mobiles=%s "
                    "reasons=%s total_updated_rows=%s"
                    % (label, bno, total_batches, bok, bskip, dict(breasons), ok),
                    flush=True,
                )
    finally:
        try:
            tgt.close()
        except Exception:
            pass
    print(
        "%sdone updated_rows=%s skip_mobiles=%s reasons=%s"
        % (label, ok, skip, dict(reasons)),
        flush=True,
    )
    return ok, skip, dict(reasons)


def run_parallel(
    mobiles: List[str], workers: int, env_path: str, batch_size: int
) -> Tuple[int, int, Dict[str, int]]:
    workers = min(max(1, int(workers)), 32)
    chunks = split_chunks(mobiles, workers)
    specs = [
        {
            "worker_id": i + 1,
            "workers": len(chunks),
            "env": env_path,
            "batch_size": batch_size,
            "plan_chunk": chunk,
        }
        for i, chunk in enumerate(chunks)
    ]
    print(
        "apply workers=%s unique_mobiles=%s batch_size=%s"
        % (len(specs), len(mobiles), batch_size),
        flush=True,
    )
    if len(specs) == 1:
        return worker_run(specs[0])
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(processes=len(specs)) as pool:
        results = pool.map(worker_run, specs)
    ok = sum(r[0] for r in results)
    skip = sum(r[1] for r in results)
    reasons: Dict[str, int] = defaultdict(int)
    for _, _, rs in results:
        for k, v in rs.items():
            reasons[k] += v
    print(
        "parallel done updated_rows=%s skip_mobiles=%s reasons=%s"
        % (ok, skip, dict(reasons)),
        flush=True,
    )
    return ok, skip, dict(reasons)


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Set application.product_scheme_param={} by mobile"
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--from-cache", action="store_true")
    p.add_argument(
        "--cache-file",
        default="/tmp/application_sn_snapshot_after.json",
    )
    p.add_argument("--workers", type=int, default=20)
    p.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="每批处理多少个 mobile（每个 mobile 一次 UPDATE）",
    )
    p.add_argument("--mobile-prefix", default="")
    p.add_argument("--work-limit", type=int, default=0)
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    if not args.from_cache:
        p.error("请使用 --from-cache（按 mobile 更新）")
    dry_run = not args.apply
    env_path = str(Path(args.env).resolve())

    cache_path = Path(args.cache_file)
    if not cache_path.is_file():
        p.error("cache not found: %s" % cache_path)

    raw = load_snapshot_cache(cache_path)
    mobiles = extract_mobiles(raw, args.mobile_prefix)
    if args.work_limit > 0:
        mobiles = mobiles[: args.work_limit]
    print(
        "unique_mobiles=%s (from cache_rows=%s) prefix=%r sample=%s"
        % (len(mobiles), len(raw), args.mobile_prefix, mobiles[:5]),
        flush=True,
    )
    if dry_run:
        print("dry-run only (use --apply)", flush=True)
        return 0
    if not mobiles:
        print("no mobiles", flush=True)
        return 0

    ok, skip, reasons = run_parallel(
        mobiles, args.workers, env_path, max(1, args.batch_size)
    )
    print(
        "done updated_rows=%s skip_mobiles=%s reasons=%s"
        % (ok, skip, reasons),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
