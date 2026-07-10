#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""将 application.product_scheme_param 批量改为 {}。

优先：读本地全量快照主键（mobile, group_user_id, sn），多进程按主键 UPDATE。
快照格式同 repair_application_sn_from_suffix.py 的 cache（JSON 数组）。

Usage:
  # 用本地快照 + 20 进程 apply
  python3 repair_application_scheme_param.py --env ./ng_migration.env --apply \\
    --from-cache --cache-file /tmp/application_sn_snapshot_after.json \\
    --workers 20 --batch-size 200

  # 只看会处理多少行
  python3 repair_application_scheme_param.py --dry-run --from-cache \\
    --cache-file /tmp/application_sn_snapshot_after.json

  # 无 cache 时：从库按主键游标扫（单进程）
  python3 repair_application_scheme_param.py --env ./ng_migration.env --apply --batch-size 500
"""
import argparse
import json
import multiprocessing
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pymysql
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
    )


def load_snapshot_cache(path: Path) -> List[dict]:
    t0 = time.time()
    print("loading cache %s ..." % path, flush=True)
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".jsonl" or text[:1] not in "[{":
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


def extract_pk_rows(
    rows: List[dict], mobile_prefix: str = ""
) -> List[dict]:
    out: List[dict] = []
    prefix = (mobile_prefix or "").strip()
    for r in rows:
        mobile = str(r.get("mobile") or "").strip()
        if not mobile:
            continue
        if prefix and not mobile.startswith(prefix):
            continue
        try:
            gid = int(r["group_user_id"])
        except (KeyError, TypeError, ValueError):
            continue
        sn = str(r.get("sn") or "").strip()
        if not sn:
            continue
        out.append({"mobile": mobile, "group_user_id": gid, "sn": sn})
    return out


def split_chunks(rows: List[dict], workers: int) -> List[List[dict]]:
    n = max(1, int(workers))
    if n == 1:
        return [rows]
    chunks: List[List[dict]] = [[] for _ in range(n)]
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


def apply_batch(tgt, rows: List[dict]) -> Tuple[int, int]:
    """按主键批量 UPDATE；返回 (ok_estimate, skip_estimate)。"""
    if not rows:
        return 0, 0
    parts: List[str] = []
    params: List[Any] = []
    for r in rows:
        parts.append("SELECT %s AS mobile, %s AS gid, %s AS sn")
        params.extend([r["mobile"], int(r["group_user_id"]), r["sn"]])
    sql = (
        """
        UPDATE application a
        INNER JOIN (
        """
        + " UNION ALL ".join(parts)
        + """
        ) x ON a.mobile = x.mobile
           AND a.group_user_id = x.gid
           AND a.sn = x.sn
        SET a.product_scheme_param = %s
        """
    )
    params.append(TARGET_VALUE)

    def _run():
        _ping(tgt)
        with tgt.cursor() as cur:
            cur.execute(sql, tuple(params))
            return int(cur.rowcount or 0)

    for attempt in range(6):
        try:
            ok = _run()
            return ok, max(0, len(rows) - ok)
        except pymysql.err.OperationalError as e:
            errno = e.args[0] if e.args else 0
            try:
                tgt.rollback()
            except Exception:
                pass
            if errno in (1213, 1205, 2013, 2006) and attempt < 5:
                time.sleep(0.3 * (2 ** attempt))
                continue
            raise
    return 0, len(rows)


def worker_run(spec: dict) -> Tuple[int, int]:
    label = "[%s/%s] " % (spec["worker_id"], spec["workers"])
    chunk = spec.get("plan_chunk") or []
    if not chunk:
        return 0, 0
    cfg = load_env(Path(spec["env"]))
    batch_size = max(1, int(spec.get("batch_size") or 200))
    print("%sstart rows=%s batch_size=%s" % (label, len(chunk), batch_size), flush=True)
    tgt = connect_target(cfg, for_apply=True)
    ok = skip = 0
    try:
        total_batches = (len(chunk) + batch_size - 1) // batch_size
        for bi in range(0, len(chunk), batch_size):
            part = chunk[bi : bi + batch_size]
            bno = bi // batch_size + 1
            try:
                bok, bskip = apply_batch(tgt, part)
                tgt.commit()
            except Exception as exc:
                print("%sbatch %s err=%s reconnect" % (label, bno, exc), flush=True)
                try:
                    tgt.rollback()
                except Exception:
                    pass
                tgt = _reconnect(cfg, tgt)
                bok, bskip = apply_batch(tgt, part)
                tgt.commit()
            ok += bok
            skip += bskip
            if bno == 1 or bno % 20 == 0 or bno == total_batches:
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
    print("%sdone ok=%s skip=%s" % (label, ok, skip), flush=True)
    return ok, skip


def run_parallel(
    rows: List[dict], workers: int, env_path: str, batch_size: int
) -> Tuple[int, int]:
    workers = min(max(1, int(workers)), 32)
    chunks = split_chunks(rows, workers)
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
        "apply workers=%s rows=%s batch_size=%s"
        % (len(specs), len(rows), batch_size),
        flush=True,
    )
    if len(specs) == 1:
        return worker_run(specs[0])
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(processes=len(specs)) as pool:
        results = pool.map(worker_run, specs)
    ok = sum(r[0] for r in results)
    skip = sum(r[1] for r in results)
    print("parallel done ok=%s skip=%s" % (ok, skip), flush=True)
    return ok, skip


def fetch_batch_from_db(
    conn,
    last: Tuple[str, int, str],
    batch_size: int,
    only_non_empty: bool,
    mobile_prefix: str,
) -> List[dict]:
    where = ["(mobile, group_user_id, sn) > (%s, %s, %s)"]
    params: List = [last[0], last[1], last[2]]
    if mobile_prefix:
        where.append("mobile LIKE %s")
        params.append(mobile_prefix + "%")
    if only_non_empty:
        where.append(
            "(product_scheme_param IS NULL OR product_scheme_param <> %s)"
        )
        params.append(TARGET_VALUE)
    params.append(batch_size)
    sql = (
        """
        SELECT mobile, group_user_id, sn
        FROM application
        WHERE """
        + " AND ".join(where)
        + """
        ORDER BY mobile, group_user_id, sn
        LIMIT %s
        """
    )
    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        return list(cur.fetchall())


def run_from_db(
    cfg: Dict[str, str],
    batch_size: int,
    only_non_empty: bool,
    mobile_prefix: str,
    work_limit: int,
    dry_run: bool,
) -> Tuple[int, int]:
    conn = connect_target(cfg, for_apply=not dry_run)
    last = ("", 0, "")
    total_seen = total_updated = 0
    t0 = time.time()
    try:
        while True:
            rows = fetch_batch_from_db(
                conn,
                last,
                max(1, batch_size),
                only_non_empty,
                mobile_prefix.strip(),
            )
            if not rows:
                break
            total_seen += len(rows)
            last = (
                str(rows[-1]["mobile"]),
                int(rows[-1]["group_user_id"]),
                str(rows[-1]["sn"]),
            )
            if dry_run:
                if total_seen <= batch_size or total_seen % (batch_size * 20) == 0:
                    print("dry-run seen=%s" % total_seen, flush=True)
            else:
                bok, _ = apply_batch(conn, rows)
                conn.commit()
                total_updated += bok
                if total_updated % (batch_size * 10) < batch_size:
                    print(
                        "progress updated=%s seen=%s elapsed=%.1fs"
                        % (total_updated, total_seen, time.time() - t0),
                        flush=True,
                    )
            if work_limit > 0 and total_seen >= work_limit:
                break
    finally:
        conn.close()
    return total_updated, total_seen


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Set application.product_scheme_param to {} via cache PK or DB scan"
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--from-cache",
        action="store_true",
        help="从本地 application 快照读主键（推荐）",
    )
    p.add_argument(
        "--cache-file",
        default="/tmp/application_sn_snapshot_after.json",
        help="快照路径（含 mobile/group_user_id/sn）",
    )
    p.add_argument("--workers", type=int, default=20, help="apply 并行进程数")
    p.add_argument("--batch-size", type=int, default=200)
    p.add_argument("--mobile-prefix", default="", help="可选过滤，如 tk_")
    p.add_argument("--work-limit", type=int, default=0)
    p.add_argument(
        "--all-rows",
        action="store_true",
        help="DB 扫库模式：不跳过已是 {} 的行",
    )
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    dry_run = not args.apply
    env_path = str(Path(args.env).resolve())
    cfg = load_env(Path(args.env))

    if args.from_cache:
        cache_path = Path(args.cache_file)
        if not cache_path.is_file():
            p.error(
                "cache not found: %s\n"
                "可用: /tmp/application_sn_snapshot.json 或 "
                "/tmp/application_sn_snapshot_after.json\n"
                "或先跑: python3 repair_application_sn_from_suffix.py "
                "--env ./ng_migration.env --dry-run "
                "--cache-file /tmp/application_sn_snapshot_after.json"
                % cache_path
            )
        raw = load_snapshot_cache(cache_path)
        rows = extract_pk_rows(raw, args.mobile_prefix)
        if args.work_limit > 0:
            rows = rows[: args.work_limit]
        print(
            "pk rows=%s mobile_prefix=%r sample=%s"
            % (
                len(rows),
                args.mobile_prefix,
                rows[:3] if rows else [],
            ),
            flush=True,
        )
        if dry_run:
            print("dry-run only (use --apply)", flush=True)
            return 0
        if not rows:
            print("no rows", flush=True)
            return 0
        ok, skip = run_parallel(
            rows, args.workers, env_path, max(1, args.batch_size)
        )
        print("done ok=%s skip=%s" % (ok, skip), flush=True)
        return 0

    # DB scan fallback (single process)
    updated, seen = run_from_db(
        cfg,
        max(1, args.batch_size),
        only_non_empty=not args.all_rows,
        mobile_prefix=args.mobile_prefix,
        work_limit=max(0, args.work_limit),
        dry_run=dry_run,
    )
    print(
        "done dry_run=%s seen=%s updated=%s" % (int(dry_run), seen, updated),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
