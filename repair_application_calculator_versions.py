#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""批量把目标库 application 三个 calculator 版本改成固定值。

  product_calculator_version  = 48
  repay_calculator_version    = 50
  rollover_calculator_version = 49

按 CRC32(mobile) 分片多进程：每进程独立读 + UPDATE。
默认 --workers 16。

Usage:
  python3 repair_application_calculator_versions.py --env ./ng_migration.env --apply
  python3 repair_application_calculator_versions.py --env ./ng_migration.env --apply --workers 16 --batch 2000
  python3 repair_application_calculator_versions.py --env ./ng_migration.env --probe
"""
from __future__ import print_function

import argparse
import multiprocessing
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pymysql
from pymysql.constants import CLIENT
from pymysql.cursors import DictCursor

HERE = Path(__file__).resolve().parent

DEFAULTS = {
    "product_calculator_version": 48,
    "repay_calculator_version": 50,
    "rollover_calculator_version": 49,
}
VERSION_COLS = (
    "product_calculator_version",
    "repay_calculator_version",
    "rollover_calculator_version",
)


def load_env(path: Path) -> Dict[str, str]:
    cfg = {}  # type: Dict[str, str]
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
        connect_timeout=30,
        read_timeout=600,
        write_timeout=600,
        autocommit=False,
        # rowcount = matched rows（值未变也计数），便于看进度
        client_flag=CLIENT.FOUND_ROWS,
    )


def _as_int(val: Any, default: int = -999999) -> int:
    if val is None or val == "":
        return default
    try:
        return int(val)
    except (TypeError, ValueError):
        try:
            return int(float(val))
        except (TypeError, ValueError):
            return default


def row_needs_update(row: dict, v_prod: int, v_repay: int, v_roll: int) -> bool:
    return (
        _as_int(row.get("product_calculator_version")) != v_prod
        or _as_int(row.get("repay_calculator_version")) != v_repay
        or _as_int(row.get("rollover_calculator_version")) != v_roll
    )


def session_opts(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SET SESSION unique_checks=0")
        cur.execute("SET SESSION foreign_key_checks=0")
        try:
            cur.execute("SET SESSION sql_log_bin=0")
        except Exception:
            pass
    conn.commit()


def ensure_columns(conn, db: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COLUMN_NAME AS c
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME='application'
              AND COLUMN_NAME IN (%s,%s,%s)
            """,
            (db, VERSION_COLS[0], VERSION_COLS[1], VERSION_COLS[2]),
        )
        found = {str(r["c"]) for r in cur.fetchall()}
    missing = [c for c in VERSION_COLS if c not in found]
    if missing:
        raise RuntimeError(
            "application 表缺少列: %s （请先 ALTER 加列再跑）" % ",".join(missing)
        )
    return list(VERSION_COLS)


def probe_sample(conn, v_prod: int, v_repay: int, v_roll: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT product_calculator_version AS p,
                   repay_calculator_version AS r,
                   rollover_calculator_version AS o,
                   COUNT(*) AS n
            FROM application
            GROUP BY product_calculator_version,
                     repay_calculator_version,
                     rollover_calculator_version
            ORDER BY n DESC
            LIMIT 20
            """
        )
        rows = list(cur.fetchall())
    print("top version combos (sample group by, may be slow on huge table):", flush=True)
    if not rows:
        print("  (empty table)", flush=True)
        return
    for r in rows:
        mark = ""
        if (
            int(r["p"] or -1) == v_prod
            and int(r["r"] or -1) == v_repay
            and int(r["o"] or -1) == v_roll
        ):
            mark = "  <- target"
        print(
            "  p=%s r=%s o=%s n=%s%s" % (r["p"], r["r"], r["o"], r["n"], mark),
            flush=True,
        )


def fetch_pk_batch(
    conn,
    batch: int,
    last: Optional[Tuple[str, Any, str]],
    shard_id: int = 0,
    shards: int = 1,
) -> List[dict]:
    """按主键顺序取下一批行；shards>1 时 CRC32(mobile)%shards=shard_id。"""
    cols = (
        "mobile, group_user_id, sn, "
        "product_calculator_version, repay_calculator_version, "
        "rollover_calculator_version"
    )
    shard_clause = ""
    shard_params = []  # type: List[Any]
    if int(shards) > 1:
        # 仅给 pymysql：%% → SQL 的 %；勿再经 str.format
        shard_clause = "CRC32(mobile) %% %s = %s"

    if last is None:
        if shard_clause:
            where = "WHERE " + shard_clause
            params = [int(shards), int(shard_id), int(batch)]
        else:
            where = ""
            params = [int(batch)]
        sql = (
            "SELECT " + cols + " FROM application " + where
            + " ORDER BY mobile, group_user_id, sn LIMIT %s"
        )
    else:
        key_clause = (
            "(mobile > %s "
            " OR (mobile = %s AND group_user_id > %s) "
            " OR (mobile = %s AND group_user_id = %s AND sn > %s))"
        )
        m, g, s = last
        if shard_clause:
            where = "WHERE " + shard_clause + " AND " + key_clause
            params = [int(shards), int(shard_id), m, m, g, m, g, s, int(batch)]
        else:
            where = "WHERE " + key_clause
            params = [m, m, g, m, g, s, int(batch)]
        sql = (
            "SELECT " + cols + " FROM application " + where
            + " ORDER BY mobile, group_user_id, sn LIMIT %s"
        )
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def update_pk_batch(
    conn,
    rows: Sequence[dict],
    v_prod: int,
    v_repay: int,
    v_roll: int,
) -> int:
    if not rows:
        return 0
    parts = []
    params = []  # type: List
    for r in rows:
        parts.append("SELECT %s AS mobile, %s AS group_user_id, %s AS sn")
        params.extend([r["mobile"], r["group_user_id"], r["sn"]])
    sql = (
        "UPDATE application a "
        "INNER JOIN (" + " UNION ALL ".join(parts) + ") x "
        "ON a.mobile=x.mobile AND a.group_user_id=x.group_user_id AND a.sn=x.sn "
        "SET a.product_calculator_version=%s, "
        "    a.repay_calculator_version=%s, "
        "    a.rollover_calculator_version=%s"
    )
    params.extend([int(v_prod), int(v_repay), int(v_roll)])
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return int(cur.rowcount or 0)


def apply_keyset(
    conn,
    batch: int,
    v_prod: int,
    v_repay: int,
    v_roll: int,
    shard_id: int = 0,
    shards: int = 1,
    label: str = "",
) -> Dict[str, int]:
    session_opts(conn)
    last = None  # type: Optional[Tuple[str, Any, str]]
    round_no = 0
    scanned = 0
    need_total = 0
    changed_total = 0
    t0 = time.time()
    prefix = ("[%s] " % label) if label else ""
    print(
        "%sstart keyset shard=%s/%s batch=%s target=(%s,%s,%s)"
        % (prefix, shard_id, shards, batch, v_prod, v_repay, v_roll),
        flush=True,
    )
    while True:
        round_no += 1
        t_batch = time.time()
        try:
            rows = fetch_pk_batch(conn, batch, last, shard_id, shards)
        except Exception as exc:
            print("%sselect failed: %s ; reconnect..." % (prefix, exc), flush=True)
            conn.ping(reconnect=True)
            session_opts(conn)
            rows = fetch_pk_batch(conn, batch, last, shard_id, shards)
        if not rows:
            break

        need_rows = [r for r in rows if row_needs_update(r, v_prod, v_repay, v_roll)]
        sample = rows[0]
        n = 0
        if need_rows:
            try:
                n = update_pk_batch(conn, need_rows, v_prod, v_repay, v_roll)
                conn.commit()
            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                print("%supdate failed batch=%s: %s" % (prefix, round_no, exc), flush=True)
                raise

        scanned += len(rows)
        need_total += len(need_rows)
        changed_total += n
        last_row = rows[-1]
        last = (
            str(last_row["mobile"]),
            last_row["group_user_id"],
            str(last_row["sn"]),
        )
        # 多进程时每 10 批打一行，避免刷屏；单进程每批都打
        if shards <= 1 or round_no == 1 or round_no % 10 == 0 or len(rows) < batch:
            print(
                "%sbatch=%s scanned=%s need=%s changed=%s "
                "need_total=%s changed_total=%s "
                "sample_ver=(%s,%s,%s) last_mobile=%s "
                "elapsed=%.1fs batch_secs=%.2fs"
                % (
                    prefix,
                    round_no,
                    scanned,
                    len(need_rows),
                    n,
                    need_total,
                    changed_total,
                    sample.get("product_calculator_version"),
                    sample.get("repay_calculator_version"),
                    sample.get("rollover_calculator_version"),
                    str(last[0])[:24],
                    time.time() - t0,
                    time.time() - t_batch,
                ),
                flush=True,
            )
    print(
        "%sdone scanned=%s need_total=%s changed_total=%s elapsed=%.1fs"
        % (prefix, scanned, need_total, changed_total, time.time() - t0),
        flush=True,
    )
    return {
        "scanned": scanned,
        "need_total": need_total,
        "changed_total": changed_total,
    }


def _worker_apply(spec: dict) -> Dict[str, int]:
    """跨进程 worker：每进程独立连接，负责一个 CRC32 分片。"""
    cfg = spec["cfg"]
    conn = connect_target(cfg)
    try:
        return apply_keyset(
            conn,
            int(spec["batch"]),
            int(spec["product"]),
            int(spec["repay"]),
            int(spec["rollover"]),
            shard_id=int(spec["shard_id"]),
            shards=int(spec["shards"]),
            label=str(spec["label"]),
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass


def apply_parallel(
    cfg: Dict[str, str],
    workers: int,
    batch: int,
    v_prod: int,
    v_repay: int,
    v_roll: int,
) -> Dict[str, int]:
    workers = max(1, int(workers))
    batch = max(100, int(batch))
    if workers == 1:
        conn = connect_target(cfg)
        try:
            return apply_keyset(
                conn, batch, v_prod, v_repay, v_roll,
                shard_id=0, shards=1, label="w0",
            )
        finally:
            conn.close()

    specs = []
    for i in range(workers):
        specs.append({
            "cfg": cfg,
            "batch": batch,
            "product": v_prod,
            "repay": v_repay,
            "rollover": v_roll,
            "shard_id": i,
            "shards": workers,
            "label": "w%s" % i,
        })
    print(
        "parallel apply workers=%s batch=%s (CRC32(mobile) %% %s)"
        % (workers, batch, workers),
        flush=True,
    )
    t0 = time.time()
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(processes=workers) as pool:
        results = pool.map(_worker_apply, specs)
    total = {"scanned": 0, "need_total": 0, "changed_total": 0}
    for r in results:
        total["scanned"] += int(r.get("scanned") or 0)
        total["need_total"] += int(r.get("need_total") or 0)
        total["changed_total"] += int(r.get("changed_total") or 0)
    print(
        "all workers done scanned=%s need_total=%s changed_total=%s elapsed=%.1fs"
        % (
            total["scanned"],
            total["need_total"],
            total["changed_total"],
            time.time() - t0,
        ),
        flush=True,
    )
    if total["scanned"] > 0 and total["need_total"] == 0:
        print(
            "NOTE: 扫描范围内三列已全是目标值，无需再改。可用 --probe 看分布。",
            flush=True,
        )
    return total


def count_stats(conn, v_prod: int, v_repay: int, v_roll: int) -> Tuple[int, int]:
    print("counting (full table, may be slow)...", flush=True)
    where = (
        "(IFNULL(product_calculator_version, -1) <> %s"
        " OR IFNULL(repay_calculator_version, -1) <> %s"
        " OR IFNULL(rollover_calculator_version, -1) <> %s)"
    )
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM application")
        total = int((cur.fetchone() or {}).get("n") or 0)
        print("  total=%s" % total, flush=True)
        cur.execute(
            "SELECT COUNT(*) AS n FROM application WHERE " + where,
            (v_prod, v_repay, v_roll),
        )
        need = int((cur.fetchone() or {}).get("n") or 0)
        print("  need_update=%s" % need, flush=True)
    return total, need


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Bulk set application calculator version defaults",
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true", help="按主键全表推进写库")
    p.add_argument("--probe", action="store_true", help="检查列 + GROUP BY 抽样")
    p.add_argument("--count", action="store_true", help="全表 COUNT（很慢）")
    p.add_argument("--batch", type=int, default=2000, help="每批行数，默认 2000")
    p.add_argument(
        "--workers", type=int, default=16,
        help="并发进程数（CRC32 分片读写），默认 16",
    )
    p.add_argument("--product", type=int, default=DEFAULTS["product_calculator_version"])
    p.add_argument("--repay", type=int, default=DEFAULTS["repay_calculator_version"])
    p.add_argument("--rollover", type=int, default=DEFAULTS["rollover_calculator_version"])
    args = p.parse_args(argv)

    env_path = Path(args.env)
    if not env_path.is_file():
        print("env not found: %s" % env_path, file=sys.stderr)
        return 2

    cfg = load_env(env_path)
    conn = connect_target(cfg)
    try:
        print(
            "target=%s:%s/%s set product=%s repay=%s rollover=%s workers=%s"
            % (
                cfg.get("TARGET_HOST"),
                cfg.get("TARGET_PORT", "3306"),
                cfg.get("TARGET_DB", "ng"),
                args.product,
                args.repay,
                args.rollover,
                args.workers,
            ),
            flush=True,
        )
        db = cfg.get("TARGET_DB", "ng")
        ensure_columns(conn, db)
        print("columns ok: %s" % ", ".join(VERSION_COLS), flush=True)

        if args.probe or args.count or not args.apply:
            if args.count:
                count_stats(conn, args.product, args.repay, args.rollover)
            if args.probe:
                probe_sample(conn, args.product, args.repay, args.rollover)
            if not args.apply:
                print("dry-run / probe only (add --apply to write)", flush=True)
                return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if args.apply:
        try:
            apply_parallel(
                cfg,
                max(1, int(args.workers)),
                max(100, int(args.batch)),
                args.product,
                args.repay,
                args.rollover,
            )
        except Exception as exc:
            print("ERROR: %s" % exc, file=sys.stderr, flush=True)
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
