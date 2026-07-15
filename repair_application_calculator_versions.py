#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""批量把目标库 application 三个 calculator 版本改成固定值。

  product_calculator_version  = 48
  repay_calculator_version    = 50
  rollover_calculator_version = 49

按主键 (mobile, group_user_id, sn) keyset 推进，避免全表 COUNT /
「UPDATE WHERE 版本不等 LIMIT」越跑越慢。

Usage:
  # 直接写库（推荐）
  python3 repair_application_calculator_versions.py --env ./ng_migration.env --apply

  # 先看列是否存在 + 抽样
  python3 repair_application_calculator_versions.py --env ./ng_migration.env --probe

  # 可选全表统计（大表很慢，默认不做）
  python3 repair_application_calculator_versions.py --env ./ng_migration.env --count
"""
from __future__ import print_function

import argparse
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pymysql
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
) -> List[dict]:
    """按主键顺序取下一批 PK，不带版本条件（保证匀速推进）。"""
    if last is None:
        sql = (
            "SELECT mobile, group_user_id, sn FROM application "
            "ORDER BY mobile, group_user_id, sn LIMIT %s"
        )
        params = (int(batch),)  # type: Tuple
    else:
        # (mobile, group_user_id, sn) > last
        sql = (
            "SELECT mobile, group_user_id, sn FROM application "
            "WHERE mobile > %s "
            "   OR (mobile = %s AND group_user_id > %s) "
            "   OR (mobile = %s AND group_user_id = %s AND sn > %s) "
            "ORDER BY mobile, group_user_id, sn LIMIT %s"
        )
        m, g, s = last
        params = (m, m, g, m, g, s, int(batch))
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
) -> int:
    session_opts(conn)
    last = None  # type: Optional[Tuple[str, Any, str]]
    round_no = 0
    scanned = 0
    affected_total = 0
    t0 = time.time()
    print(
        "start keyset update batch=%s (no full COUNT; progress every batch)"
        % batch,
        flush=True,
    )
    while True:
        round_no += 1
        t_batch = time.time()
        try:
            rows = fetch_pk_batch(conn, batch, last)
        except Exception as exc:
            print("select failed: %s ; reconnect..." % exc, flush=True)
            conn.ping(reconnect=True)
            session_opts(conn)
            rows = fetch_pk_batch(conn, batch, last)
        if not rows:
            break
        try:
            n = update_pk_batch(conn, rows, v_prod, v_repay, v_roll)
            conn.commit()
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            print("update failed batch=%s: %s" % (round_no, exc), flush=True)
            raise
        scanned += len(rows)
        affected_total += n
        last_row = rows[-1]
        last = (
            str(last_row["mobile"]),
            last_row["group_user_id"],
            str(last_row["sn"]),
        )
        print(
            "batch=%s scanned=%s affected=%s total_affected=%s "
            "last_mobile=%s elapsed=%.1fs batch_secs=%.2fs"
            % (
                round_no,
                scanned,
                n,
                affected_total,
                last[0][:24],
                time.time() - t0,
                time.time() - t_batch,
            ),
            flush=True,
        )
    print(
        "done scanned=%s affected=%s elapsed=%.1fs"
        % (scanned, affected_total, time.time() - t0),
        flush=True,
    )
    return affected_total


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
            "target=%s:%s/%s set product=%s repay=%s rollover=%s"
            % (
                cfg.get("TARGET_HOST"),
                cfg.get("TARGET_PORT", "3306"),
                cfg.get("TARGET_DB", "ng"),
                args.product,
                args.repay,
                args.rollover,
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

        apply_keyset(
            conn,
            max(100, int(args.batch)),
            args.product,
            args.repay,
            args.rollover,
        )
        return 0
    except Exception as exc:
        print("ERROR: %s" % exc, file=sys.stderr, flush=True)
        return 1
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
