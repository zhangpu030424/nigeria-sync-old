#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""批量把目标库 application 三个 calculator 版本改成固定值。

  product_calculator_version  = 48
  repay_calculator_version    = 50
  rollover_calculator_version = 49

直接 UPDATE 目标库全表（不依赖 reconcile plan / SINCE_DATE）。
按 LIMIT 分批提交，避免长事务锁死。

Usage:
  # 只统计，不写库
  python3 repair_application_calculator_versions.py --env ./ng_migration.env

  # 写库
  python3 repair_application_calculator_versions.py --env ./ng_migration.env --apply --batch 5000
"""
from __future__ import print_function

import argparse
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pymysql
from pymysql.cursors import DictCursor

HERE = Path(__file__).resolve().parent

DEFAULTS = {
    "product_calculator_version": 48,
    "repay_calculator_version": 50,
    "rollover_calculator_version": 49,
}


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


def _need_where(v_prod: int, v_repay: int, v_roll: int) -> str:
    return (
        "(IFNULL(product_calculator_version, -1) <> {0}"
        " OR IFNULL(repay_calculator_version, -1) <> {1}"
        " OR IFNULL(rollover_calculator_version, -1) <> {2})"
    ).format(int(v_prod), int(v_repay), int(v_roll))


def count_stats(conn, v_prod: int, v_repay: int, v_roll: int) -> Tuple[int, int]:
    where = _need_where(v_prod, v_repay, v_roll)
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM application")
        total = int((cur.fetchone() or {}).get("n") or 0)
        cur.execute("SELECT COUNT(*) AS n FROM application WHERE " + where)
        need = int((cur.fetchone() or {}).get("n") or 0)
    return total, need


def apply_batches(
    conn,
    batch: int,
    v_prod: int,
    v_repay: int,
    v_roll: int,
) -> int:
    where = _need_where(v_prod, v_repay, v_roll)
    sql = (
        "UPDATE application SET "
        "product_calculator_version=%s, "
        "repay_calculator_version=%s, "
        "rollover_calculator_version=%s "
        "WHERE " + where + " LIMIT %s"
    )
    params = (int(v_prod), int(v_repay), int(v_roll), int(batch))
    affected_total = 0
    round_no = 0
    t0 = time.time()
    while True:
        round_no += 1
        with conn.cursor() as cur:
            cur.execute(sql, params)
            n = int(cur.rowcount or 0)
        conn.commit()
        affected_total += n
        print(
            "batch=%s affected=%s total_affected=%s elapsed=%.1fs"
            % (round_no, n, affected_total, time.time() - t0),
            flush=True,
        )
        if n <= 0:
            break
    return affected_total


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Bulk set application calculator version defaults",
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true", help="写库；默认只统计")
    p.add_argument("--batch", type=int, default=5000, help="每批 LIMIT，默认 5000")
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
        total, need = count_stats(conn, args.product, args.repay, args.rollover)
        print(
            "application rows total=%s need_update=%s already_ok=%s"
            % (total, need, total - need),
            flush=True,
        )
        if not args.apply:
            print("dry-run only (add --apply to write)", flush=True)
            return 0
        if need <= 0:
            print("nothing to update", flush=True)
            return 0
        n = apply_batches(
            conn, max(1, args.batch), args.product, args.repay, args.rollover,
        )
        total2, need2 = count_stats(conn, args.product, args.repay, args.rollover)
        print(
            "done affected=%s remain_need_update=%s total=%s"
            % (n, need2, total2),
            flush=True,
        )
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
