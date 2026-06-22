#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Backfill ng.user_bankcard.id with snowflake IDs for rows where id=0 only."""
import argparse
import os
import sys
import time
from pathlib import Path
from typing import Dict, List

import pymysql
from pymysql.cursors import DictCursor

import ng_migration_run as mig


HERE = Path(__file__).resolve().parent
DEFAULT_ENV = HERE / "ng_migration.env"
DEFAULT_PROGRESS = "/tmp/ng_user_bankcard_id_backfill.progress"


def load_env(path: Path) -> Dict[str, str]:
    cfg: Dict[str, str] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            cfg[k.strip()] = v.strip().strip("'\"")
    for k, v in cfg.items():
        os.environ.setdefault(k, v)
    return cfg


def connect_target(cfg: Dict[str, str]):
    return pymysql.connect(
        host=cfg["TARGET_HOST"],
        port=int(cfg.get("TARGET_PORT", "3306")),
        user=cfg["TARGET_USER"],
        password=cfg["TARGET_PASSWORD"],
        database=cfg["TARGET_DB"],
        charset="utf8mb4",
        autocommit=False,
        cursorclass=DictCursor,
        connect_timeout=30,
        read_timeout=300,
        write_timeout=300,
    )


def read_progress(path: str) -> int:
    p = Path(path)
    if not p.exists():
        return 0
    text = p.read_text(encoding="utf-8").strip()
    return int(text or "0")


def write_progress(path: str, updated_rows: int) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(str(int(updated_rows)), encoding="utf-8")


def fetch_batch(conn, batch_size: int) -> List[dict]:
    sql = """
        SELECT group_user_id, bank_code, bank_account_number
        FROM user_bankcard
        WHERE id = 0
        ORDER BY group_user_id ASC
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (batch_size,))
        return list(cur.fetchall())


def update_batch(conn, rows: List[dict], id_gen: mig.SnowflakeIdGenerator) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMPORARY TABLE IF NOT EXISTS tmp_user_bankcard_id_backfill (
                group_user_id BIGINT NOT NULL,
                bank_account_number VARCHAR(255) NOT NULL,
                new_id BIGINT NOT NULL,
                PRIMARY KEY (group_user_id, bank_account_number)
            ) ENGINE=MEMORY
        """)
        cur.execute("TRUNCATE TABLE tmp_user_bankcard_id_backfill")
        params = [
            (
                int(row["group_user_id"]),
                row.get("bank_account_number") or "",
                id_gen.next_id(),
            )
            for row in rows
        ]
        cur.executemany(
            """
            INSERT INTO tmp_user_bankcard_id_backfill
                (group_user_id, bank_account_number, new_id)
            VALUES (%s, %s, %s)
            """,
            params,
        )
        cur.execute("""
            UPDATE user_bankcard ub
            INNER JOIN tmp_user_bankcard_id_backfill t
                ON ub.group_user_id = t.group_user_id
               AND ub.bank_account_number = t.bank_account_number
            SET ub.id = t.new_id
            WHERE ub.id = 0
        """)
        return cur.rowcount


def count_remaining(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM user_bankcard WHERE id = 0")
        return int(cur.fetchone()["c"] or 0)


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Backfill user_bankcard.id snowflake IDs for rows where id=0.",
    )
    p.add_argument("--env", default=str(DEFAULT_ENV))
    p.add_argument("--batch-size", type=int, default=5000)
    p.add_argument("--progress-file", default=DEFAULT_PROGRESS)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--reset-progress", action="store_true")
    p.add_argument("--worker-id", type=int, default=None)
    p.add_argument("--epoch-ms", type=int, default=None)
    return p.parse_args(argv)


def main(argv: List[str] = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be positive")
    cfg = load_env(Path(args.env))
    if args.worker_id is not None:
        cfg["snowflake_worker_id"] = args.worker_id
    if args.epoch_ms is not None:
        cfg["snowflake_epoch_ms"] = args.epoch_ms
    id_gen = mig.get_snowflake_generator(cfg)

    if args.reset_progress and Path(args.progress_file).exists():
        Path(args.progress_file).unlink()
    progress_updated = read_progress(args.progress_file)
    conn = connect_target(cfg)
    total_updated = 0
    t0 = time.time()
    try:
        remaining_before = count_remaining(conn)
        print(f"remaining_before={remaining_before} previous_progress_updated={progress_updated}")
        while True:
            rows = fetch_batch(conn, args.batch_size)
            if not rows:
                break
            max_group_user_id = max(int(row["group_user_id"]) for row in rows)
            if args.dry_run:
                print(
                    f"dry_run batch rows={len(rows)} "
                    f"group_user_id<= {max_group_user_id}"
                )
                break
            updated = update_batch(conn, rows, id_gen)
            conn.commit()
            total_updated += updated
            write_progress(args.progress_file, progress_updated + total_updated)
            elapsed = max(time.time() - t0, 0.001)
            print(
                f"batch rows={len(rows)} updated={updated} total_updated={total_updated} "
                f"group_user_id<= {max_group_user_id} elapsed={elapsed:.1f}s"
            )
            if updated == 0:
                raise RuntimeError("selected id=0 rows but updated 0 rows; aborting to avoid loop")
        remaining_after = count_remaining(conn)
        print(f"remaining_after={remaining_after} total_updated={total_updated}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
