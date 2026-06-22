#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""只读对比源库与目标库行数；不修改数据、不停止迁移。"""
import os
import re
import sys
from collections import Counter
from pathlib import Path

import pymysql
from pymysql.cursors import DictCursor


def load_env(path: str) -> dict:
    env = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip("'\"")
    return env


def connect(cfg: dict, kind: str):
    kw = dict(
        charset="utf8mb4",
        cursorclass=DictCursor,
        connect_timeout=30,
        read_timeout=120,
    )
    if kind == "src":
        return pymysql.connect(
            host=cfg["SOURCE_HOST"],
            port=int(cfg.get("SOURCE_PORT", "3306")),
            user=cfg["SOURCE_USER"],
            password=cfg["SOURCE_PASSWORD"],
            **kw,
        )
    return pymysql.connect(
        host=cfg["TARGET_HOST"],
        port=int(cfg.get("TARGET_PORT", "3306")),
        user=cfg["TARGET_USER"],
        password=cfg["TARGET_PASSWORD"],
        database=cfg["TARGET_DB"],
        **kw,
    )


def q1(conn, sql, args=None):
    with conn.cursor() as cur:
        cur.execute(sql, args or ())
        return cur.fetchone()


def load_progress(path: str) -> dict:
    out = {}
    p = Path(path)
    if not p.exists():
        return out
    for line in p.read_text(encoding="utf-8").splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def tgt_table_rows(cfg: dict, table: str) -> tuple:
    """迁移进行中优先 information_schema 估算，避免大表 COUNT 拖垮连接。"""
    db = cfg["TARGET_DB"]
    conn = connect(cfg, "tgt")
    try:
        row = q1(
            conn,
            """
            SELECT TABLE_ROWS AS c
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """,
            (db, table),
        )
        approx = int(row["c"] or 0) if row else 0
    finally:
        conn.close()

    if table in ("user", "user_info", "loan"):
        conn = connect(cfg, "tgt")
        try:
            exact = q1(conn, "SELECT COUNT(*) AS c FROM `%s`" % table)["c"]
            return exact, "exact"
        except Exception:
            return approx, "approx"
    return approx, "approx"


def tgt_field_empty(cfg: dict, table: str, cond: str) -> str:
    conn = connect(cfg, "tgt")
    try:
        return str(q1(conn, "SELECT COUNT(*) AS c FROM `%s` WHERE %s" % (table, cond))["c"])
    except Exception as exc:
        return "skip(%s)" % type(exc).__name__
    finally:
        conn.close()


def main() -> int:
    base = Path(__file__).resolve().parent
    cfg = load_env(str(base / "ng_migration.env"))
    max_uid = int(cfg.get("MAX_USER_ID", "9153604"))
    prog = load_progress(cfg.get("PROGRESS_FILE", "/tmp/ng_mig_all_progress.env"))
    skip_path = cfg.get("SKIP_LOG_FILE") or "/tmp/ng_mig_all.skip.log"

    mig_running = os.system("pgrep -f ng_migration_run.py >/dev/null 2>&1") == 0

    src = connect(cfg, "src")
    try:
        src_user = q1(src, "SELECT COUNT(*) AS c FROM ng_loan_market.`user` WHERE id <= %s", (max_uid,))["c"]
        src_app_all = q1(
            src,
            """
            SELECT COUNT(*) AS c FROM ng_loan_market.application a
            WHERE a.applicationNo IS NOT NULL AND a.applicationNo <> ''
            """,
        )["c"]
        src_app_max_id = q1(src, "SELECT MAX(id) AS m FROM ng_loan_market.application")["m"]
        src_ud_bank = q1(
            src,
            """
            SELECT COUNT(DISTINCT ud.userId) AS c
            FROM ng_loan_market.user_data ud
            INNER JOIN (
                SELECT userId, MAX(id) AS max_id FROM ng_loan_market.user_data GROUP BY userId
            ) t ON ud.userId = t.userId AND ud.id = t.max_id
            WHERE ud.bankCode IS NOT NULL AND ud.bankCode <> ''
              AND ud.bankAccount IS NOT NULL AND ud.bankAccount <> ''
              AND ud.userId <= %s
            """,
            (max_uid,),
        )["c"]
        src_user_product = q1(
            src,
            """
            SELECT COUNT(*) AS c FROM (
                SELECT userId, productId, MAX(id) AS max_id
                FROM ng_loan_market.application
                WHERE userId > 0 AND userId <= %s
                  AND productId IS NOT NULL AND productId <> 0
                GROUP BY userId, productId
            ) x
            """,
            (max_uid,),
        )["c"]

        app_w_keys = [k for k in prog if k.startswith("app_lo.W")]
        app_synced_upto = max((int(prog[k]) for k in app_w_keys), default=0)
        src_app_upto = 0
        if app_synced_upto:
            src_app_upto = q1(
                src,
                """
                SELECT COUNT(*) AS c FROM ng_loan_market.application a
                WHERE a.applicationNo IS NOT NULL AND a.applicationNo <> ''
                  AND a.id <= %s
                """,
                (app_synced_upto,),
            )["c"]
    finally:
        src.close()

    tgt_counts = {}
    tgt_modes = {}
    for t in (
        "user", "user_info", "user_bankcard", "user_product",
        "application", "loan", "id_mapping",
    ):
        c, mode = tgt_table_rows(cfg, t)
        tgt_counts[t] = c
        tgt_modes[t] = mode

    field_issues = {
        "application.gaid_idfa_empty": tgt_field_empty(
            cfg, "application", "gaid_idfa IS NULL OR gaid_idfa = ''"
        ),
        "application.bank_account_number_empty": tgt_field_empty(
            cfg, "application", "bank_account_number IS NULL OR bank_account_number = ''"
        ),
        "application.id_number_empty": tgt_field_empty(
            cfg, "application", "id_number IS NULL OR id_number = ''"
        ),
        "user_info.id_number_empty": tgt_field_empty(
            cfg, "user_info", "id_number IS NULL OR id_number = ''"
        ),
    }

    kind_cnt = Counter()
    vt_type_cnt = Counter()
    if Path(skip_path).exists():
        vt_re = re.compile(r"vt_type=(\S+)")
        with open(skip_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                parts = line.split("\t")
                if len(parts) < 2:
                    continue
                kind_cnt[parts[1]] += 1
                if parts[1] == "vt_miss":
                    m = vt_re.search(line)
                    if m:
                        vt_type_cnt[m.group(1)] += 1

    print("=== MIGRATION STATUS (read-only) ===")
    print("running:", "yes" if mig_running else "no")
    print("full_user_done:", prog.get("full_user_done", "0"))
    print("full_app_done:", prog.get("full_app_done", "0"))
    print("app_synced_max_id:", app_synced_upto, "/ src_max:", src_app_max_id)
    print("note: target 大表用 information_schema 估算(approx)，避免影响正在跑的写入")
    print()

    print("=== ROW COUNTS: SOURCE vs TARGET ===")
    rows = [
        ("user", src_user, tgt_counts["user"], "源 user.id<=%d" % max_uid),
        ("user_info", src_user, tgt_counts["user_info"], "应与 user 接近"),
        ("user_bankcard", src_ud_bank, tgt_counts["user_bankcard"], "源=有银行卡 distinct user"),
        ("user_product", src_user_product, tgt_counts["user_product"], "源=userId+productId"),
        ("application_all", src_app_all, tgt_counts["application"], "全量有效 application"),
        ("application_upto", src_app_upto, tgt_counts["application"], "id<=%d 已扫段" % app_synced_upto),
        ("loan", None, tgt_counts["loan"], "仅有 repay_plan 的已入库单"),
        ("id_mapping", None, tgt_counts["id_mapping"], "每单多行"),
    ]
    print("%-22s %12s %12s %12s %6s  %s" % ("table", "source", "target", "delta", "mode", "note"))
    for name, s, t, note in rows:
        mode = tgt_modes.get(name.split("_")[0] if name.startswith("application") else name, "-")
        if name.startswith("application"):
            mode = tgt_modes.get("application", "?")
        if s is None:
            print("%-22s %12s %12d %12s %6s  %s" % (name, "-", t, "-", mode, note))
        else:
            print("%-22s %12d %12d %12d %6s  %s" % (name, s, t, t - s, mode, note))
    print()

    print("=== TARGET: 敏感字段为空（已入库行） ===")
    for k, v in sorted(field_issues.items()):
        print("  %s: %s" % (k, v))
    print()

    print("=== SKIP LOG ===")
    print("total:", sum(kind_cnt.values()))
    for k, v in kind_cnt.most_common():
        print("  %s: %d" % (k, v))
    for k, v in vt_type_cnt.most_common():
        print("  vt_miss.%s: %d" % (k, v))
    return 0


if __name__ == "__main__":
    sys.exit(main())
