#!/usr/bin/env python3
import pymysql
from pymysql.cursors import DictCursor
from pathlib import Path

cfg = {}
for line in Path("/opt/ng-migration-runner/ng_migration.env").read_text().splitlines():
    if "=" in line and not line.strip().startswith("#"):
        k, v = line.split("=", 1)
        cfg[k.strip()] = v.strip().strip("'\"")

tgt = pymysql.connect(
    host=cfg["TARGET_HOST"], port=int(cfg["TARGET_PORT"]),
    user=cfg["TARGET_USER"], password=cfg["TARGET_PASSWORD"],
    database=cfg["TARGET_DB"], charset="utf8mb4", cursorclass=DictCursor, read_timeout=90,
)
db = cfg["TARGET_DB"]
with tgt.cursor() as cur:
    for t in ("user", "user_info"):
        cur.execute("SELECT COUNT(*) AS c FROM `%s`" % t)
        print("%s_exact\t%d" % (t, cur.fetchone()["c"]))
    cur.execute(
        "SELECT table_name, table_rows FROM information_schema.tables "
        "WHERE table_schema=%s AND table_name IN "
        "('user_bankcard','user_product','application','loan','id_mapping')",
        (db,),
    )
    for r in cur.fetchall():
        name = r.get("table_name") or r.get("TABLE_NAME")
        rows = r.get("table_rows") if r.get("table_rows") is not None else r.get("TABLE_ROWS")
        print("approx\t%s\t%d" % (name, int(rows or 0)))
    for cond, label in (
        ("gaid_idfa IS NULL OR gaid_idfa = ''", "application_gaid_empty"),
        ("bank_account_number IS NULL OR bank_account_number = ''", "application_bank_empty"),
    ):
        try:
            cur.execute("SELECT COUNT(*) AS c FROM application WHERE %s" % cond)
            print("%s\t%d" % (label, cur.fetchone()["c"]))
        except Exception as e:
            print("%s\tskip" % label)
tgt.close()
