#!/usr/bin/env python3
import pymysql
from pymysql.cursors import DictCursor
from pathlib import Path

cfg = {}
for line in Path("/opt/ng-migration-runner/ng_migration.env").read_text().splitlines():
    if "=" in line and not line.strip().startswith("#"):
        k, v = line.split("=", 1)
        cfg[k.strip()] = v.strip().strip("'\"")

c = pymysql.connect(
    host=cfg["TARGET_HOST"], port=int(cfg["TARGET_PORT"]),
    user=cfg["TARGET_USER"], password=cfg["TARGET_PASSWORD"],
    database=cfg["TARGET_DB"], charset="utf8mb4", cursorclass=DictCursor,
    read_timeout=600,
)
with c.cursor() as cur:
    cur.execute("SELECT COUNT(*) AS c FROM application")
    print("application_exact\t%d" % cur.fetchone()["c"])
    cur.execute(
        "SELECT COUNT(*) AS c FROM application "
        "WHERE gaid_idfa IS NULL OR gaid_idfa = ''"
    )
    print("application_gaid_empty\t%d" % cur.fetchone()["c"])
    cur.execute(
        "SELECT COUNT(*) AS c FROM application "
        "WHERE bank_account_number IS NULL OR bank_account_number = ''"
    )
    print("application_bank_empty\t%d" % cur.fetchone()["c"])
c.close()
