#!/usr/bin/env python3
import pymysql
from pymysql.cursors import DictCursor
from pathlib import Path

cfg = {}
for line in Path("/opt/ng-migration-runner/ng_migration.env").read_text().splitlines():
    if "=" in line and not line.strip().startswith("#"):
        k, v = line.split("=", 1)
        cfg[k.strip()] = v.strip().strip("'\"")

mu = int(cfg.get("MAX_USER_ID", "9153604"))
prog = {}
pf = Path("/tmp/ng_mig_all_progress.env")
if pf.exists():
    for line in pf.read_text().splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            prog[k.strip()] = v.strip()
upto = max((int(prog[k]) for k in prog if k.startswith("app_lo.W")), default=0)

src = pymysql.connect(
    host=cfg["SOURCE_HOST"], port=int(cfg.get("SOURCE_PORT", "3306")),
    user=cfg["SOURCE_USER"], password=cfg["SOURCE_PASSWORD"],
    charset="utf8mb4", cursorclass=DictCursor, read_timeout=120,
)
with src.cursor() as cur:
    cur.execute("SELECT COUNT(*) AS c FROM ng_loan_market.`user` WHERE id <= %s", (mu,))
    print("source_user\t%d" % cur.fetchone()["c"])
    cur.execute(
        "SELECT COUNT(*) AS c FROM ng_loan_market.application "
        "WHERE applicationNo IS NOT NULL AND applicationNo <> ''"
    )
    print("source_application_all\t%d" % cur.fetchone()["c"])
    if upto:
        cur.execute(
            "SELECT COUNT(*) AS c FROM ng_loan_market.application "
            "WHERE applicationNo IS NOT NULL AND applicationNo <> '' AND id <= %s",
            (upto,),
        )
        print("source_application_upto\t%d\tid<=%d" % (cur.fetchone()["c"], upto))
    cur.execute("SELECT MAX(id) AS m FROM ng_loan_market.application")
    print("source_app_max_id\t%d" % int(cur.fetchone()["m"] or 0))
src.close()
