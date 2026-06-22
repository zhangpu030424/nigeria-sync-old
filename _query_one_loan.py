#!/usr/bin/env python3
import json
import os
import sys
import importlib.util
from pathlib import Path

import pymysql

APP_NO = sys.argv[1] if len(sys.argv) > 1 else "178119493412026311"
HERE = Path(__file__).resolve().parent

for line in (HERE / "ng_migration.env").read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1)
    os.environ.setdefault(k.strip(), v.strip().strip("'\""))

spec = importlib.util.spec_from_file_location("mig", HERE / "ng_migration_run.py")
mig = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mig)

src = pymysql.connect(
    host=os.environ["SOURCE_HOST"],
    port=int(os.environ.get("SOURCE_PORT", 3306)),
    user=os.environ["SOURCE_USER"],
    password=os.environ["SOURCE_PASSWORD"],
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)

m, c = "ng_loan_market", "ng_loan_core"
with src.cursor() as cur:
    cur.execute(
        f"""
        SELECT a.applicationNo AS application_no, a.applicationNo AS sn,
               a.`userId` AS user_id, IFNULL(ca.sn, '') AS core_sn
        FROM {m}.application a
        LEFT JOIN {c}.application ca ON ca.ext_sn = a.applicationNo
        WHERE a.applicationNo = %s
        """,
        (APP_NO,),
    )
    app = cur.fetchone()

if not app:
    print(json.dumps({"error": "APPLICATION_NOT_FOUND", "application_no": APP_NO}, ensure_ascii=False))
    sys.exit(0)

# 与脚本一致：sn_to_app_no 的 key 是 market applicationNo
sn_to_app_no = {app["sn"]: app["application_no"]}
loans_script = mig._fetch_loan_rows_from_source(src, sn_to_app_no)

# 诊断：repay_plan 实际挂在 core sn 上
loans_via_core = []
core_sn = app.get("core_sn")
if core_sn:
    loans_via_core = mig._fetch_loan_rows_from_source(src, {str(core_sn): APP_NO})

with src.cursor() as cur:
    cur.execute(
        f"""
        SELECT rp.plan_sn, rp.sn, rp.start_date, rp.due_date, rp.prin_amt, rp.interest,
               rp.orig_fee, rp.penalty, rp.amt, rp.`status`, rp.repaid_amt,
               rp.repay_last_time, rp.settle_time, rp.created_at
        FROM {c}.repay_plan rp
        WHERE rp.sn = %s
        ORDER BY rp.plan_sn DESC
        LIMIT 3
        """,
        (core_sn,),
    )
    repay_plan_rows = list(cur.fetchall()) if core_sn else []

print(json.dumps({
    "application_no": APP_NO,
    "core_sn": core_sn,
    "loan_by_current_script": loans_script,
    "loan_count_script": len(loans_script),
    "note": "script queries repay_plan WHERE sn=applicationNo; see loan_if_matched_by_core_sn when empty",
    "repay_plan_on_core_sn_top3": repay_plan_rows,
    "loan_if_matched_by_core_sn": loans_via_core,
}, ensure_ascii=False, indent=2, default=str))
src.close()
