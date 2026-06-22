#!/usr/bin/env python3
import json
import os
import sys
import importlib.util
from pathlib import Path

import pymysql

APP_NO = sys.argv[1] if len(sys.argv) > 1 else "176504777512022039"
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
sql = f"""
    SELECT
        a.id AS src_id, a.applicationNo AS application_no,
        CASE WHEN a.mobile LIKE '+234%%' THEN a.mobile
             WHEN a.mobile LIKE '234%%' THEN CONCAT('+', a.mobile)
             WHEN a.mobile LIKE '0%%' THEN CONCAT('+234', SUBSTRING(a.mobile, 2))
             ELSE CONCAT('+234', a.mobile) END AS mobile,
        '1.0.0' AS app_version, a.`appId` AS app_id, a.`userId` AS user_id,
        a.applicationNo AS sn,
        CASE WHEN a.`repeatLoan` = 0 THEN 1 ELSE 0 END AS is_first_apply,
        IFNULL(NULLIF(a.gaid, ''), NULL) AS gaid_idfa,
        IFNULL(d.deviceUUID, '') AS device_uuid,
        IFNULL(a.bankCode, '') AS bank_code,
        IFNULL(a.bankAccount, '') AS bank_account_number,
        CAST(a.`productId` AS CHAR) AS product_id,
        a.term, a.shouldLoanAmount AS should_loan_amount,
        a.amount AS amount, a.repayment AS repayment,
        a.disburseAmount AS disburse_amount,
        a.applyDate AS apply_date, a.dueDate AS due_date,
        IFNULL(ca.apply_time, 0) AS core_apply_time,
        IFNULL(ca.audit_time, 0) AS core_audit_time,
        IFNULL(ca.orig_fee, 0) AS core_orig_fee,
        IFNULL(ca.sn, '') AS core_sn,
        a.disburseTime AS disburse_time, a.paidTime AS paid_time,
        a.`status` AS src_status, IFNULL(u.credentialNo, '') AS id2,
        CAST(UNIX_TIMESTAMP(a.created) AS UNSIGNED) * 1000 AS event_time
    FROM {m}.application a
    LEFT JOIN {m}.`user` u ON u.id = a.`userId`
    LEFT JOIN {m}.device d ON d.id = a.`deviceId`
    LEFT JOIN {c}.application ca ON ca.ext_sn = a.applicationNo
    WHERE a.applicationNo = %s
"""
with src.cursor() as cur:
    cur.execute(sql, (APP_NO,))
    raw = cur.fetchone()

if not raw:
    print(json.dumps({"error": "NOT_FOUND"}, ensure_ascii=False))
    sys.exit(0)

bvn_map = mig._fetch_bvn_map_from_source(src, [int(raw["user_id"])])
repay_map = mig._fetch_repay_map(src, [raw["sn"]])

vt = mig.VtTokenResolver(
    src, enabled=True, chunk=2000, vt_db=os.environ.get("VT_TOKEN_DB", "ng_loan_market"),
)
mig._register_app_batch_vt(vt, [raw], bvn_map)
vt.prefetch()
app = (mig._build_application_rows([raw], bvn_map, repay_map, vt=vt) or [None])[0]

if app:
    app = dict(app)
    rp = json.loads(app["repayment_plan"])
    app["repayment_plan"] = rp
    app["principal_match"] = app.get("principal") == rp.get("principal")

print(json.dumps({
    "application_no": APP_NO,
    "core_sn": raw.get("core_sn"),
    "source_amounts": {
        "amount": raw.get("amount"),
        "shouldLoanAmount": raw.get("should_loan_amount"),
        "disburseAmount": raw.get("disburse_amount"),
        "repayment": raw.get("repayment"),
        "core_orig_fee": raw.get("core_orig_fee"),
    },
    "last_paid_time_ms": repay_map.get(raw["sn"], 0) * 1000,
    "application_vt": app if app else "SKIPPED_VT_MISS",
    "vt_summary": vt.summary(),
}, ensure_ascii=False, indent=2, default=str))
src.close()
