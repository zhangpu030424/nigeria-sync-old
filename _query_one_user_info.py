#!/usr/bin/env python3
import json
import os
import sys
import importlib.util
from pathlib import Path

import pymysql

USER_ID = int(sys.argv[1]) if len(sys.argv) > 1 else 9157979
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

m = "ng_loan_market"
with src.cursor() as cur:
    cur.execute(
        f"""
        SELECT u.id AS user_id, u.`appId` AS app_id, u.mobile AS mobile_raw,
            CASE WHEN u.mobile LIKE '+234%%' THEN u.mobile
                 WHEN u.mobile LIKE '234%%' THEN CONCAT('+', u.mobile)
                 WHEN u.mobile LIKE '0%%' THEN CONCAT('+234', SUBSTRING(u.mobile, 2))
                 ELSE CONCAT('+234', u.mobile) END AS mobile,
            ap.name AS app_name,
            CASE WHEN u.`isCancel` IN (1, '1') THEN UNIX_TIMESTAMP(u.updated) * 1000 ELSE 0 END AS closed_time,
            IFNULL(CAST(u.`deviceId` AS CHAR), '') AS reg_device_uuid,
            UNIX_TIMESTAMP(u.created) * 1000 AS reg_time, 0 AS test_flag
        FROM {m}.`user` u
        LEFT JOIN {m}.app ap ON ap.id = u.`appId`
        WHERE u.id = %s
        """,
        (USER_ID,),
    )
    rows_user = list(cur.fetchall())

if not rows_user:
    print(json.dumps({"error": "USER_NOT_FOUND", "user_id": USER_ID}, ensure_ascii=False))
    sys.exit(0)

keys = mig._extract_user_batch_keys(rows_user)
lookups = mig._make_user_lookups(
    mig._select_ud_rows_by_user_ids(src, keys["user_ids"]),
    mig._fetch_lup_by_app_mobile(src, keys["app_mobile_pairs"], 400),
    mig._fetch_uri_by_user_ids(src, keys["user_ids"]),
    mig._fetch_dac_by_device_ids(src, keys["device_ids"]),
    [],
)

vt = mig.VtTokenResolver(
    src, enabled=True, chunk=2000, vt_db=os.environ.get("VT_TOKEN_DB", "ng_loan_market"),
)
mig._register_user_batch_vt(vt, rows_user, lookups)
vt.prefetch()

rows_plain = mig._build_user_info_rows(rows_user, lookups, vt=None)
row_plain = rows_plain[0] if rows_plain else None
if row_plain:
    row_plain = dict(row_plain)
    row_plain["info"] = json.loads(row_plain["info"])

rows_vt = mig._build_user_info_rows(rows_user, lookups, vt=vt)
row_vt = rows_vt[0] if rows_vt else None
if row_vt:
    row_vt = dict(row_vt)
    row_vt["info"] = json.loads(row_vt["info"])

print(json.dumps({
    "user_id": USER_ID,
    "source_user": rows_user[0],
    "lookups_summary": {
        "has_user_data": USER_ID in lookups["ud_by_user"],
        "has_uri": USER_ID in lookups["uri_by_user"],
        "has_lup": mig._lookup_lup(lookups["lup_by_key"], rows_user[0]) is not None,
        "device_id": mig._device_id_from_uuid(rows_user[0].get("reg_device_uuid")),
        "channel": lookups["channel_by_device"].get(mig._device_id_from_uuid(rows_user[0].get("reg_device_uuid"))),
    },
    "user_info_plaintext": row_plain,
    "user_info_vt_production": row_vt,
    "vt_summary": vt.summary(),
}, ensure_ascii=False, indent=2, default=str))
src.close()
