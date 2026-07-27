-- ng_loan_market 增量 Lookup 视图（Flink JDBC Lookup 点查优化版）
-- 原则:
--   1. 数值列 CAST AS SIGNED，避免 JDBC BigInteger → Flink BIGINT ClassCast
--   2. 禁止视图内全表 GROUP BY 物化；Latest 行用相关子查询 MAX(id)（WHERE user_id=? 时可走索引）
-- 部署: ./scripts/deploy-source-ddl.sh --force

SET SESSION wait_timeout = 28800;

-- ---------- 辅助：appId+mobile → user_id（lup CDC 触发解析 user_id）----------
CREATE OR REPLACE VIEW users_by_app_mobile_lookup AS
SELECT u.`appId`                   AS app_id,
       u.mobile                    AS mobile_raw,
       CAST(u.id AS SIGNED)        AS user_id
FROM `user` u
WHERE u.id IS NOT NULL;

-- ---------- 辅助：deviceId → user_id（dac CDC 触发）----------
CREATE OR REPLACE VIEW users_by_device_lookup AS
SELECT CAST(u.`deviceId` AS SIGNED) AS device_id,
       CAST(u.id AS SIGNED)          AS user_id
FROM `user` u
WHERE u.`deviceId` IS NOT NULL AND u.`deviceId` > 0;

-- ---------- user 增量 Lookup（Flink: WHERE id = ?；对齐 nigeria-flink-sync 裸 id + CHAR）----------
CREATE OR REPLACE VIEW user_incr_lookup AS
SELECT u.id_signed                                                   AS id,
       CAST(u.`appId` AS SIGNED)                                     AS app_id,
       CASE
           WHEN u.mobile LIKE '+234%' THEN u.mobile
           WHEN u.mobile LIKE '234%' THEN CONCAT('+', u.mobile)
           WHEN u.mobile LIKE '0%' THEN CONCAT('+234', SUBSTRING(u.mobile, 2))
           ELSE CONCAT('+234', u.mobile)
           END                                                       AS mobile_norm,
       CAST(NULL AS CHAR)                                            AS mobile_token,
       CAST(IFNULL(reg_d.deviceUUID, '') AS CHAR)                    AS reg_device_uuid,
       CAST(CASE
                WHEN u.`isCancel` IN (1, '1') THEN UNIX_TIMESTAMP(u.updated) * 1000
                ELSE 0
            END AS SIGNED)                                           AS closed_time,
       CAST(UNIX_TIMESTAMP(u.created) * 1000 AS SIGNED)              AS reg_time,
       CAST(0 AS SIGNED)                                             AS test_flag,
       CAST(IFNULL(lup.password, '') AS CHAR)                         AS password,
       CAST(dac.channel AS CHAR)                                      AS dac_channel,
       CAST(dac.google_ads_campaign_id AS CHAR)                      AS google_ads_campaign_id,
       CAST(dac.google_ads_adgroup_id AS CHAR)                       AS google_ads_adgroup_id,
       CAST(dac.fb_install_referrer_campaign_id AS CHAR)            AS fb_install_referrer_campaign_id,
       CAST(dac.fb_install_referrer_campaign_group_id AS CHAR)       AS fb_install_referrer_campaign_group_id
FROM `user` u
         LEFT JOIN device reg_d ON reg_d.id = u.`deviceId`
         LEFT JOIN log_user_password lup
                   ON lup.id = (
                       SELECT CAST(l2.id AS SIGNED)
                       FROM log_user_password l2
                       WHERE l2.`appId` = u.`appId` AND l2.mobile = u.mobile
                       ORDER BY l2.id DESC
                       LIMIT 1
                   )
         LEFT JOIN device_ad_channel dac
                   ON dac.id = (
                       SELECT CAST(d2.id AS SIGNED)
                       FROM device_ad_channel d2
                       WHERE d2.`deviceId` = u.`deviceId`
                       ORDER BY d2.id DESC
                       LIMIT 1
                   );

-- ---------- user_info 增量 bundle Lookup（Flink: WHERE user_id = ?）----------
CREATE OR REPLACE VIEW user_info_incr_bundle_lookup AS
SELECT CAST(u.id AS SIGNED)                                          AS user_id,
       TRIM(CONCAT(
               IFNULL(ud.`firstName`, ''), ' ',
               IFNULL(ud.`middleName`, ''), ' ',
               IFNULL(ud.`lastName`, '')
           ))                                                        AS full_name,
       vt_id.token                                                   AS id_number_token,
       IFNULL(lup.password, '')                                      AS password,
       uri.ip                                                        AS registration_ip,
       dac.channel                                                   AS install_channel,
       ap.name                                                       AS app_name,
       CAST(u.`appId` AS SIGNED)                                     AS app_id,
       CAST(UNIX_TIMESTAMP(u.created) * 1000 AS SIGNED)              AS reg_time,
       ud.bvn                                                        AS bvn_raw,
       ud.email, ud.birthday, ud.gender,
       ud.`addressState`, ud.`addressDistrict`, ud.address,
       ud.company, ud.education, ud.marital, ud.profession, ud.salary,
       ud.`emergencyContact`, ud.`numberOfChildren`, ud.`payCycle`, ud.`salaryDay`
FROM `user` u
         LEFT JOIN app ap ON ap.id = u.`appId`
         LEFT JOIN user_data ud
                   ON ud.`userId` = u.id
                       AND ud.id = (
                           SELECT CAST(MAX(ud2.id) AS SIGNED)
                           FROM user_data ud2
                           WHERE ud2.`userId` = u.id
                       )
         LEFT JOIN vt_token_cache vt_id
                   ON vt_id.vt_type = 'id_number' AND vt_id.status = 1
                       AND vt_id.token IS NOT NULL AND TRIM(vt_id.token) <> ''
                       AND vt_id.raw_value COLLATE utf8mb4_bin = TRIM(ud.bvn) COLLATE utf8mb4_bin
         LEFT JOIN user_reg_ip uri
                   ON uri.`userId` = u.id
                       AND uri.id = (
                           SELECT CAST(MAX(uri2.id) AS SIGNED)
                           FROM user_reg_ip uri2
                           WHERE uri2.`userId` = u.id
                       )
         LEFT JOIN log_user_password lup
                   ON lup.`appId` = u.`appId` AND lup.mobile = u.mobile
                       AND lup.id = (
                           SELECT CAST(MAX(l2.id) AS SIGNED)
                           FROM log_user_password l2
                           WHERE l2.`appId` = u.`appId` AND l2.mobile = u.mobile
                       )
         LEFT JOIN device_ad_channel dac
                   ON dac.`deviceId` = u.`deviceId`
                       AND dac.id = (
                           SELECT CAST(MAX(d2.id) AS SIGNED)
                           FROM device_ad_channel d2
                           WHERE d2.`deviceId` = u.`deviceId`
                       );

-- ---------- user_bankcard 增量 Lookup（Flink: WHERE user_id = ?）----------
CREATE OR REPLACE VIEW user_bankcard_incr_lookup AS
SELECT CAST(ud.`userId` AS SIGNED)                                   AS user_id,
       IFNULL(ud.bankCode, '')                                       AS bank_code,
       TRIM(ud.bankAccount)                                          AS bank_account_raw,
       vt_b.token                                                    AS bank_account_token,
       1                                                             AS is_default
FROM user_data ud
         LEFT JOIN vt_token_cache vt_b
                   ON vt_b.vt_type = 'bank_account' AND vt_b.status = 1
                       AND vt_b.token IS NOT NULL AND TRIM(vt_b.token) <> ''
                       AND vt_b.raw_value COLLATE utf8mb4_bin = TRIM(ud.bankAccount) COLLATE utf8mb4_bin
WHERE ud.bankAccount IS NOT NULL AND TRIM(ud.bankAccount) <> ''
  AND ud.id = (
      SELECT CAST(MAX(ud2.id) AS SIGNED) FROM user_data ud2 WHERE ud2.`userId` = ud.`userId`
  );

-- ---------- user_product 增量 Lookup（Flink: WHERE user_id = ? AND product_id = ?）----------
CREATE OR REPLACE VIEW user_product_incr_lookup AS
SELECT CAST(a.`userId` AS SIGNED)                                    AS user_id,
       CAST(a.`productId` AS SIGNED)                                 AS product_id,
       CAST(a.amount AS SIGNED)                                      AS credit_amount
FROM application a
WHERE a.`productId` IS NOT NULL AND a.`productId` <> 0
  AND a.id = (
      SELECT CAST(MAX(a2.id) AS SIGNED)
      FROM application a2
      WHERE a2.`userId` = a.`userId` AND a2.`productId` = a.`productId`
  );

-- ---------- application 增量 bundle Lookup（Flink: WHERE app_row_id = ?）----------
CREATE OR REPLACE VIEW application_incr_bundle_lookup AS
SELECT CAST(a.id AS SIGNED)                                          AS app_row_id,
       a.applicationNo                                               AS market_no,
       CONCAT('ng', LPAD(CAST(a.`appId` AS CHAR), 4, '0'), '-', a.applicationNo) AS application_no,
       CASE
           WHEN a.mobile LIKE '+234%' THEN a.mobile
           WHEN a.mobile LIKE '234%' THEN CONCAT('+', a.mobile)
           WHEN a.mobile LIKE '0%' THEN CONCAT('+234', SUBSTRING(a.mobile, 2))
           ELSE CONCAT('+234', a.mobile)
           END                                                       AS mobile_norm,
       vt_m.token                                                    AS mobile_token,
       'ng01'                                                        AS bid,
       CAST(a.`appId` AS SIGNED)                                     AS app_id,
       '1.0.0'                                                       AS app_version,
       CAST(a.`userId` AS SIGNED)                                    AS user_id,
       a.applicationNo                                               AS sn,
       CASE WHEN a.`repeatLoan` = 0 THEN 1 ELSE 0 END                AS is_first_apply,
       IFNULL(NULLIF(a.gaid, ''), NULL)                              AS gaid_raw,
       vt_g.token                                                    AS gaid_token,
       IFNULL(CAST(a.`deviceDataId` AS CHAR), '')                    AS device_uuid,
       IFNULL(a.bankCode, '')                                        AS bank_code,
       IFNULL(a.bankAccount, '')                                     AS bank_account_raw,
       vt_b.token                                                    AS bank_account_token,
       CAST(a.`productId` AS CHAR)                                   AS product_id,
       a.term,
       a.shouldLoanAmount                                            AS should_loan_amount,
       a.amount,
       a.repayment,
       a.disburseAmount                                              AS disburse_amount,
       a.applyDate                                                   AS apply_date,
       a.dueDate                                                     AS due_date,
       ca.sn                                                         AS core_sn,
       CAST(IFNULL(ca.apply_time, 0) AS SIGNED)                      AS core_apply_time,
       CAST(IFNULL(ca.audit_time, 0) AS SIGNED)                      AS core_audit_time,
       CAST(IFNULL(ca.orig_fee, 0) AS SIGNED)                        AS core_orig_fee,
       a.disburseTime                                                AS disburse_time,
       a.paidTime                                                    AS paid_time,
       a.`status`                                                    AS src_status,
       IFNULL(u.credentialNo, '')                                    AS id2_raw,
       vt_id.token                                                   AS id_number_token,
       vt_i2.token                                                   AS id2_token,
       CAST(IFNULL((
           SELECT MAX(rr.repay_time)
           FROM ng_loan_core.application ca2
                    INNER JOIN ng_loan_core.repay_record rr ON rr.sn = ca2.sn
           WHERE ca2.ext_sn = a.applicationNo
       ), 0) AS SIGNED)                                              AS last_repay_time,
       CAST(UNIX_TIMESTAMP(a.created) AS SIGNED) * 1000              AS event_time
FROM application a
         LEFT JOIN `user` u ON u.id = a.`userId`
         LEFT JOIN ng_loan_core.application ca ON ca.ext_sn = a.applicationNo
         LEFT JOIN user_data ud
                   ON ud.`userId` = a.`userId`
                       AND ud.id = (
                           SELECT CAST(MAX(ud2.id) AS SIGNED)
                           FROM user_data ud2
                           WHERE ud2.`userId` = a.`userId`
                       )
         LEFT JOIN vt_token_cache vt_m
                   ON vt_m.vt_type = 'mobile' AND vt_m.status = 1
                       AND vt_m.raw_value COLLATE utf8mb4_bin = (
                           CASE
                               WHEN a.mobile LIKE '+234%' THEN a.mobile
                               WHEN a.mobile LIKE '234%' THEN CONCAT('+', a.mobile)
                               WHEN a.mobile LIKE '0%' THEN CONCAT('+234', SUBSTRING(a.mobile, 2))
                               ELSE CONCAT('+234', a.mobile)
                               END
                           ) COLLATE utf8mb4_bin
         LEFT JOIN vt_token_cache vt_id
                   ON vt_id.vt_type = 'id_number' AND vt_id.status = 1
                       AND vt_id.raw_value COLLATE utf8mb4_bin = TRIM(ud.bvn) COLLATE utf8mb4_bin
         LEFT JOIN vt_token_cache vt_b
                   ON vt_b.vt_type = 'bank_account' AND vt_b.status = 1
                       AND vt_b.raw_value COLLATE utf8mb4_bin = TRIM(a.bankAccount) COLLATE utf8mb4_bin
         LEFT JOIN vt_token_cache vt_g
                   ON vt_g.vt_type = 'gaid_idfa' AND vt_g.status = 1
                       AND vt_g.raw_value COLLATE utf8mb4_bin = TRIM(a.gaid) COLLATE utf8mb4_bin
         LEFT JOIN vt_token_cache vt_i2
                   ON vt_i2.vt_type = 'id2' AND vt_i2.status = 1
                       AND vt_i2.raw_value COLLATE utf8mb4_bin = TRIM(u.credentialNo) COLLATE utf8mb4_bin
WHERE a.applicationNo IS NOT NULL AND TRIM(a.applicationNo) <> ''
  AND ca.sn IS NOT NULL AND TRIM(ca.sn) <> '';

-- ---------- 辅助：applicationNo → app_row_id ----------
CREATE OR REPLACE VIEW market_app_id_by_no_lookup AS
SELECT applicationNo,
       CAST(id AS SIGNED) AS app_row_id
FROM application
WHERE applicationNo IS NOT NULL AND TRIM(applicationNo) <> '';

-- ---------- 辅助：userId → app_row_id（user_data 变更触发）----------
CREATE OR REPLACE VIEW market_app_ids_by_user_lookup AS
SELECT CAST(id AS SIGNED)      AS app_row_id,
       CAST(`userId` AS SIGNED) AS user_id
FROM application
WHERE `userId` IS NOT NULL;

-- ---------- 辅助：market app id → core sn ----------
CREATE OR REPLACE VIEW market_app_core_sn_lookup AS
SELECT CAST(ma.id AS SIGNED) AS id,
       ca.sn                 AS core_sn
FROM application ma
         INNER JOIN ng_loan_core.application ca ON ca.ext_sn = ma.applicationNo
WHERE ma.disburseTime > 0;

-- ---------- loan 增量 bundle Lookup ----------
CREATE OR REPLACE VIEW loan_incr_bundle_lookup AS
SELECT rp.sn,
       rp.plan_sn,
       CONCAT('ng', LPAD(CAST(ma.`appId` AS CHAR), 4, '0'), '-', ma.applicationNo) AS application_no,
       CONCAT('ng-', rp.sn, '-', LPAD(1, 2, '0'), LPAD(0, 3, '0'))               AS loan_no,
       1                                                                           AS period,
       0                                                                           AS roll_sequence,
       rp.start_date,
       rp.due_date,
       rp.prin_amt,
       rp.interest,
       rp.orig_fee,
       rp.penalty,
       rp.amt,
       rp.`status`                                                                 AS rp_status,
       IFNULL(rp.repaid_amt, 0)                                                     AS repaid_amt,
       rp.repay_last_time,
       rp.settle_time,
       rp.created_at
FROM ng_loan_core.repay_plan rp
         INNER JOIN ng_loan_core.application ca ON ca.sn = rp.sn
         INNER JOIN application ma ON ma.applicationNo = ca.ext_sn
WHERE ma.disburseTime > 0
  AND rp.plan_sn = (
      SELECT MAX(rp2.plan_sn)
      FROM ng_loan_core.repay_plan rp2
      WHERE rp2.sn = rp.sn
  );
