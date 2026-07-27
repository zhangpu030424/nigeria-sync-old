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
SELECT CAST(u.id AS SIGNED)                                      AS id,
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
-- 勿 JOIN vt_token_cache；id_number_token 由 Flink vt_tokenize(bvn) 兜底
CREATE OR REPLACE VIEW user_info_incr_bundle_lookup AS
SELECT CAST(u.id AS SIGNED)                                          AS user_id,
       CAST(TRIM(CONCAT(
               IFNULL(ud.`firstName`, ''), ' ',
               IFNULL(ud.`middleName`, ''), ' ',
               IFNULL(ud.`lastName`, '')
           )) AS CHAR)                                               AS full_name,
       CAST(NULL AS CHAR)                                            AS id_number_token,
       CAST(IFNULL(lup.password, '') AS CHAR)                        AS password,
       CAST(uri.ip AS CHAR)                                          AS registration_ip,
       CAST(dac.channel AS CHAR)                                     AS install_channel,
       CAST(ap.name AS CHAR)                                         AS app_name,
       CAST(u.`appId` AS SIGNED)                                     AS app_id,
       CAST(UNIX_TIMESTAMP(u.created) * 1000 AS SIGNED)              AS reg_time,
       CAST(ud.bvn AS CHAR)                                          AS bvn_raw,
       CAST(ud.email AS CHAR) AS email,
       CAST(ud.birthday AS CHAR) AS birthday,
       CAST(ud.gender AS SIGNED) AS gender,
       CAST(ud.`addressState` AS CHAR) AS addressState,
       CAST(ud.`addressDistrict` AS CHAR) AS addressDistrict,
       CAST(ud.address AS CHAR) AS address,
       CAST(ud.company AS CHAR) AS company,
       CAST(ud.education AS SIGNED) AS education,
       CAST(ud.marital AS SIGNED) AS marital,
       CAST(ud.profession AS CHAR) AS profession,
       CAST(ud.salary AS CHAR) AS salary,
       CAST(ud.`emergencyContact` AS CHAR) AS emergencyContact,
       CAST(ud.`numberOfChildren` AS SIGNED) AS numberOfChildren,
       CAST(ud.`payCycle` AS SIGNED) AS payCycle,
       CAST(ud.`salaryDay` AS SIGNED) AS salaryDay
FROM `user` u
         LEFT JOIN app ap ON ap.id = u.`appId`
         LEFT JOIN user_data ud
                   ON ud.id = (
                       SELECT ud2.id FROM user_data ud2
                       WHERE ud2.`userId` = u.id
                       ORDER BY ud2.id DESC LIMIT 1
                   )
         LEFT JOIN user_reg_ip uri
                   ON uri.id = (
                       SELECT uri2.id FROM user_reg_ip uri2
                       WHERE uri2.`userId` = u.id
                       ORDER BY uri2.id DESC LIMIT 1
                   )
         LEFT JOIN log_user_password lup
                   ON lup.id = (
                       SELECT l2.id FROM log_user_password l2
                       WHERE l2.`appId` = u.`appId` AND l2.mobile = u.mobile
                       ORDER BY l2.id DESC LIMIT 1
                   )
         LEFT JOIN device_ad_channel dac
                   ON dac.id = (
                       SELECT d2.id FROM device_ad_channel d2
                       WHERE d2.`deviceId` = u.`deviceId`
                       ORDER BY d2.id DESC LIMIT 1
                   );

-- ---------- user_bankcard 增量 Lookup（Flink: WHERE user_id = ?）----------
-- 勿 JOIN vt_token_cache（慢）；token 由 Flink UDF / 明文兜底
CREATE OR REPLACE VIEW user_bankcard_incr_lookup AS
SELECT CAST(ud.`userId` AS SIGNED)                                   AS user_id,
       CAST(IFNULL(ud.bankCode, '') AS CHAR)                         AS bank_code,
       CAST(TRIM(ud.bankAccount) AS CHAR)                            AS bank_account_raw,
       CAST(NULL AS CHAR)                                            AS bank_account_token,
       CAST(1 AS SIGNED)                                             AS is_default
FROM user_data ud
WHERE ud.bankAccount IS NOT NULL AND TRIM(ud.bankAccount) <> ''
  AND ud.id = (
      SELECT ud2.id
      FROM user_data ud2
      WHERE ud2.`userId` = ud.`userId`
      ORDER BY ud2.id DESC
      LIMIT 1
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
-- 勿 JOIN vt_token_cache（5 路 VT 会导致秒级点查）；token 由 Flink vt_tokenize 兜底
CREATE OR REPLACE VIEW application_incr_bundle_lookup AS
SELECT CAST(a.id AS SIGNED)                                          AS app_row_id,
       CAST(a.applicationNo AS CHAR)                                 AS market_no,
       CONCAT('ng', LPAD(CAST(a.`appId` AS CHAR), 4, '0'), '-', a.applicationNo) AS application_no,
       CASE
           WHEN a.mobile LIKE '+234%' THEN a.mobile
           WHEN a.mobile LIKE '234%' THEN CONCAT('+', a.mobile)
           WHEN a.mobile LIKE '0%' THEN CONCAT('+234', SUBSTRING(a.mobile, 2))
           ELSE CONCAT('+234', a.mobile)
           END                                                       AS mobile_norm,
       CAST(NULL AS CHAR)                                            AS mobile_token,
       'ng01'                                                        AS bid,
       CAST(a.`appId` AS SIGNED)                                     AS app_id,
       '1.0.0'                                                       AS app_version,
       CAST(a.`userId` AS SIGNED)                                    AS user_id,
       CAST(a.applicationNo AS CHAR)                                 AS sn,
       CAST(CASE WHEN a.`repeatLoan` = 0 THEN 1 ELSE 0 END AS SIGNED) AS is_first_apply,
       CAST(IFNULL(NULLIF(a.gaid, ''), NULL) AS CHAR)                AS gaid_raw,
       CAST(NULL AS CHAR)                                            AS gaid_token,
       CAST(IFNULL(CAST(a.`deviceDataId` AS CHAR), '') AS CHAR)      AS device_uuid,
       CAST(IFNULL(a.bankCode, '') AS CHAR)                          AS bank_code,
       CAST(IFNULL(a.bankAccount, '') AS CHAR)                       AS bank_account_raw,
       CAST(NULL AS CHAR)                                            AS bank_account_token,
       CAST(a.`productId` AS CHAR)                                   AS product_id,
       CAST(a.term AS SIGNED)                                        AS term,
       CAST(IFNULL(a.shouldLoanAmount, 0) AS SIGNED)                 AS should_loan_amount,
       CAST(IFNULL(a.amount, 0) AS SIGNED)                           AS amount,
       CAST(IFNULL(a.repayment, 0) AS SIGNED)                        AS repayment,
       CAST(IFNULL(a.disburseAmount, 0) AS SIGNED)                   AS disburse_amount,
       CAST(IFNULL(a.applyDate, 0) AS SIGNED)                        AS apply_date,
       CAST(IFNULL(a.dueDate, 0) AS SIGNED)                          AS due_date,
       CAST(ca.sn AS CHAR)                                           AS core_sn,
       CAST(IFNULL(ca.apply_time, 0) AS SIGNED)                      AS core_apply_time,
       CAST(IFNULL(ca.audit_time, 0) AS SIGNED)                      AS core_audit_time,
       CAST(IFNULL(ca.orig_fee, 0) AS SIGNED)                        AS core_orig_fee,
       CAST(IFNULL(a.disburseTime, 0) AS SIGNED)                     AS disburse_time,
       CAST(IFNULL(a.paidTime, 0) AS SIGNED)                         AS paid_time,
       CAST(IFNULL(a.`status`, 0) AS SIGNED)                         AS src_status,
       CAST(IFNULL(u.credentialNo, '') AS CHAR)                      AS id2_raw,
       CAST(NULL AS CHAR)                                            AS id_number_token,
       CAST(NULL AS CHAR)                                            AS id2_token,
       CAST(IFNULL((
           SELECT MAX(rr.repay_time)
           FROM ng_loan_core.repay_record rr
           WHERE rr.sn = ca.sn
       ), 0) AS SIGNED)                                              AS last_repay_time,
       CAST(UNIX_TIMESTAMP(a.created) AS SIGNED) * 1000              AS event_time
FROM application a
         LEFT JOIN `user` u ON u.id = a.`userId`
         LEFT JOIN ng_loan_core.application ca ON ca.ext_sn = a.applicationNo
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

-- ---------- loan 增量 bundle Lookup（Flink: WHERE sn = ?）----------
-- 数值一律 CAST SIGNED，避免 JDBC BigInteger/Long 与 Flink INT 冲突
CREATE OR REPLACE VIEW loan_incr_bundle_lookup AS
SELECT CAST(rp.sn AS CHAR)                                                         AS sn,
       CAST(rp.plan_sn AS SIGNED)                                                  AS plan_sn,
       CONCAT('ng', LPAD(CAST(ma.`appId` AS CHAR), 4, '0'), '-', ma.applicationNo) AS application_no,
       CONCAT('ng-', rp.sn, '-', LPAD(1, 2, '0'), LPAD(0, 3, '0'))               AS loan_no,
       CAST(1 AS SIGNED)                                                           AS period,
       CAST(0 AS SIGNED)                                                           AS roll_sequence,
       CAST(rp.start_date AS SIGNED)                                               AS start_date,
       CAST(rp.due_date AS SIGNED)                                                 AS due_date,
       CAST(IFNULL(rp.prin_amt, 0) AS SIGNED)                                      AS prin_amt,
       CAST(IFNULL(rp.interest, 0) AS SIGNED)                                      AS interest,
       CAST(IFNULL(rp.orig_fee, 0) AS SIGNED)                                      AS orig_fee,
       CAST(IFNULL(rp.penalty, 0) AS SIGNED)                                       AS penalty,
       CAST(IFNULL(rp.amt, 0) AS SIGNED)                                           AS amt,
       CAST(IFNULL(rp.`status`, 0) AS SIGNED)                                      AS rp_status,
       CAST(IFNULL(rp.repaid_amt, 0) AS SIGNED)                                    AS repaid_amt,
       CAST(IFNULL(rp.repay_last_time, 0) AS SIGNED)                               AS repay_last_time,
       CAST(IFNULL(rp.settle_time, 0) AS SIGNED)                                   AS settle_time,
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
