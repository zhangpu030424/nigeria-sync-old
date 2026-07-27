-- ng_loan_market 增量 Lookup 视图（Flink JDBC Lookup 用）
-- 部署: ./scripts/deploy-source-ddl.sh
-- 前置: flink_cdc 账号需 SELECT on ng_loan_market + ng_loan_core

SET SESSION wait_timeout = 28800;

-- ---------- 手机号规范化（与 ng_migration_run 一致）----------
-- 在视图中内联 CASE，勿依赖函数

-- ---------- 辅助：appId+mobile → user_id ----------
CREATE OR REPLACE VIEW users_by_app_mobile_lookup AS
SELECT u.`appId` AS app_id,
       u.mobile AS mobile_raw,
       u.id     AS user_id
FROM `user` u
WHERE u.id IS NOT NULL;

-- ---------- 辅助：deviceId → user_id ----------
CREATE OR REPLACE VIEW users_by_device_lookup AS
SELECT u.`deviceId` AS device_id,
       u.id         AS user_id
FROM `user` u
WHERE u.`deviceId` IS NOT NULL AND u.`deviceId` > 0;

-- ---------- user 增量 Lookup ----------
CREATE OR REPLACE VIEW user_incr_lookup AS
SELECT u.id                                                          AS user_id,
       u.`appId`                                                     AS app_id,
       CASE
           WHEN u.mobile LIKE '+234%' THEN u.mobile
           WHEN u.mobile LIKE '234%' THEN CONCAT('+', u.mobile)
           WHEN u.mobile LIKE '0%' THEN CONCAT('+234', SUBSTRING(u.mobile, 2))
           ELSE CONCAT('+234', u.mobile)
           END                                                       AS mobile_norm,
       vt_m.token                                                    AS mobile_token,
       IFNULL(reg_d.deviceUUID, '')                                  AS reg_device_uuid,
       CASE WHEN u.`isCancel` IN (1, '1') THEN UNIX_TIMESTAMP(u.updated) * 1000 ELSE 0 END AS closed_time,
       UNIX_TIMESTAMP(u.created) * 1000                              AS reg_time,
       0                                                             AS test_flag,
       IFNULL(lup.password, '')                                      AS password,
       dac.channel                                                   AS dac_channel,
       dac.google_ads_campaign_id,
       dac.google_ads_adgroup_id,
       dac.fb_install_referrer_campaign_id,
       dac.fb_install_referrer_campaign_group_id
FROM `user` u
         LEFT JOIN device reg_d ON reg_d.id = u.`deviceId`
         LEFT JOIN vt_token_cache vt_m
                   ON vt_m.vt_type = 'mobile' AND vt_m.status = 1
                       AND vt_m.token IS NOT NULL AND TRIM(vt_m.token) <> ''
                       AND vt_m.raw_value COLLATE utf8mb4_bin = (
                           CASE
                               WHEN u.mobile LIKE '+234%' THEN u.mobile
                               WHEN u.mobile LIKE '234%' THEN CONCAT('+', u.mobile)
                               WHEN u.mobile LIKE '0%' THEN CONCAT('+234', SUBSTRING(u.mobile, 2))
                               ELSE CONCAT('+234', u.mobile)
                               END
                           ) COLLATE utf8mb4_bin
         LEFT JOIN (
    SELECT lup.`appId`, lup.mobile, lup.password
    FROM log_user_password lup
             INNER JOIN (
        SELECT `appId`, mobile, MAX(id) AS max_id
        FROM log_user_password
        GROUP BY `appId`, mobile
    ) t ON lup.`appId` = t.`appId` AND lup.mobile = t.mobile AND lup.id = t.max_id
) lup ON lup.`appId` = u.`appId` AND lup.mobile = u.mobile
         LEFT JOIN (
    SELECT dac.`deviceId`, dac.channel, dac.google_ads_campaign_id, dac.google_ads_adgroup_id,
           dac.fb_install_referrer_campaign_id, dac.fb_install_referrer_campaign_group_id
    FROM device_ad_channel dac
             INNER JOIN (
        SELECT `deviceId`, MAX(id) AS max_id FROM device_ad_channel GROUP BY `deviceId`
    ) t ON dac.`deviceId` = t.`deviceId` AND dac.id = t.max_id
) dac ON dac.`deviceId` = u.`deviceId`;

-- ---------- user_info 增量 bundle Lookup ----------
CREATE OR REPLACE VIEW user_info_incr_bundle_lookup AS
SELECT u.id AS user_id,
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
       u.`appId`                                                     AS app_id,
       UNIX_TIMESTAMP(u.created) * 1000                              AS reg_time,
       ud.bvn                                                        AS bvn_raw,
       ud.email, ud.birthday, ud.gender,
       ud.`addressState`, ud.`addressDistrict`, ud.address,
       ud.company, ud.education, ud.marital, ud.profession, ud.salary,
       ud.`emergencyContact`, ud.`numberOfChildren`, ud.`payCycle`, ud.`salaryDay`
FROM `user` u
         LEFT JOIN app ap ON ap.id = u.`appId`
         LEFT JOIN (
    SELECT ud.*
    FROM user_data ud
             INNER JOIN (SELECT `userId`, MAX(id) AS max_id FROM user_data GROUP BY `userId`) t
                        ON ud.`userId` = t.`userId` AND ud.id = t.max_id
) ud ON ud.`userId` = u.id
         LEFT JOIN vt_token_cache vt_id
                   ON vt_id.vt_type = 'id_number' AND vt_id.status = 1
                       AND vt_id.token IS NOT NULL AND TRIM(vt_id.token) <> ''
                       AND vt_id.raw_value COLLATE utf8mb4_bin = TRIM(ud.bvn) COLLATE utf8mb4_bin
         LEFT JOIN (
    SELECT uri.`userId`, uri.ip
    FROM user_reg_ip uri
             INNER JOIN (SELECT `userId`, MAX(id) AS max_id FROM user_reg_ip GROUP BY `userId`) t
                        ON uri.`userId` = t.`userId` AND uri.id = t.max_id
) uri ON uri.`userId` = u.id
         LEFT JOIN (
    SELECT lup.`appId`, lup.mobile, lup.password
    FROM log_user_password lup
             INNER JOIN (
        SELECT `appId`, mobile, MAX(id) AS max_id FROM log_user_password GROUP BY `appId`, mobile
    ) t ON lup.`appId` = t.`appId` AND lup.mobile = t.mobile AND lup.id = t.max_id
) lup ON lup.`appId` = u.`appId` AND lup.mobile = u.mobile
         LEFT JOIN (
    SELECT dac.`deviceId`, dac.channel
    FROM device_ad_channel dac
             INNER JOIN (SELECT `deviceId`, MAX(id) AS max_id FROM device_ad_channel GROUP BY `deviceId`) t
                        ON dac.`deviceId` = t.`deviceId` AND dac.id = t.max_id
) dac ON dac.`deviceId` = u.`deviceId`;

-- ---------- user_bankcard 增量 Lookup ----------
CREATE OR REPLACE VIEW user_bankcard_incr_lookup AS
SELECT ud.`userId`                                              AS user_id,
       IFNULL(ud.bankCode, '')                                  AS bank_code,
       TRIM(ud.bankAccount)                                     AS bank_account_raw,
       vt_b.token                                               AS bank_account_token,
       1                                                        AS is_default
FROM user_data ud
         INNER JOIN (
    SELECT `userId`, MAX(id) AS max_id FROM user_data GROUP BY `userId`
) pick ON ud.`userId` = pick.`userId` AND ud.id = pick.max_id
         LEFT JOIN vt_token_cache vt_b
                   ON vt_b.vt_type = 'bank_account' AND vt_b.status = 1
                       AND vt_b.token IS NOT NULL AND TRIM(vt_b.token) <> ''
                       AND vt_b.raw_value COLLATE utf8mb4_bin = TRIM(ud.bankAccount) COLLATE utf8mb4_bin
WHERE ud.bankAccount IS NOT NULL AND TRIM(ud.bankAccount) <> '';

-- ---------- user_product 增量 Lookup（每 user+product 最新申请金额）----------
CREATE OR REPLACE VIEW user_product_incr_lookup AS
SELECT pick.`userId`   AS user_id,
       pick.`productId` AS product_id,
       a.amount        AS credit_amount
FROM (
         SELECT `userId`, `productId`, MAX(id) AS max_id
         FROM application
         WHERE `productId` IS NOT NULL AND `productId` <> 0
         GROUP BY `userId`, `productId`
     ) pick
         INNER JOIN application a ON a.id = pick.max_id;

-- ---------- application 增量 bundle Lookup（market + core + bvn + repay）----------
CREATE OR REPLACE VIEW application_incr_bundle_lookup AS
SELECT a.id                                                          AS app_row_id,
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
       a.`appId`                                                     AS app_id,
       '1.0.0'                                                       AS app_version,
       a.`userId`                                                    AS user_id,
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
       IFNULL(ca.apply_time, 0)                                      AS core_apply_time,
       IFNULL(ca.audit_time, 0)                                      AS core_audit_time,
       IFNULL(ca.orig_fee, 0)                                        AS core_orig_fee,
       a.disburseTime                                                AS disburse_time,
       a.paidTime                                                    AS paid_time,
       a.`status`                                                    AS src_status,
       IFNULL(u.credentialNo, '')                                    AS id2_raw,
       vt_id.token                                                   AS id_number_token,
       vt_i2.token                                                   AS id2_token,
       IFNULL(rr.last_repay_time, 0)                                 AS last_repay_time,
       CAST(UNIX_TIMESTAMP(a.created) AS UNSIGNED) * 1000            AS event_time
FROM application a
         LEFT JOIN `user` u ON u.id = a.`userId`
         LEFT JOIN ng_loan_core.application ca ON ca.ext_sn = a.applicationNo
         LEFT JOIN (
    SELECT ud.`userId`, ud.bvn
    FROM user_data ud
             INNER JOIN (SELECT `userId`, MAX(id) AS max_id FROM user_data GROUP BY `userId`) t
                        ON ud.`userId` = t.`userId` AND ud.id = t.max_id
) ud ON ud.`userId` = a.`userId`
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
         LEFT JOIN (
    SELECT ca2.ext_sn, MAX(rr.repay_time) AS last_repay_time
    FROM ng_loan_core.application ca2
             INNER JOIN ng_loan_core.repay_record rr ON rr.sn = ca2.sn
    GROUP BY ca2.ext_sn
) rr ON rr.ext_sn = a.applicationNo
WHERE a.applicationNo IS NOT NULL AND TRIM(a.applicationNo) <> ''
  AND ca.sn IS NOT NULL AND TRIM(ca.sn) <> '';

-- ---------- 辅助：applicationNo → app_row_id ----------
CREATE OR REPLACE VIEW market_app_id_by_no_lookup AS
SELECT applicationNo, id AS app_row_id
FROM application
WHERE applicationNo IS NOT NULL AND TRIM(applicationNo) <> '';

-- ---------- 辅助：userId → app_row_id（user_data 变更触发）----------
CREATE OR REPLACE VIEW market_app_ids_by_user_lookup AS
SELECT id AS app_row_id, `userId` AS user_id
FROM application
WHERE `userId` IS NOT NULL;

-- ---------- 辅助：market app id → core sn ----------
CREATE OR REPLACE VIEW market_app_core_sn_lookup AS
SELECT ma.id,
       ca.sn AS core_sn
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
         INNER JOIN (
    SELECT sn, MAX(plan_sn) AS max_plan_sn
    FROM ng_loan_core.repay_plan
    GROUP BY sn
) pick ON rp.sn = pick.sn AND rp.plan_sn = pick.max_plan_sn
         INNER JOIN ng_loan_core.application ca ON ca.sn = rp.sn
         INNER JOIN application ma ON ma.applicationNo = ca.ext_sn
WHERE ma.disburseTime > 0;
