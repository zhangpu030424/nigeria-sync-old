-- 增量 application：market + core CDC 触发 + bundle Lookup
CREATE TEMPORARY FUNCTION vt_tokenize AS 'com.nigeria.flink.udf.VtTokenizeFunction';

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '200ms';
SET 'table.exec.mini-batch.size' = '${FLINK_MINI_BATCH_SIZE}';
SET 'execution.checkpointing.interval' = '${FLINK_CHECKPOINT_INTERVAL}';
SET 'execution.checkpointing.timeout' = '${FLINK_CHECKPOINT_TIMEOUT}';
SET 'execution.checkpointing.min-pause' = '120s';
SET 'execution.checkpointing.tolerable-failed-checkpoints' = '10';
SET 'execution.checkpointing.unaligned' = 'true';
SET 'table.exec.state.ttl' = '2 h';
SET 'table.exec.async-lookup.buffer-capacity' = '200';
SET 'table.exec.async-lookup.timeout' = '60s';

CREATE TABLE IF NOT EXISTS cdc_market_app (
    id BIGINT,
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${MARKET_MYSQL_HOST}',
    'port' = '${MARKET_MYSQL_PORT}',
    'username' = '${MARKET_MYSQL_USER}',
    'password' = '${MARKET_MYSQL_PASSWORD}',
    'database-name' = '${MARKET_MYSQL_DATABASE}',
    'table-name' = 'application',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_APP_MARKET}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only'
);

CREATE TABLE IF NOT EXISTS cdc_core_app (
    id BIGINT,
    ext_sn STRING,
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${CORE_MYSQL_HOST}',
    'port' = '${CORE_MYSQL_PORT}',
    'username' = '${CORE_MYSQL_USER}',
    'password' = '${CORE_MYSQL_PASSWORD}',
    'database-name' = '${CORE_MYSQL_DATABASE}',
    'table-name' = 'application',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_APP_CORE}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only'
);

CREATE TABLE IF NOT EXISTS cdc_user_data (
    id BIGINT,
    `userId` BIGINT,
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${MARKET_MYSQL_HOST}',
    'port' = '${MARKET_MYSQL_PORT}',
    'username' = '${MARKET_MYSQL_USER}',
    'password' = '${MARKET_MYSQL_PASSWORD}',
    'database-name' = '${MARKET_MYSQL_DATABASE}',
    'table-name' = 'user_data',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_APP_USER_DATA}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only'
);

CREATE TABLE IF NOT EXISTS dim_app_bundle (
    app_row_id BIGINT,
    market_no STRING,
    application_no STRING,
    mobile_norm STRING,
    mobile_token STRING,
    bid STRING,
    app_id BIGINT,
    app_version STRING,
    user_id BIGINT,
    sn STRING,
    is_first_apply BIGINT,
    gaid_raw STRING,
    gaid_token STRING,
    device_uuid STRING,
    bank_code STRING,
    bank_account_raw STRING,
    bank_account_token STRING,
    product_id STRING,
    term BIGINT,
    amount BIGINT,
    repayment BIGINT,
    disburse_amount BIGINT,
    apply_date BIGINT,
    due_date BIGINT,
    core_sn STRING,
    core_apply_time BIGINT,
    core_audit_time BIGINT,
    core_orig_fee BIGINT,
    disburse_time BIGINT,
    paid_time BIGINT,
    src_status BIGINT,
    id_number_token STRING,
    bvn_raw STRING,
    last_repay_time BIGINT,
    PRIMARY KEY (app_row_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MARKET_MYSQL_HOST}:${MARKET_MYSQL_PORT}/${MARKET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'application_incr_bundle_lookup',
    'username' = '${MARKET_MYSQL_USER}',
    'password' = '${MARKET_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TABLE IF NOT EXISTS dim_market_app_by_no (
    applicationNo STRING,
    app_row_id BIGINT,
    PRIMARY KEY (applicationNo) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MARKET_MYSQL_HOST}:${MARKET_MYSQL_PORT}/${MARKET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'market_app_id_by_no_lookup',
    'username' = '${MARKET_MYSQL_USER}',
    'password' = '${MARKET_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TABLE IF NOT EXISTS dim_apps_by_user (
    user_id BIGINT,
    app_row_id BIGINT,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MARKET_MYSQL_HOST}:${MARKET_MYSQL_PORT}/${MARKET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'market_app_ids_by_user_lookup',
    'username' = '${MARKET_MYSQL_USER}',
    'password' = '${MARKET_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TABLE IF NOT EXISTS sink_application (
    application_no STRING,
    mobile STRING,
    coupon_code STRING,
    bid STRING,
    app_id BIGINT,
    app_version STRING,
    user_id BIGINT,
    group_user_id BIGINT,
    sn STRING,
    is_test INT,
    is_first_apply INT,
    is_auto_apply INT,
    id_number STRING,
    gaid_idfa STRING,
    device_uuid STRING,
    session_id STRING,
    bank_code STRING,
    bank_account_name STRING,
    bank_account_number STRING,
    product_id STRING,
    product_scheme_id STRING,
    product_calculator_version INT,
    repay_calculator_version INT,
    rollover_calculator_version INT,
    product_scheme_param STRING,
    term INT,
    periods INT,
    repayment_method INT,
    repayment_plan STRING,
    credit_limit BIGINT,
    loan_amount BIGINT,
    principal BIGINT,
    total_amount BIGINT,
    disbursed_amount BIGINT,
    created_time BIGINT,
    submited_time BIGINT,
    reviewed_time BIGINT,
    disbursed_time BIGINT,
    last_paid_time BIGINT,
    paid_off_time BIGINT,
    lock_expire_time BIGINT,
    status INT,
    PRIMARY KEY (mobile, group_user_id, sn) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?${TARGET_JDBC_PARAMS}',
    'table-name' = 'application',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.max-retries' = '${FLINK_SINK_MAX_RETRIES}'
);

CREATE TEMPORARY VIEW v_app_triggers AS
SELECT id AS app_row_id, proc_time FROM cdc_market_app WHERE id IS NOT NULL
UNION ALL
SELECT m.app_row_id, c.proc_time
FROM cdc_core_app AS c
INNER JOIN dim_market_app_by_no FOR SYSTEM_TIME AS OF c.proc_time AS m
    ON m.applicationNo = c.ext_sn
WHERE c.ext_sn IS NOT NULL AND TRIM(c.ext_sn) <> ''
UNION ALL
SELECT a.app_row_id, ud.proc_time
FROM cdc_user_data AS ud
INNER JOIN dim_apps_by_user FOR SYSTEM_TIME AS OF ud.proc_time AS a
    ON a.user_id = ud.`userId`
WHERE ud.`userId` IS NOT NULL;

INSERT INTO sink_application
SELECT
    b.application_no,
    CASE
        WHEN b.mobile_token IS NOT NULL AND TRIM(b.mobile_token) <> '' THEN b.mobile_token
        WHEN b.mobile_norm IS NULL OR TRIM(b.mobile_norm) = '' THEN ''
        ELSE COALESCE(vt_tokenize(TRIM(b.mobile_norm)), '')
    END AS mobile,
    '' AS coupon_code,
    b.bid,
    b.app_id,
    b.app_version,
    b.user_id,
    b.user_id AS group_user_id,
    b.sn,
    0 AS is_test,
    CAST(b.is_first_apply AS INT) AS is_first_apply,
    0 AS is_auto_apply,
    COALESCE(
        NULLIF(TRIM(b.id_number_token), ''),
        CASE WHEN b.bvn_raw IS NOT NULL AND TRIM(b.bvn_raw) <> '' THEN COALESCE(vt_tokenize(TRIM(b.bvn_raw)), '') ELSE '' END
    ) AS id_number,
    CASE
        WHEN b.gaid_token IS NOT NULL AND TRIM(b.gaid_token) <> '' THEN b.gaid_token
        WHEN b.gaid_raw IS NULL OR TRIM(b.gaid_raw) = '' THEN CAST(NULL AS STRING)
        ELSE vt_tokenize(TRIM(b.gaid_raw))
    END AS gaid_idfa,
    b.device_uuid,
    CAST(NULL AS STRING) AS session_id,
    b.bank_code,
    '' AS bank_account_name,
    CASE
        WHEN b.bank_account_token IS NOT NULL AND TRIM(b.bank_account_token) <> '' THEN b.bank_account_token
        WHEN b.bank_account_raw IS NULL OR TRIM(b.bank_account_raw) = '' THEN ''
        ELSE vt_tokenize(TRIM(b.bank_account_raw))
    END AS bank_account_number,
    b.product_id,
    'PROD-002-D7' AS product_scheme_id,
    48 AS product_calculator_version,
    50 AS repay_calculator_version,
    49 AS rollover_calculator_version,
    '{"product_scheme_id":"PROD-002-D7"}' AS product_scheme_param,
    CAST(b.term AS INT) AS term,
    1 AS periods,
    1 AS repayment_method,
    CONCAT(
        '{"roll_sequence":0,"period":1,"principal":', CAST(GREATEST(COALESCE(b.disburse_amount, 0), 0) AS STRING),
        ',"disbursed_amount":', CAST(GREATEST(COALESCE(b.disburse_amount, 0), 0) AS STRING),
        ',"interest":0,"admin_fee":', CAST(GREATEST(COALESCE(b.core_orig_fee, 0), 0) AS STRING),
        ',"service_fee":0,"tax_fee":0,"reduction_amount":0,"total_amount":', CAST(GREATEST(COALESCE(b.repayment, 0), 0) AS STRING),
        ',"term":', CAST(COALESCE(b.term, 0) AS STRING),
        ',"roll_allowed":0}'
    ) AS repayment_plan,
    -- 目标库 amount 列为 bigint unsigned；源库 repayment 存在负数，直接写入会 Data truncation 并拖垮整 job
    GREATEST(COALESCE(b.amount, 0), 0) AS credit_limit,
    GREATEST(COALESCE(b.amount, 0), 0) AS loan_amount,
    GREATEST(COALESCE(b.disburse_amount, 0), 0) AS principal,
    GREATEST(COALESCE(b.repayment, 0), 0) AS total_amount,
    GREATEST(COALESCE(b.disburse_amount, 0), 0) AS disbursed_amount,
    COALESCE(b.apply_date, 0) * 1000 AS created_time,
    COALESCE(b.core_apply_time, 0) * 1000 AS submited_time,
    COALESCE(b.core_audit_time, 0) * 1000 AS reviewed_time,
    COALESCE(b.disburse_time, 0) * 1000 AS disbursed_time,
    COALESCE(b.last_repay_time, 0) * 1000 AS last_paid_time,
    COALESCE(b.paid_time, 0) * 1000 AS paid_off_time,
    CASE WHEN COALESCE(b.apply_date, 0) > 0 THEN (b.apply_date + 7 * 86400) * 1000 ELSE 0 END AS lock_expire_time,
    CASE CAST(b.src_status AS INT)
        WHEN 0 THEN 1 WHEN 1 THEN 1 WHEN 2 THEN 1 WHEN 4 THEN 1
        WHEN 5 THEN 3 WHEN 3 THEN 5 WHEN 6 THEN 5 WHEN 8 THEN 7 WHEN 7 THEN 11
        WHEN 9 THEN 13 WHEN 10 THEN 13 WHEN 12 THEN 15
        WHEN 11 THEN 20 WHEN 13 THEN 20 WHEN 14 THEN 20 WHEN 16 THEN 20
        WHEN 15 THEN 23 WHEN 17 THEN 27 WHEN 18 THEN 27 WHEN 19 THEN 27
        ELSE CAST(b.src_status AS INT)
    END AS status
FROM v_app_triggers AS t
INNER JOIN dim_app_bundle FOR SYSTEM_TIME AS OF t.proc_time AS b
    ON b.app_row_id = t.app_row_id
WHERE b.core_sn IS NOT NULL AND TRIM(b.core_sn) <> ''
  AND b.mobile_norm IS NOT NULL AND TRIM(b.mobile_norm) <> '';
