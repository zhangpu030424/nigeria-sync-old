-- 增量 application：market + core CDC 触发 + bundle Lookup
-- Lookup 键：裸 applicationNo（唯一索引），避免 UNSIGNED id 的 BigInteger ClassCast / CAST 全表扫
-- 视图：application_incr_bundle_by_no_lookup / market_app_no_by_user_lookup（不改源表结构）
-- VT：一行 4 字段合并一次 /v2t（vt_tokenize_app）+ 进程内 LRU
CREATE TEMPORARY FUNCTION vt_tokenize_app AS 'com.nigeria.flink.udf.VtTokenizeAppFieldsFunction';

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
    applicationNo STRING,
    -- status 走 CDC，避免 Lookup 缓存把中间态写成最终态（1→5→6 / 7→13 秒级连跳）
    status INT,
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
    'debezium.snapshot.mode' = 'schema_only',
    'debezium.bigint.unsigned.handling.mode' = 'long'
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
    'debezium.snapshot.mode' = 'schema_only',
    'debezium.bigint.unsigned.handling.mode' = 'long'
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
    market_application_no STRING,
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
    PRIMARY KEY (market_application_no) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MARKET_MYSQL_HOST}:${MARKET_MYSQL_PORT}/${MARKET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'application_incr_bundle_by_no_lookup',
    'username' = '${MARKET_MYSQL_USER}',
    'password' = '${MARKET_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TABLE IF NOT EXISTS dim_app_no_by_user (
    user_id BIGINT,
    market_application_no STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MARKET_MYSQL_HOST}:${MARKET_MYSQL_PORT}/${MARKET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'market_app_no_by_user_lookup',
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
SELECT applicationNo AS market_application_no, status AS cdc_status, proc_time
FROM cdc_market_app
WHERE applicationNo IS NOT NULL AND TRIM(applicationNo) <> ''
UNION ALL
SELECT ext_sn AS market_application_no, CAST(NULL AS INT) AS cdc_status, proc_time
FROM cdc_core_app
WHERE ext_sn IS NOT NULL AND TRIM(ext_sn) <> ''
UNION ALL
SELECT m.market_application_no, CAST(NULL AS INT) AS cdc_status, ud.proc_time
FROM cdc_user_data AS ud
INNER JOIN dim_app_no_by_user FOR SYSTEM_TIME AS OF ud.proc_time AS m
    ON m.user_id = ud.`userId`
WHERE ud.`userId` IS NOT NULL;

INSERT INTO sink_application
SELECT
    x.application_no,
    COALESCE(x.vt.mobile, '') AS mobile,
    '' AS coupon_code,
    x.bid,
    x.app_id,
    x.app_version,
    x.user_id,
    x.user_id AS group_user_id,
    x.sn,
    0 AS is_test,
    CAST(x.is_first_apply AS INT) AS is_first_apply,
    0 AS is_auto_apply,
    COALESCE(x.vt.id_number, '') AS id_number,
    x.vt.gaid_idfa AS gaid_idfa,
    x.device_uuid,
    CAST(NULL AS STRING) AS session_id,
    x.bank_code,
    '' AS bank_account_name,
    COALESCE(x.vt.bank_account, '') AS bank_account_number,
    x.product_id,
    'PROD-002-D7' AS product_scheme_id,
    48 AS product_calculator_version,
    50 AS repay_calculator_version,
    49 AS rollover_calculator_version,
    '{"product_scheme_id":"PROD-002-D7"}' AS product_scheme_param,
    CAST(x.term AS INT) AS term,
    1 AS periods,
    1 AS repayment_method,
    CONCAT(
        '{"roll_sequence":0,"period":1,"principal":', CAST(GREATEST(COALESCE(x.disburse_amount, 0), 0) AS STRING),
        ',"disbursed_amount":', CAST(GREATEST(COALESCE(x.disburse_amount, 0), 0) AS STRING),
        ',"interest":0,"admin_fee":', CAST(GREATEST(COALESCE(x.core_orig_fee, 0), 0) AS STRING),
        ',"service_fee":0,"tax_fee":0,"reduction_amount":0,"total_amount":', CAST(GREATEST(COALESCE(x.repayment, 0), 0) AS STRING),
        ',"term":', CAST(COALESCE(x.term, 0) AS STRING),
        ',"roll_allowed":0}'
    ) AS repayment_plan,
    -- 目标库 amount 列为 bigint unsigned；源库 repayment 存在负数，直接写入会 Data truncation 并拖垮整 job
    GREATEST(COALESCE(x.amount, 0), 0) AS credit_limit,
    GREATEST(COALESCE(x.amount, 0), 0) AS loan_amount,
    GREATEST(COALESCE(x.disburse_amount, 0), 0) AS principal,
    GREATEST(COALESCE(x.repayment, 0), 0) AS total_amount,
    GREATEST(COALESCE(x.disburse_amount, 0), 0) AS disbursed_amount,
    COALESCE(x.apply_date, 0) * 1000 AS created_time,
    COALESCE(x.core_apply_time, 0) * 1000 AS submited_time,
    COALESCE(x.core_audit_time, 0) * 1000 AS reviewed_time,
    COALESCE(x.disburse_time, 0) * 1000 AS disbursed_time,
    COALESCE(x.last_repay_time, 0) * 1000 AS last_paid_time,
    COALESCE(x.paid_time, 0) * 1000 AS paid_off_time,
    CASE WHEN COALESCE(x.apply_date, 0) > 0 THEN (x.apply_date + 7 * 86400) * 1000 ELSE 0 END AS lock_expire_time,
    -- market CDC 优先用事件自带 status；core/user_data 触发仍回落 Lookup src_status
    CASE CAST(COALESCE(x.cdc_status, x.src_status) AS INT)
        WHEN 0 THEN 1 WHEN 1 THEN 1 WHEN 2 THEN 1 WHEN 4 THEN 1
        WHEN 5 THEN 3 WHEN 3 THEN 5 WHEN 6 THEN 5 WHEN 8 THEN 7 WHEN 7 THEN 11
        WHEN 9 THEN 13 WHEN 10 THEN 13 WHEN 12 THEN 15
        WHEN 11 THEN 20 WHEN 13 THEN 20 WHEN 14 THEN 20 WHEN 16 THEN 20
        WHEN 15 THEN 23 WHEN 17 THEN 27 WHEN 18 THEN 27 WHEN 19 THEN 27
        ELSE CAST(COALESCE(x.cdc_status, x.src_status) AS INT)
    END AS status
FROM (
    SELECT
        b.*,
        t.cdc_status,
        vt_tokenize_app(
            b.mobile_token, b.mobile_norm,
            b.id_number_token, b.bvn_raw,
            b.gaid_token, b.gaid_raw,
            b.bank_account_token, b.bank_account_raw
        ) AS vt
    FROM v_app_triggers AS t
    INNER JOIN dim_app_bundle FOR SYSTEM_TIME AS OF t.proc_time AS b
        ON b.market_application_no = t.market_application_no
    WHERE b.core_sn IS NOT NULL AND TRIM(b.core_sn) <> ''
      AND b.mobile_norm IS NOT NULL AND TRIM(b.mobile_norm) <> ''
) AS x;
