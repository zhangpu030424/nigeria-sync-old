-- 增量 user：CDC 触发 + Lookup 组装（ng_loan_market）
-- 前置: ./scripts/deploy-source-ddl.sh
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

CREATE TABLE IF NOT EXISTS cdc_user (
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
    'table-name' = 'user',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_USER}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS cdc_lup (
    id BIGINT,
    `appId` BIGINT,
    mobile STRING,
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${MARKET_MYSQL_HOST}',
    'port' = '${MARKET_MYSQL_PORT}',
    'username' = '${MARKET_MYSQL_USER}',
    'password' = '${MARKET_MYSQL_PASSWORD}',
    'database-name' = '${MARKET_MYSQL_DATABASE}',
    'table-name' = 'log_user_password',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_LUP}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only'
);

CREATE TABLE IF NOT EXISTS cdc_dac (
    id BIGINT,
    `deviceId` BIGINT,
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${MARKET_MYSQL_HOST}',
    'port' = '${MARKET_MYSQL_PORT}',
    'username' = '${MARKET_MYSQL_USER}',
    'password' = '${MARKET_MYSQL_PASSWORD}',
    'database-name' = '${MARKET_MYSQL_DATABASE}',
    'table-name' = 'device_ad_channel',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_DAC}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only'
);

CREATE TABLE IF NOT EXISTS dim_user_row (
    user_id BIGINT,
    app_id BIGINT,
    mobile_norm STRING,
    mobile_token STRING,
    reg_device_uuid STRING,
    closed_time BIGINT,
    reg_time BIGINT,
    test_flag INT,
    password STRING,
    dac_channel STRING,
    google_ads_campaign_id STRING,
    google_ads_adgroup_id STRING,
    fb_install_referrer_campaign_id STRING,
    fb_install_referrer_campaign_group_id STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MARKET_MYSQL_HOST}:${MARKET_MYSQL_PORT}/${MARKET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_incr_lookup',
    'username' = '${MARKET_MYSQL_USER}',
    'password' = '${MARKET_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '300000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TABLE IF NOT EXISTS dim_users_by_app_mobile (
    app_id BIGINT,
    mobile_raw STRING,
    user_id BIGINT,
    PRIMARY KEY (app_id, mobile_raw) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MARKET_MYSQL_HOST}:${MARKET_MYSQL_PORT}/${MARKET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'users_by_app_mobile_lookup',
    'username' = '${MARKET_MYSQL_USER}',
    'password' = '${MARKET_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '200000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TABLE IF NOT EXISTS dim_users_by_device (
    device_id BIGINT,
    user_id BIGINT,
    PRIMARY KEY (device_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MARKET_MYSQL_HOST}:${MARKET_MYSQL_PORT}/${MARKET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'users_by_device_lookup',
    'username' = '${MARKET_MYSQL_USER}',
    'password' = '${MARKET_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '200000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TABLE IF NOT EXISTS sink_user (
    user_id BIGINT,
    app_id BIGINT,
    group_user_id BIGINT,
    info_user_id BIGINT,
    mobile STRING,
    password STRING,
    closed_time BIGINT,
    reg_device_uuid STRING,
    reg_time BIGINT,
    test_flag INT,
    utm_source STRING,
    utm_medium STRING,
    utm_campaign STRING,
    utm_content STRING,
    utm_term STRING,
    campaign_id STRING,
    ad_group_id STRING,
    advertiser_id STRING,
    PRIMARY KEY (mobile, app_id, closed_time) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?${TARGET_JDBC_PARAMS}',
    'table-name' = 'user',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.max-retries' = '${FLINK_SINK_MAX_RETRIES}'
);

CREATE TEMPORARY VIEW v_user_triggers AS
SELECT id AS user_id, proc_time FROM cdc_user WHERE id IS NOT NULL
UNION ALL
SELECT m.user_id, l.proc_time
FROM cdc_lup AS l
INNER JOIN dim_users_by_app_mobile FOR SYSTEM_TIME AS OF l.proc_time AS m
    ON m.app_id = l.`appId` AND m.mobile_raw = l.mobile
UNION ALL
SELECT d.user_id, c.proc_time
FROM cdc_dac AS c
INNER JOIN dim_users_by_device FOR SYSTEM_TIME AS OF c.proc_time AS d
    ON d.device_id = c.`deviceId`;

INSERT INTO sink_user
SELECT
    u.user_id,
    u.app_id,
    u.user_id AS group_user_id,
    u.user_id AS info_user_id,
    CASE
        WHEN u.mobile_token IS NOT NULL AND TRIM(u.mobile_token) <> '' THEN u.mobile_token
        WHEN u.mobile_norm IS NULL OR TRIM(u.mobile_norm) = '' THEN CAST(NULL AS STRING)
        ELSE vt_tokenize(TRIM(u.mobile_norm))
    END AS mobile,
    u.password,
    u.closed_time,
    u.reg_device_uuid,
    u.reg_time,
    u.test_flag,
    CASE UPPER(IFNULL(u.dac_channel, ''))
        WHEN 'ORGANIC' THEN 'organic'
        WHEN 'FB' THEN 'facebook'
        WHEN 'TT' THEN 'tiktok'
        WHEN 'GG' THEN 'google'
        ELSE NULL
    END AS utm_source,
    CAST(NULL AS STRING) AS utm_medium,
    CAST(NULL AS STRING) AS utm_campaign,
    CAST(NULL AS STRING) AS utm_content,
    CAST(NULL AS STRING) AS utm_term,
    CASE WHEN u.dac_channel = 'GG' THEN u.google_ads_campaign_id ELSE u.fb_install_referrer_campaign_id END AS campaign_id,
    CASE WHEN u.dac_channel = 'GG' THEN u.google_ads_adgroup_id ELSE u.fb_install_referrer_campaign_group_id END AS ad_group_id,
    CAST(NULL AS STRING) AS advertiser_id
FROM v_user_triggers AS t
INNER JOIN dim_user_row FOR SYSTEM_TIME AS OF t.proc_time AS u
    ON u.user_id = t.user_id
WHERE u.mobile_norm IS NOT NULL AND TRIM(u.mobile_norm) <> ''
  AND (
    (u.mobile_token IS NOT NULL AND TRIM(u.mobile_token) <> '')
    OR (u.mobile_norm IS NOT NULL AND TRIM(u.mobile_norm) <> '')
  );
