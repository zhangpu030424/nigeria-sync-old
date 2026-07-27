-- 增量 user_info：多源 CDC + bundle Lookup
CREATE TEMPORARY FUNCTION vt_tokenize AS 'com.nigeria.flink.udf.VtTokenizeFunction';

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '200ms';
SET 'table.exec.mini-batch.size' = '${FLINK_MINI_BATCH_SIZE}';
SET 'execution.checkpointing.interval' = '${FLINK_CHECKPOINT_INTERVAL}';
SET 'execution.checkpointing.timeout' = '${FLINK_CHECKPOINT_TIMEOUT}';
SET 'execution.checkpointing.min-pause' = '120s';
SET 'execution.checkpointing.unaligned' = 'true';

CREATE TABLE IF NOT EXISTS cdc_user (
    id BIGINT, proc_time AS PROCTIME(), PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc', 'hostname' = '${MARKET_MYSQL_HOST}', 'port' = '${MARKET_MYSQL_PORT}',
    'username' = '${MARKET_MYSQL_USER}', 'password' = '${MARKET_MYSQL_PASSWORD}',
    'database-name' = '${MARKET_MYSQL_DATABASE}', 'table-name' = 'user',
    'server-time-zone' = 'Africa/Lagos', 'server-id' = '${CDC_SERVER_ID_UI_USER}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}', 'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true', 'debezium.snapshot.mode' = 'schema_only'
);

CREATE TABLE IF NOT EXISTS cdc_user_data (
    id BIGINT, `userId` BIGINT, proc_time AS PROCTIME(), PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc', 'hostname' = '${MARKET_MYSQL_HOST}', 'port' = '${MARKET_MYSQL_PORT}',
    'username' = '${MARKET_MYSQL_USER}', 'password' = '${MARKET_MYSQL_PASSWORD}',
    'database-name' = '${MARKET_MYSQL_DATABASE}', 'table-name' = 'user_data',
    'server-time-zone' = 'Africa/Lagos', 'server-id' = '${CDC_SERVER_ID_USER_DATA}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}', 'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true', 'debezium.snapshot.mode' = 'schema_only'
);

CREATE TABLE IF NOT EXISTS cdc_uri (
    id BIGINT, `userId` BIGINT, proc_time AS PROCTIME(), PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc', 'hostname' = '${MARKET_MYSQL_HOST}', 'port' = '${MARKET_MYSQL_PORT}',
    'username' = '${MARKET_MYSQL_USER}', 'password' = '${MARKET_MYSQL_PASSWORD}',
    'database-name' = '${MARKET_MYSQL_DATABASE}', 'table-name' = 'user_reg_ip',
    'server-time-zone' = 'Africa/Lagos', 'server-id' = '${CDC_SERVER_ID_URI}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}', 'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true', 'debezium.snapshot.mode' = 'schema_only'
);

CREATE TABLE IF NOT EXISTS dim_user_info_bundle (
    user_id BIGINT, full_name STRING, id_number_token STRING, password STRING,
    registration_ip STRING, install_channel STRING, app_name STRING, app_id BIGINT, reg_time BIGINT,
    bvn_raw STRING,
    email STRING, birthday STRING, gender INT,
    addressState STRING, addressDistrict STRING, address STRING,
    company STRING, education INT, marital INT, profession STRING, salary STRING,
    emergencyContact STRING, numberOfChildren INT, payCycle INT, salaryDay INT,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MARKET_MYSQL_HOST}:${MARKET_MYSQL_PORT}/${MARKET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_info_incr_bundle_lookup',
    'username' = '${MARKET_MYSQL_USER}', 'password' = '${MARKET_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '300000', 'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TABLE IF NOT EXISTS sink_user_info (
    user_id BIGINT, id_number STRING, full_name STRING, password STRING,
    live_image STRING, id_card STRING, info STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?${TARGET_JDBC_PARAMS}',
    'table-name' = 'user_info', 'username' = '${TARGET_MYSQL_USER}', 'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}', 'sink.max-retries' = '${FLINK_SINK_MAX_RETRIES}'
);

CREATE TEMPORARY VIEW v_ui_triggers AS
SELECT id AS user_id, proc_time FROM cdc_user WHERE id IS NOT NULL
UNION ALL
SELECT `userId` AS user_id, proc_time FROM cdc_user_data WHERE `userId` IS NOT NULL
UNION ALL
SELECT `userId` AS user_id, proc_time FROM cdc_uri WHERE `userId` IS NOT NULL;

INSERT INTO sink_user_info
SELECT
    b.user_id,
    COALESCE(b.id_number_token, CASE WHEN b.bvn_raw IS NOT NULL AND TRIM(b.bvn_raw) <> '' THEN vt_tokenize(b.bvn_raw, 'id_number') ELSE '' END) AS id_number,
    TRIM(b.full_name) AS full_name,
    b.password,
    '' AS live_image,
    '' AS id_card,
    CONCAT(
        '{"full_name":', CASE WHEN b.full_name IS NULL THEN 'null' ELSE CONCAT('"', REPLACE(b.full_name, '"', '\\"'), '"') END,
        ',"registration_ip":', CASE WHEN b.registration_ip IS NULL THEN 'null' ELSE CONCAT('"', b.registration_ip, '"') END,
        ',"registration_time":', CASE WHEN b.reg_time IS NULL THEN 'null' ELSE CAST(b.reg_time AS STRING) END,
        ',"install_source":', CASE WHEN b.install_channel IS NULL THEN 'null' ELSE CONCAT('"', b.install_channel, '"') END,
        ',"app":{"name":', CASE WHEN b.app_name IS NULL THEN 'null' ELSE CONCAT('"', b.app_name, '"') END,
        ',"app_id":', CASE WHEN b.app_id IS NULL THEN 'null' ELSE CONCAT('"', CAST(b.app_id AS STRING), '"') END, ',"version":null}}'
    ) AS info
FROM v_ui_triggers AS t
INNER JOIN dim_user_info_bundle FOR SYSTEM_TIME AS OF t.proc_time AS b
    ON b.user_id = t.user_id;
