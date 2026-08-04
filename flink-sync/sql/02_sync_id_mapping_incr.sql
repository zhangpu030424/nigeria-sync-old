-- 增量 id_mapping：market application / user_data / user CDC + bundle Lookup
-- 口径对齐 ng_migration_run._build_id_mapping_rows：
--   id(锚点)=mobile token；单向展开 type=
--     mobile / gaid_idfa / device_uuid / bank_account / id_number / id2
-- Lookup 键：裸 applicationNo（唯一索引），避免 UNSIGNED id BigInteger ClassCast
-- 前置: ./scripts/deploy-source-ddl.sh（含 id_mapping_incr_bundle_by_no_lookup）
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
    applicationNo STRING,
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
    'server-id' = '${CDC_SERVER_ID_IDMAP_APP}',
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
    'server-id' = '${CDC_SERVER_ID_IDMAP_USER_DATA}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only'
);

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
    'server-id' = '${CDC_SERVER_ID_IDMAP_USER}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only'
);

CREATE TABLE IF NOT EXISTS dim_idmap_bundle (
    market_application_no STRING,
    app_row_id BIGINT,
    app_id BIGINT,
    mobile_norm STRING,
    mobile_token STRING,
    gaid_raw STRING,
    gaid_token STRING,
    device_uuid STRING,
    bank_account_raw STRING,
    bank_account_token STRING,
    bvn_raw STRING,
    id_number_token STRING,
    id2_raw STRING,
    id2_token STRING,
    event_time BIGINT,
    PRIMARY KEY (market_application_no) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MARKET_MYSQL_HOST}:${MARKET_MYSQL_PORT}/${MARKET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'id_mapping_incr_bundle_by_no_lookup',
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

CREATE TABLE IF NOT EXISTS sink_id_mapping (
    id STRING,
    app_id INT,
    mapping_id STRING,
    `type` STRING,
    event_time BIGINT,
    PRIMARY KEY (id, app_id, mapping_id, `type`) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?${TARGET_JDBC_PARAMS}',
    'table-name' = 'id_mapping',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '200ms',
    'sink.max-retries' = '${FLINK_SINK_MAX_RETRIES}'
);

CREATE TEMPORARY VIEW v_idmap_triggers AS
SELECT applicationNo AS market_application_no, proc_time
FROM cdc_market_app
WHERE applicationNo IS NOT NULL AND TRIM(applicationNo) <> ''
UNION ALL
SELECT m.market_application_no, ud.proc_time
FROM cdc_user_data AS ud
INNER JOIN dim_app_no_by_user FOR SYSTEM_TIME AS OF ud.proc_time AS m
    ON m.user_id = ud.`userId`
WHERE ud.`userId` IS NOT NULL
UNION ALL
SELECT m.market_application_no, u.proc_time
FROM cdc_user AS u
INNER JOIN dim_app_no_by_user FOR SYSTEM_TIME AS OF u.proc_time AS m
    ON m.user_id = u.id
WHERE u.id IS NOT NULL;

-- 先解析 token，再 ARRAY/UNNEST 展开（对齐迁移固定 type 顺序）
INSERT INTO sink_id_mapping
SELECT e.id, x.app_id, e.mapping_id, e.`type`, x.event_time
FROM (
    SELECT
        CAST(p.app_id AS INT) AS app_id,
        COALESCE(p.event_time, 0) AS event_time,
        p.mobile_tok,
        p.gaid_tok,
        p.device_uuid,
        p.bank_tok,
        p.id_number_tok,
        p.id2_tok,
        ARRAY[
            ROW(p.mobile_tok, p.mobile_tok, CAST('mobile' AS STRING)),
            ROW(p.mobile_tok, p.gaid_tok, CAST('gaid_idfa' AS STRING)),
            ROW(p.mobile_tok, p.device_uuid, CAST('device_uuid' AS STRING)),
            ROW(p.mobile_tok, p.bank_tok, CAST('bank_account' AS STRING)),
            ROW(p.mobile_tok, p.id_number_tok, CAST('id_number' AS STRING)),
            ROW(p.mobile_tok, p.id2_tok, CAST('id2' AS STRING))
        ] AS edges
    FROM (
        SELECT
            b.app_id,
            b.event_time,
            CASE
                WHEN b.mobile_token IS NOT NULL AND TRIM(b.mobile_token) <> '' THEN b.mobile_token
                WHEN b.mobile_norm IS NULL OR TRIM(b.mobile_norm) = '' THEN CAST(NULL AS STRING)
                ELSE vt_tokenize(TRIM(b.mobile_norm))
            END AS mobile_tok,
            CASE
                WHEN b.gaid_token IS NOT NULL AND TRIM(b.gaid_token) <> '' THEN b.gaid_token
                WHEN b.gaid_raw IS NULL OR TRIM(b.gaid_raw) = '' THEN CAST(NULL AS STRING)
                ELSE vt_tokenize(TRIM(b.gaid_raw))
            END AS gaid_tok,
            CASE
                WHEN b.device_uuid IS NULL OR TRIM(b.device_uuid) = '' THEN CAST(NULL AS STRING)
                ELSE TRIM(b.device_uuid)
            END AS device_uuid,
            CASE
                WHEN b.bank_account_token IS NOT NULL AND TRIM(b.bank_account_token) <> '' THEN b.bank_account_token
                WHEN b.bank_account_raw IS NULL OR TRIM(b.bank_account_raw) = '' THEN CAST(NULL AS STRING)
                ELSE vt_tokenize(TRIM(b.bank_account_raw))
            END AS bank_tok,
            CASE
                WHEN b.id_number_token IS NOT NULL AND TRIM(b.id_number_token) <> '' THEN b.id_number_token
                WHEN b.bvn_raw IS NULL OR TRIM(b.bvn_raw) = '' THEN CAST(NULL AS STRING)
                ELSE vt_tokenize(TRIM(b.bvn_raw))
            END AS id_number_tok,
            CASE
                WHEN b.id2_token IS NOT NULL AND TRIM(b.id2_token) <> '' THEN b.id2_token
                WHEN b.id2_raw IS NULL OR TRIM(b.id2_raw) = '' THEN CAST(NULL AS STRING)
                ELSE vt_tokenize(TRIM(b.id2_raw))
            END AS id2_tok
        FROM v_idmap_triggers AS t
        INNER JOIN dim_idmap_bundle FOR SYSTEM_TIME AS OF t.proc_time AS b
            ON b.market_application_no = t.market_application_no
        WHERE b.mobile_norm IS NOT NULL AND TRIM(b.mobile_norm) <> ''
    ) AS p
    WHERE p.mobile_tok IS NOT NULL AND TRIM(p.mobile_tok) <> ''
) AS x
CROSS JOIN UNNEST(x.edges) AS e(id, mapping_id, `type`)
WHERE e.id IS NOT NULL AND TRIM(e.id) <> ''
  AND e.mapping_id IS NOT NULL AND TRIM(e.mapping_id) <> '';
