-- 增量 user_bankcard：user_data CDC + Lookup + 雪花发号
CREATE TEMPORARY FUNCTION snowflake_id AS 'com.nigeria.flink.udf.SnowflakeIdFunction';
CREATE TEMPORARY FUNCTION bankcard_id_resolve AS 'com.nigeria.flink.udf.UserBankcardIdResolveFunction';

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'execution.checkpointing.interval' = '${FLINK_CHECKPOINT_INTERVAL}';
SET 'execution.checkpointing.timeout' = '${FLINK_CHECKPOINT_TIMEOUT}';

CREATE TABLE IF NOT EXISTS cdc_user_data (
    id BIGINT, `userId` BIGINT, proc_time AS PROCTIME(), PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc', 'hostname' = '${MARKET_MYSQL_HOST}', 'port' = '${MARKET_MYSQL_PORT}',
    'username' = '${MARKET_MYSQL_USER}', 'password' = '${MARKET_MYSQL_PASSWORD}',
    'database-name' = '${MARKET_MYSQL_DATABASE}', 'table-name' = 'user_data',
    'server-time-zone' = 'Africa/Lagos', 'server-id' = '${CDC_SERVER_ID_BANKCARD}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}', 'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true', 'debezium.snapshot.mode' = 'schema_only'
);

CREATE TABLE IF NOT EXISTS dim_bankcard (
    user_id BIGINT, bank_code STRING, bank_account_raw STRING, bank_account_token STRING, is_default INT,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MARKET_MYSQL_HOST}:${MARKET_MYSQL_PORT}/${MARKET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'user_bankcard_incr_lookup',
    'username' = '${MARKET_MYSQL_USER}', 'password' = '${MARKET_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '200000', 'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TABLE IF NOT EXISTS sink_user_bankcard (
    id BIGINT, group_user_id BIGINT, bank_code STRING, bank_account_number STRING, is_default INT,
    PRIMARY KEY (group_user_id, bank_account_number) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?${TARGET_JDBC_PARAMS}',
    'table-name' = 'user_bankcard', 'username' = '${TARGET_MYSQL_USER}', 'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}', 'sink.max-retries' = '${FLINK_SINK_MAX_RETRIES}'
);

INSERT INTO sink_user_bankcard
SELECT
    bankcard_id_resolve(b.user_id, COALESCE(b.bank_account_token, b.bank_account_raw)) AS id,
    b.user_id AS group_user_id,
    b.bank_code,
    COALESCE(b.bank_account_token, b.bank_account_raw) AS bank_account_number,
    b.is_default
FROM cdc_user_data AS c
INNER JOIN dim_bankcard FOR SYSTEM_TIME AS OF c.proc_time AS b
    ON b.user_id = c.`userId`
WHERE COALESCE(b.bank_account_token, b.bank_account_raw) IS NOT NULL
  AND TRIM(COALESCE(b.bank_account_token, b.bank_account_raw)) <> '';
