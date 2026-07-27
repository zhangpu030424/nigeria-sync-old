-- 增量 user_product：application CDC + 最新额度 Lookup
SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '200ms';
SET 'table.exec.mini-batch.size' = '${FLINK_MINI_BATCH_SIZE}';
SET 'execution.checkpointing.interval' = '${FLINK_CHECKPOINT_INTERVAL}';
SET 'execution.checkpointing.timeout' = '${FLINK_CHECKPOINT_TIMEOUT}';
SET 'execution.checkpointing.min-pause' = '120s';

CREATE TABLE IF NOT EXISTS cdc_application (
    id BIGINT, `userId` BIGINT, `productId` BIGINT, proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc', 'hostname' = '${MARKET_MYSQL_HOST}', 'port' = '${MARKET_MYSQL_PORT}',
    'username' = '${MARKET_MYSQL_USER}', 'password' = '${MARKET_MYSQL_PASSWORD}',
    'database-name' = '${MARKET_MYSQL_DATABASE}', 'table-name' = 'application',
    'server-time-zone' = 'Africa/Lagos', 'server-id' = '${CDC_SERVER_ID_USER_PRODUCT}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}', 'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true', 'debezium.snapshot.mode' = 'schema_only'
);

CREATE TABLE IF NOT EXISTS dim_user_product (
    user_id BIGINT, product_id BIGINT, credit_amount BIGINT,
    PRIMARY KEY (user_id, product_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MARKET_MYSQL_HOST}:${MARKET_MYSQL_PORT}/${MARKET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'user_product_incr_lookup',
    'username' = '${MARKET_MYSQL_USER}', 'password' = '${MARKET_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '200000', 'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TABLE IF NOT EXISTS sink_user_product (
    group_user_id BIGINT, product_id BIGINT, schemes STRING, is_open INT,
    credit_amount BIGINT, unpaid_amount BIGINT, locked_amount BIGINT, available_amount BIGINT,
    PRIMARY KEY (group_user_id, product_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?${TARGET_JDBC_PARAMS}',
    'table-name' = 'user_product', 'username' = '${TARGET_MYSQL_USER}', 'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}', 'sink.max-retries' = '${FLINK_SINK_MAX_RETRIES}'
);

INSERT INTO sink_user_product
SELECT
    p.user_id AS group_user_id,
    p.product_id,
    '[]' AS schemes,
    1 AS is_open,
    COALESCE(p.credit_amount, 0) AS credit_amount,
    0 AS unpaid_amount,
    0 AS locked_amount,
    COALESCE(p.credit_amount, 0) AS available_amount
FROM cdc_application AS c
INNER JOIN dim_user_product FOR SYSTEM_TIME AS OF c.proc_time AS p
    ON p.user_id = c.`userId` AND p.product_id = c.`productId`
WHERE c.`productId` IS NOT NULL AND c.`productId` <> 0;
