-- 增量 loan：core repay_plan / repay_record + market application CDC
-- sn 维表用 DECIMAL + 视图裸列，保证 JDBC Lookup 走 repay_plan.sn 索引（勿 CAST CHAR）
SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '200ms';
SET 'table.exec.mini-batch.size' = '${FLINK_MINI_BATCH_SIZE}';
SET 'execution.checkpointing.interval' = '${FLINK_CHECKPOINT_INTERVAL}';
SET 'execution.checkpointing.timeout' = '${FLINK_CHECKPOINT_TIMEOUT}';
SET 'execution.checkpointing.min-pause' = '120s';
SET 'execution.checkpointing.tolerable-failed-checkpoints' = '10';
SET 'execution.checkpointing.unaligned' = 'true';
SET 'table.exec.async-lookup.buffer-capacity' = '200';
SET 'table.exec.async-lookup.timeout' = '60s';

CREATE TABLE IF NOT EXISTS cdc_repay_plan (
    sn DECIMAL(20, 0),
    plan_sn BIGINT,
    proc_time AS PROCTIME(),
    PRIMARY KEY (sn, plan_sn) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${CORE_MYSQL_HOST}',
    'port' = '${CORE_MYSQL_PORT}',
    'username' = '${CORE_MYSQL_USER}',
    'password' = '${CORE_MYSQL_PASSWORD}',
    'database-name' = '${CORE_MYSQL_DATABASE}',
    'table-name' = 'repay_plan',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_LOAN_PLAN}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only'
);

CREATE TABLE IF NOT EXISTS cdc_repay_record (
    id BIGINT,
    sn DECIMAL(20, 0),
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${CORE_MYSQL_HOST}',
    'port' = '${CORE_MYSQL_PORT}',
    'username' = '${CORE_MYSQL_USER}',
    'password' = '${CORE_MYSQL_PASSWORD}',
    'database-name' = '${CORE_MYSQL_DATABASE}',
    'table-name' = 'repay_record',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_LOAN_REPAY}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only'
);

CREATE TABLE IF NOT EXISTS cdc_market_app_disburse (
    id DECIMAL(20, 0),
    `disburseTime` BIGINT,
    -- paidTime 变更也要触发 loan 重算（paid_time = paidTime * 100）
    `paidTime` BIGINT,
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
    'server-id' = '${CDC_SERVER_ID_LOAN_MARKET_APP}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only'
);

CREATE TABLE IF NOT EXISTS dim_loan_bundle (
    sn DECIMAL(20, 0),
    application_no STRING,
    loan_no STRING,
    `period` BIGINT,
    roll_sequence BIGINT,
    start_date BIGINT,
    due_date BIGINT,
    prin_amt BIGINT,
    interest BIGINT,
    orig_fee BIGINT,
    penalty BIGINT,
    amt BIGINT,
    rp_status BIGINT,
    repaid_amt BIGINT,
    repay_last_time BIGINT,
    settle_time BIGINT,
    market_paid_time BIGINT,
    created_at TIMESTAMP(3),
    PRIMARY KEY (sn) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MARKET_MYSQL_HOST}:${MARKET_MYSQL_PORT}/${MARKET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'loan_incr_bundle_lookup',
    'username' = '${MARKET_MYSQL_USER}',
    'password' = '${MARKET_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TABLE IF NOT EXISTS dim_core_sn_by_market_app (
    id DECIMAL(20, 0),
    core_sn DECIMAL(20, 0),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MARKET_MYSQL_HOST}:${MARKET_MYSQL_PORT}/${MARKET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'market_app_core_sn_lookup',
    'username' = '${MARKET_MYSQL_USER}',
    'password' = '${MARKET_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TABLE IF NOT EXISTS sink_loan (
    loan_no STRING,
    application_no STRING,
    `period` INT,
    roll_sequence INT,
    start_date STRING,
    due_date STRING,
    due_date_final STRING,
    principal BIGINT,
    interest BIGINT,
    admin_fee BIGINT,
    service_fee BIGINT,
    tax_fee BIGINT,
    penalty_amount BIGINT,
    reduction_amount BIGINT,
    total_amount BIGINT,
    paid_amount BIGINT,
    paid_time BIGINT,
    paid_off_date STRING,
    created_time BIGINT,
    status INT,
    PRIMARY KEY (application_no, `period`, roll_sequence) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?${TARGET_JDBC_PARAMS}',
    'table-name' = 'loan',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.max-retries' = '${FLINK_SINK_MAX_RETRIES}'
);

CREATE TEMPORARY VIEW v_loan_triggers AS
SELECT sn, proc_time FROM cdc_repay_plan WHERE sn IS NOT NULL
UNION ALL
SELECT sn, proc_time FROM cdc_repay_record WHERE sn IS NOT NULL
UNION ALL
-- 先过滤已放款再 Lookup，避免每条 application CDC 都打 JDBC
SELECT m.core_sn AS sn, a.proc_time
FROM (
    SELECT id, proc_time
    FROM cdc_market_app_disburse
    WHERE COALESCE(`disburseTime`, 0) > 0
) AS a
INNER JOIN dim_core_sn_by_market_app FOR SYSTEM_TIME AS OF a.proc_time AS m
    ON m.id = a.id
WHERE m.core_sn IS NOT NULL;

INSERT INTO sink_loan
SELECT
    l.loan_no,
    l.application_no,
    CAST(l.`period` AS INT),
    CAST(l.roll_sequence AS INT),
    DATE_FORMAT(FROM_UNIXTIME(CASE WHEN l.start_date > 10000000000 THEN l.start_date / 1000 ELSE l.start_date END), 'yyyy-MM-dd') AS start_date,
    DATE_FORMAT(FROM_UNIXTIME(CASE WHEN l.due_date > 10000000000 THEN l.due_date / 1000 ELSE l.due_date END), 'yyyy-MM-dd') AS due_date,
    DATE_FORMAT(FROM_UNIXTIME(CASE WHEN l.due_date > 10000000000 THEN l.due_date / 1000 ELSE l.due_date END), 'yyyy-MM-dd') AS due_date_final,
    GREATEST(COALESCE(l.prin_amt, 0), 0) AS principal,
    GREATEST(COALESCE(l.interest, 0), 0) AS interest,
    GREATEST(COALESCE(l.orig_fee, 0), 0) AS admin_fee,
    0 AS service_fee,
    0 AS tax_fee,
    GREATEST(COALESCE(l.penalty, 0), 0) AS penalty_amount,
    0 AS reduction_amount,
    GREATEST(COALESCE(l.amt, 0), 0) AS total_amount,
    CASE WHEN l.rp_status IN (2, 4) THEN GREATEST(COALESCE(l.repaid_amt, 0), 0) ELSE 0 END AS paid_amount,
    -- market.application.paidTime * 100（见 loan_incr_bundle_lookup.market_paid_time）
    CASE WHEN COALESCE(l.market_paid_time, 0) > 0 THEN l.market_paid_time ELSE CAST(NULL AS BIGINT) END AS paid_time,
    CASE WHEN COALESCE(l.settle_time, 0) > 0
        THEN DATE_FORMAT(FROM_UNIXTIME(CASE WHEN l.settle_time > 10000000000 THEN l.settle_time / 1000 ELSE l.settle_time END), 'yyyy-MM-dd')
        ELSE CAST(NULL AS STRING) END AS paid_off_date,
    CAST(UNIX_TIMESTAMP(CAST(l.created_at AS STRING)) * 1000 AS BIGINT) AS created_time,
    CAST(CASE
        WHEN l.rp_status = 1 AND COALESCE(l.repaid_amt, 0) = 0 THEN 20
        WHEN l.rp_status = 1 AND COALESCE(l.repaid_amt, 0) <> 0 THEN 24
        WHEN l.rp_status = 3 THEN 23
        WHEN l.rp_status = 4 THEN 25
        WHEN l.rp_status = 2 THEN 27
        ELSE l.rp_status
    END AS INT) AS status
FROM v_loan_triggers AS t
INNER JOIN dim_loan_bundle FOR SYSTEM_TIME AS OF t.proc_time AS l
    ON l.sn = t.sn
WHERE l.application_no IS NOT NULL AND TRIM(l.application_no) <> '';
