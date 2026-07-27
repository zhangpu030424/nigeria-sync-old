-- 增量 user_info：多源 CDC + bundle Lookup
CREATE TEMPORARY FUNCTION vt_tokenize AS 'com.nigeria.flink.udf.VtTokenizeFunction';
CREATE TEMPORARY FUNCTION vt_format_emergency_contacts AS 'com.nigeria.flink.udf.VtTokenizeEmergencyContactsFunction';

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
    user_id DECIMAL(20, 0), full_name STRING, id_number_token STRING, password STRING,
    registration_ip STRING, install_channel STRING, app_name STRING, app_id BIGINT, reg_time BIGINT,
    bvn_raw STRING,
    email STRING, birthday STRING, gender BIGINT,
    addressState STRING, addressDistrict STRING, address STRING,
    company STRING, education BIGINT, marital BIGINT, profession STRING, salary STRING,
    emergencyContact STRING, numberOfChildren BIGINT, payCycle BIGINT, salaryDay BIGINT,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MARKET_MYSQL_HOST}:${MARKET_MYSQL_PORT}/${MARKET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_info_incr_bundle_lookup',
    'username' = '${MARKET_MYSQL_USER}', 'password' = '${MARKET_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '100000', 'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL_USER_INFO}'
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
SELECT CAST(id AS DECIMAL(20, 0)) AS user_id, proc_time FROM cdc_user WHERE id IS NOT NULL
UNION ALL
SELECT CAST(`userId` AS DECIMAL(20, 0)) AS user_id, proc_time FROM cdc_user_data WHERE `userId` IS NOT NULL
UNION ALL
SELECT CAST(`userId` AS DECIMAL(20, 0)) AS user_id, proc_time FROM cdc_uri WHERE `userId` IS NOT NULL;

-- JSON 标量字符串：空白 → null；转义双引号与反斜杠
-- 结构对齐 ng_migration_run._build_user_info_json；emergency_contacts 由 UDF 从源库元组转对象并 VT
-- 无 user_data 资料时不写目标，避免注册瞬间空壳 + Lookup 缓存把后续补数盖住
INSERT INTO sink_user_info
SELECT
    CAST(b.user_id AS BIGINT),
    COALESCE(
        NULLIF(TRIM(b.id_number_token), ''),
        CASE WHEN b.bvn_raw IS NOT NULL AND TRIM(b.bvn_raw) <> '' THEN vt_tokenize(TRIM(b.bvn_raw)) ELSE '' END
    ) AS id_number,
    COALESCE(NULLIF(TRIM(b.full_name), ''), '') AS full_name,
    b.password,
    '' AS live_image,
    '' AS id_card,
    CONCAT(
        '{',
        '"full_name":', CASE WHEN b.full_name IS NULL OR TRIM(b.full_name) = '' THEN 'null'
            ELSE CONCAT('"', REPLACE(REPLACE(TRIM(b.full_name), '\\', '\\\\'), '"', '\\"'), '"') END,
        ',"email":', CASE WHEN b.email IS NULL OR TRIM(b.email) = '' THEN 'null'
            ELSE CONCAT('"', REPLACE(REPLACE(TRIM(b.email), '\\', '\\\\'), '"', '\\"'), '"') END,
        ',"birthday":', CASE WHEN b.birthday IS NULL OR TRIM(b.birthday) = '' THEN 'null'
            ELSE CONCAT('"', REPLACE(REPLACE(TRIM(b.birthday), '\\', '\\\\'), '"', '\\"'), '"') END,
        ',"gender":', CASE WHEN b.gender IS NULL THEN 'null' ELSE CAST(b.gender AS STRING) END,
        ',"id_card":null',
        ',"live_image":null',
        ',"face_similarity":null',
        ',"address":{',
            '"province":', CASE WHEN b.addressState IS NULL OR TRIM(b.addressState) = '' THEN 'null'
                ELSE CONCAT('"', REPLACE(REPLACE(TRIM(b.addressState), '\\', '\\\\'), '"', '\\"'), '"') END,
            ',"city":', CASE WHEN b.addressDistrict IS NULL OR TRIM(b.addressDistrict) = '' THEN 'null'
                ELSE CONCAT('"', REPLACE(REPLACE(TRIM(b.addressDistrict), '\\', '\\\\'), '"', '\\"'), '"') END,
            ',"district":null',
            ',"village":null',
            ',"detail":', CASE WHEN b.address IS NULL OR TRIM(b.address) = '' THEN 'null'
                ELSE CONCAT('"', REPLACE(REPLACE(TRIM(b.address), '\\', '\\\\'), '"', '\\"'), '"') END,
        '}',
        ',"company":', CASE WHEN b.company IS NULL OR TRIM(b.company) = '' THEN 'null'
            ELSE CONCAT('"', REPLACE(REPLACE(TRIM(b.company), '\\', '\\\\'), '"', '\\"'), '"') END,
        ',"education":', CASE WHEN b.education IS NULL THEN 'null' ELSE CAST(b.education AS STRING) END,
        ',"loan_purpose":null',
        ',"marital":', CASE WHEN b.marital IS NULL THEN 'null' ELSE CAST(b.marital AS STRING) END,
        ',"job_type":null',
        ',"profession":', CASE WHEN b.profession IS NULL OR TRIM(b.profession) = '' THEN 'null'
            ELSE CONCAT('"', REPLACE(REPLACE(TRIM(b.profession), '\\', '\\\\'), '"', '\\"'), '"') END,
        ',"religion":null',
        ',"salary":', CASE WHEN b.salary IS NULL OR TRIM(b.salary) = '' THEN 'null'
            ELSE CONCAT('"', REPLACE(REPLACE(TRIM(b.salary), '\\', '\\\\'), '"', '\\"'), '"') END,
        ',"emergency_contacts":', vt_format_emergency_contacts(COALESCE(b.emergencyContact, '')),
        ',"registration_ip":', CASE WHEN b.registration_ip IS NULL OR TRIM(b.registration_ip) = '' THEN 'null'
            ELSE CONCAT('"', REPLACE(REPLACE(TRIM(b.registration_ip), '\\', '\\\\'), '"', '\\"'), '"') END,
        ',"registration_time":', CASE WHEN b.reg_time IS NULL OR b.reg_time = 0 THEN 'null' ELSE CAST(b.reg_time AS STRING) END,
        ',"children_num":', CASE WHEN b.numberOfChildren IS NULL THEN 'null' ELSE CAST(b.numberOfChildren AS STRING) END,
        ',"pay_cycle":', CASE WHEN b.payCycle IS NULL THEN 'null' ELSE CAST(b.payCycle AS STRING) END,
        ',"salary_day":', CASE WHEN b.salaryDay IS NULL THEN 'null' ELSE CAST(b.salaryDay AS STRING) END,
        ',"survey":{"survey_loan_cnt":null,"survey_outstanding_cnt":null,"survey_overdue_max_days":null,"survey_overdue_6m":null,"survey_loan_amt_total":null}',
        ',"app":{',
            '"name":', CASE WHEN b.app_name IS NULL OR TRIM(b.app_name) = '' THEN 'null'
                ELSE CONCAT('"', REPLACE(REPLACE(TRIM(b.app_name), '\\', '\\\\'), '"', '\\"'), '"') END,
            ',"app_id":', CASE WHEN b.app_id IS NULL THEN 'null' ELSE CONCAT('"', CAST(b.app_id AS STRING), '"') END,
            ',"version":null',
        '}',
        ',"install_source":', CASE WHEN b.install_channel IS NULL OR TRIM(b.install_channel) = '' THEN 'null'
            ELSE CONCAT('"', REPLACE(REPLACE(TRIM(b.install_channel), '\\', '\\\\'), '"', '\\"'), '"') END,
        ',"credit_limit":null',
        '}'
    ) AS info
FROM v_ui_triggers AS t
INNER JOIN dim_user_info_bundle FOR SYSTEM_TIME AS OF t.proc_time AS b
    ON b.user_id = t.user_id
WHERE (
    (b.full_name IS NOT NULL AND TRIM(b.full_name) <> '')
    OR (b.email IS NOT NULL AND TRIM(b.email) <> '')
    OR (b.bvn_raw IS NOT NULL AND TRIM(b.bvn_raw) <> '')
);
