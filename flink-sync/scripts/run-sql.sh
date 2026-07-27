#!/usr/bin/env bash
# 读取 .env 替换占位符，在 JobManager 容器内执行 Flink SQL
# 用法: bash scripts/run-sql.sh sql/02_sync_user_incr.sql
set -euo pipefail
cd "$(dirname "$0")/.."

[[ -f .env ]] || { echo "请先: cp .env.example .env"; exit 1; }

SQL_FILE="${1:-}"
if [[ -z "$SQL_FILE" || ! -f "$SQL_FILE" ]]; then
  echo "用法: $0 <sql文件路径>"
  exit 1
fi

# shellcheck source=scripts/lib/load-project-env.sh
source scripts/lib/load-project-env.sh
load_project_env "$(pwd)"

# 强制注入 Flink 运行参数（避免 .env 空值或父 shell 未 export 导致 envsubst 留空）
export FLINK_PARALLELISM="${FLINK_PARALLELISM:-4}"
export FLINK_MINI_BATCH_SIZE="${FLINK_MINI_BATCH_SIZE:-5000}"
export FLINK_SINK_BUFFER_ROWS="${FLINK_SINK_BUFFER_ROWS:-10000}"
export FLINK_CDC_CHUNK_SIZE="${FLINK_CDC_CHUNK_SIZE:-200000}"
export FLINK_CDC_FETCH_SIZE="${FLINK_CDC_FETCH_SIZE:-20000}"
export FLINK_CHECKPOINT_INTERVAL="${FLINK_CHECKPOINT_INTERVAL:-300s}"
export FLINK_CHECKPOINT_TIMEOUT="${FLINK_CHECKPOINT_TIMEOUT:-1800s}"
export CDC_STARTUP_MODE="${CDC_STARTUP_MODE:-initial}"
export CDC_STARTUP_TIMESTAMP_MILLIS="${CDC_STARTUP_TIMESTAMP_MILLIS:-0}"
export TARGET_JDBC_PARAMS="${TARGET_JDBC_PARAMS:-useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true&autoReconnect=true&maxReconnects=10&connectTimeout=30000&socketTimeout=0&tcpKeepAlive=true}"
export FLINK_SINK_MAX_RETRIES="${FLINK_SINK_MAX_RETRIES:-15}"
export FLINK_JDBC_RETRY_TIMEOUT="${FLINK_JDBC_RETRY_TIMEOUT:-120s}"
export LOOKUP_CACHE_TTL="${LOOKUP_CACHE_TTL:-30s}"

_par="${FLINK_PARALLELISM:-4}"
_sid="${CDC_SERVER_ID_SPAN:-$_par}"
if [[ "$_sid" -lt "$_par" ]]; then _sid="$_par"; fi

export CDC_SERVER_ID_USER="${CDC_SERVER_ID_USER:-6101-$((6100 + _sid))}"
export CDC_SERVER_ID_LUP="${CDC_SERVER_ID_LUP:-6121-$((6120 + _sid))}"
export CDC_SERVER_ID_DAC="${CDC_SERVER_ID_DAC:-6141-$((6140 + _sid))}"
export CDC_SERVER_ID_USER_DATA="${CDC_SERVER_ID_USER_DATA:-6161-$((6160 + _sid))}"
export CDC_SERVER_ID_URI="${CDC_SERVER_ID_URI:-6181-$((6180 + _sid))}"
export CDC_SERVER_ID_UI_USER="${CDC_SERVER_ID_UI_USER:-6201-$((6200 + _sid))}"
export CDC_SERVER_ID_BANKCARD="${CDC_SERVER_ID_BANKCARD:-6221-$((6220 + _sid))}"
export CDC_SERVER_ID_USER_PRODUCT="${CDC_SERVER_ID_USER_PRODUCT:-6241-$((6240 + _sid))}"
export CDC_SERVER_ID_APP_MARKET="${CDC_SERVER_ID_APP_MARKET:-6261-$((6260 + _sid))}"
export CDC_SERVER_ID_APP_CORE="${CDC_SERVER_ID_APP_CORE:-6281-$((6280 + _sid))}"
export CDC_SERVER_ID_LOAN_PLAN="${CDC_SERVER_ID_LOAN_PLAN:-6301-$((6300 + _sid))}"
export CDC_SERVER_ID_LOAN_REPAY="${CDC_SERVER_ID_LOAN_REPAY:-6321-$((6320 + _sid))}"
export CDC_SERVER_ID_LOAN_MARKET_APP="${CDC_SERVER_ID_LOAN_MARKET_APP:-6341-$((6340 + _sid))}"
# application 也订阅 user_data，必须与 user_info 的 CDC_SERVER_ID_USER_DATA 错开
export CDC_SERVER_ID_APP_USER_DATA="${CDC_SERVER_ID_APP_USER_DATA:-6361-$((6360 + _sid))}"
export LOOKUP_CACHE_TTL_USER_INFO="${LOOKUP_CACHE_TTL_USER_INFO:-5s}"

VARS='${MARKET_MYSQL_HOST} ${MARKET_MYSQL_PORT} ${MARKET_MYSQL_USER} ${MARKET_MYSQL_PASSWORD} ${MARKET_MYSQL_DATABASE} ${CORE_MYSQL_HOST} ${CORE_MYSQL_PORT} ${CORE_MYSQL_USER} ${CORE_MYSQL_PASSWORD} ${CORE_MYSQL_DATABASE} ${TARGET_MYSQL_HOST} ${TARGET_MYSQL_PORT} ${TARGET_MYSQL_USER} ${TARGET_MYSQL_PASSWORD} ${TARGET_MYSQL_DATABASE} ${TARGET_JDBC_PARAMS} ${FLINK_PARALLELISM} ${FLINK_MINI_BATCH_SIZE} ${FLINK_SINK_BUFFER_ROWS} ${FLINK_SINK_MAX_RETRIES} ${FLINK_JDBC_RETRY_TIMEOUT} ${FLINK_CDC_CHUNK_SIZE} ${FLINK_CDC_FETCH_SIZE} ${FLINK_CHECKPOINT_INTERVAL} ${FLINK_CHECKPOINT_TIMEOUT} ${CDC_STARTUP_MODE} ${CDC_STARTUP_TIMESTAMP_MILLIS} ${LOOKUP_CACHE_TTL} ${LOOKUP_CACHE_TTL_USER_INFO} ${CDC_SERVER_ID_USER} ${CDC_SERVER_ID_LUP} ${CDC_SERVER_ID_DAC} ${CDC_SERVER_ID_USER_DATA} ${CDC_SERVER_ID_URI} ${CDC_SERVER_ID_UI_USER} ${CDC_SERVER_ID_BANKCARD} ${CDC_SERVER_ID_USER_PRODUCT} ${CDC_SERVER_ID_APP_MARKET} ${CDC_SERVER_ID_APP_CORE} ${CDC_SERVER_ID_LOAN_PLAN} ${CDC_SERVER_ID_LOAN_REPAY} ${CDC_SERVER_ID_LOAN_MARKET_APP} ${CDC_SERVER_ID_APP_USER_DATA}'

PREPARED="/tmp/ng-sync-flink-run-$$.sql"
envsubst "$VARS" < "$SQL_FILE" > "$PREPARED"

CONTAINER="${FLINK_JOBMANAGER_CONTAINER:-ng-sync-flink-jobmanager}"
REMOTE="/tmp/ng-sync-flink-run.sql"

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  echo ">> ERR: JobManager [$CONTAINER] 未运行，请先 ./scripts/up.sh"
  rm -f "$PREPARED"
  exit 1
fi

echo ">> 执行: $SQL_FILE (parallelism=${FLINK_PARALLELISM} mini_batch=${FLINK_MINI_BATCH_SIZE})"
grep -E "^SET 'parallelism|^SET 'table.exec.mini-batch|^SET 'execution.checkpointing.interval" "$PREPARED" 2>/dev/null | head -6 || true
docker cp "$PREPARED" "${CONTAINER}:${REMOTE}"
SQL_LOG="$(mktemp)"
trap 'rm -f "$PREPARED" "$SQL_LOG"' EXIT
docker exec "$CONTAINER" ./bin/sql-client.sh -D "parallelism.default=${FLINK_PARALLELISM}" -f "$REMOTE" \
  2>&1 | tee "$SQL_LOG"
rm -f "$PREPARED"
PREPARED=""

FLINK_JOB_ID="$(sed -n 's/.*Job ID: \([a-f0-9]\{32\}\).*/\1/p' "$SQL_LOG" | tail -1)"
mkdir -p logs
if [[ -n "$FLINK_JOB_ID" ]]; then
  echo "$FLINK_JOB_ID" > logs/last-flink-job-id
  echo ">> FLINK_JOB_ID=${FLINK_JOB_ID}"
fi
rm -f "$SQL_LOG"
trap - EXIT
echo ">> 完成"
