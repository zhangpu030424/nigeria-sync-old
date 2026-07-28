#!/usr/bin/env bash
# 提交全部增量 Job（实时同步入口）
#
# 用法:
#   ./scripts/sync-incr-auto.sh
#   ./scripts/sync-incr-auto.sh --jobs user,application
#   ./scripts/sync-incr-auto.sh --startup-mode latest-offset
#   ./scripts/sync-incr-auto.sh --keep-jobs
#   ./scripts/sync-incr-auto.sh --bulk-start-ms 1781240247171
set -euo pipefail
cd "$(dirname "$0")/.."

CANCEL_JOBS=1
SKIP_DDL=0
JOBS_FILTER=""
BULK_START_MS_ARG=""
STARTUP_MODE="${CDC_STARTUP_MODE:-latest-offset}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --jobs=*) JOBS_FILTER="${1#--jobs=}" ;;
    --jobs) shift; JOBS_FILTER="${1:-}" ;;
    --bulk-start-ms=*) BULK_START_MS_ARG="${1#--bulk-start-ms=}" ;;
    --bulk-start-ms) shift; BULK_START_MS_ARG="${1:-}" ;;
    --startup-mode=*) STARTUP_MODE="${1#--startup-mode=}" ;;
    --startup-mode) shift; STARTUP_MODE="${1:-latest-offset}" ;;
    --skip-ddl) SKIP_DDL=1 ;;
    --keep-jobs) CANCEL_JOBS=0 ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) echo "未知参数: $1"; exit 1 ;;
  esac
  shift
done

[[ -f .env ]] || { echo "ERR: cp .env.example .env"; exit 1; }
# shellcheck source=scripts/lib/load-project-env.sh
source scripts/lib/load-project-env.sh
load_project_env "$(pwd)"
# shellcheck source=scripts/lib/bulk-start-ms.sh
source scripts/lib/bulk-start-ms.sh
# shellcheck source=scripts/lib/sync-jobs.sh
source scripts/lib/sync-jobs.sh

# bulk-start 用 market 源库时钟
export SOURCE_MYSQL_HOST="${MARKET_MYSQL_HOST}"
export SOURCE_MYSQL_PORT="${MARKET_MYSQL_PORT}"
export SOURCE_MYSQL_USER="${MARKET_MYSQL_USER}"
export SOURCE_MYSQL_PASSWORD="${MARKET_MYSQL_PASSWORD}"

sync_jobs_load "${JOBS_FILTER}"

echo "=========================================="
echo "尼日贷超增量同步 sync-incr-auto"
echo "  Jobs: ${SYNC_ENABLED_JOBS[*]}"
echo "  CDC_STARTUP_MODE=${STARTUP_MODE}"
echo "=========================================="

resolve_bulk_start_ms "$BULK_START_MS_ARG"
export CDC_STARTUP_TIMESTAMP_MILLIS="${BULK_START_MS}"
echo ">> bulk-start-ms=${BULK_START_MS} → CDC_STARTUP_TIMESTAMP_MILLIS"

if [[ "$SKIP_DDL" -eq 0 ]]; then
  echo ">> [1] 部署源库 Lookup 视图"
  ./scripts/deploy-source-ddl.sh
else
  echo ">> [1] 跳过 DDL"
fi

if [[ "$CANCEL_JOBS" -eq 1 ]]; then
  echo ">> [2] Cancel 存量 Job"
  bash scripts/cancel-flink-jobs.sh --yes || true
else
  echo ">> [2] 保留存量 Job"
fi

CONF="config/sync-jobs.conf"
echo ">> [3] 提交增量 Job"
for job in "${SYNC_ENABLED_JOBS[@]}"; do
  line=""
  while IFS= read -r row || [[ -n "$row" ]]; do
    row="${row%%#*}"
    row="$(echo "$row" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ -z "$row" ]] && continue
    [[ "$row" == "$job|"* ]] && line="$row" && break
  done < "$CONF"
  [[ -n "$line" ]] || { echo "ERR: 未知 job $job"; exit 1; }
  sync_job_parse_line "$line"
  INCR_SQL="$SYNC_JOB_INCR_SQL"
  [[ -f "$INCR_SQL" ]] || { echo "ERR: 缺少 $INCR_SQL"; exit 1; }

  par=$(sync_job_parallelism "$job" incr)
  export FLINK_PARALLELISM="$par"
  export CDC_STARTUP_MODE="$STARTUP_MODE"
  if [[ "$STARTUP_MODE" == "timestamp" ]]; then
    export CDC_STARTUP_TIMESTAMP_MILLIS="${BULK_START_MS}"
  fi

  echo ""
  echo "########################################"
  echo "# incr: ${job}  parallelism=${par}  mode=${CDC_STARTUP_MODE}"
  echo "########################################"
  ./scripts/run-sql.sh "$INCR_SQL"
  sleep 2
done

echo ""
echo "=========================================="
echo "增量 Job 已提交"
JM="${FLINK_JOBMANAGER_CONTAINER:-ng-sync-flink-jobmanager}"
docker exec "$JM" ./bin/flink list -r 2>/dev/null || true
echo "Web UI: http://<host>:${FLINK_WEB_PORT:-8090}"
echo "兜底对账: cd .. && SINCE_DAYS=7 ./run_reconcile_cron.sh"
echo "=========================================="
