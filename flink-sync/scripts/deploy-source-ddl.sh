#!/usr/bin/env bash
# 部署 ng_loan_market Lookup 视图（增量 Job 前置）
# 用法:
#   ./scripts/deploy-source-ddl.sh
#   ./scripts/deploy-source-ddl.sh --force
set -euo pipefail
cd "$(dirname "$0")/.."

FORCE=0
FORCE_INDEXES=0
for arg in "$@"; do
  case "$arg" in
    --force) FORCE=1 ;;
    --force-indexes) FORCE_INDEXES=1 ;;
    -h|--help)
      echo "用法: $0 [--force]"
      exit 0
      ;;
  esac
done

[[ -f .env ]] || { echo "请先: cp .env.example .env"; exit 1; }
# shellcheck source=scripts/lib/load-project-env.sh
source scripts/lib/load-project-env.sh
load_project_env "$(pwd)"
# shellcheck source=scripts/lib/mysql-market.sh
source scripts/lib/mysql-market.sh

DDL="sql/ddl/market_lookup_views.sql"
[[ -f "$DDL" ]] || { echo "ERR: 缺少 $DDL"; exit 1; }

if [[ "$FORCE" -eq 0 && "${DEPLOY_SOURCE_DDL_SKIP_IF_OK:-1}" == "1" ]]; then
  cnt=$(mysql_market_query \
    "SELECT COUNT(*) FROM information_schema.views WHERE table_schema='${MARKET_MYSQL_DATABASE}' AND table_name='user_incr_lookup';" \
    2>/dev/null || echo "0")
  if [[ "${cnt:-0}" -ge 1 ]]; then
    echo ">> Lookup 视图已存在，跳过（--force 强制重建）"
    exit 0
  fi
fi

if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "${FLINK_JOBMANAGER_CONTAINER:-ng-sync-flink-jobmanager}"; then
  running=$(docker exec "${FLINK_JOBMANAGER_CONTAINER:-ng-sync-flink-jobmanager}" ./bin/flink list 2>/dev/null \
    | grep -cE 'RUNNING|RESTARTING|CANCELLING' || true)
  if [[ "${running:-0}" -gt 0 ]]; then
    echo "WARN: 仍有 ${running} 个 Flink Job 在跑，CREATE VIEW 可能等 MDL 锁。建议先:"
    echo "  bash scripts/cancel-flink-jobs.sh --yes --keep-checkpoints"
  fi
fi

echo ">> 部署 Lookup 视图到 ${MARKET_MYSQL_USER}@${MARKET_MYSQL_HOST}/${MARKET_MYSQL_DATABASE}"
if [[ "$FORCE_INDEXES" -eq 1 || "$FORCE" -eq 1 ]] && [[ -f sql/ddl/market_lookup_indexes.sql ]]; then
  echo ">> 部署 Lookup 索引/生成列（id_signed）"
  mysql_market_cmd --init-command="SET SESSION lock_wait_timeout=${SOURCE_DDL_LOCK_WAIT_TIMEOUT:-120}" \
    < sql/ddl/market_lookup_indexes.sql || echo "WARN: market_lookup_indexes.sql 未完整（可能无 ALTER 权限）"
fi
mysql_market_cmd --init-command="SET SESSION lock_wait_timeout=${SOURCE_DDL_LOCK_WAIT_TIMEOUT:-120}" < "$DDL"
echo ">> 完成"
