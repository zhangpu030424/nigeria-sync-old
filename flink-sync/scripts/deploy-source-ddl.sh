#!/usr/bin/env bash
# 部署 ng_loan_market Lookup 视图（增量 Job 前置）
# 用法:
#   ./scripts/deploy-source-ddl.sh
#   ./scripts/deploy-source-ddl.sh --force
set -euo pipefail
cd "$(dirname "$0")/.."

FORCE=0
for arg in "$@"; do
  case "$arg" in
    --force) FORCE=1 ;;
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

echo ">> 部署 Lookup 视图到 ${MARKET_MYSQL_USER}@${MARKET_MYSQL_HOST}/${MARKET_MYSQL_DATABASE}"
mysql_market_cmd < "$DDL"
echo ">> 完成"
