#!/usr/bin/env bash
# cron 入口：默认按表顺序 apply 已有 plan（与 apply_reconcile_plans.sh 一致）
#
# 环境变量：
#   MODE=apply|all     默认 apply（用最新 dated plan 写库）；all=完整 load+plan+apply
#   PLAN_DATE          apply 时默认 latest；all 时默认当天 YYYYMMDD（写出新 plan）
#   TABLES / START_TABLE / FILTER_DATES / APPLY_* 等同 apply_reconcile_plans.sh
#   DRY_RUN / SINCE_DATE / FROM_CACHE 等在 MODE=all 时传给 run_reconcile_all.sh
#   LOCK_FILE          默认 /tmp/reconcile_cron.lock
#   CRON_LOG           默认 /tmp/reconcile_logs/reconcile_cron.log
#
# crontab 示例（每天 12:00）：
#   0 12 * * * /opt/ng-migration-old/nigeria-sync-old/run_reconcile_cron.sh
#
# 或用 ./install_reconcile_cron.sh 安装
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

LOG_DIR="${LOG_DIR:-/tmp/reconcile_logs}"
CRON_LOG="${CRON_LOG:-$LOG_DIR/reconcile_cron.log}"
LOCK_FILE="${LOCK_FILE:-/tmp/reconcile_cron.lock}"
MODE="${MODE:-apply}"

mkdir -p "$LOG_DIR"

# 同时写入 cron 总日志（crontab 也可自行 redirect）
exec >>"$CRON_LOG" 2>&1

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

# flock：已有任务在跑则直接退出（不叠跑）
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  log "skip: another reconcile cron is running (lock=$LOCK_FILE)"
  exit 0
fi

log "======== reconcile cron start MODE=$MODE PWD=$HERE ========"

case "$MODE" in
  apply)
    # 默认用最新 dated plan；可用 PLAN_DATE=20260714 指定
    export PLAN_DATE="${PLAN_DATE:-latest}"
    export TABLES="${TABLES:-user user_info user_bankcard user_product application loan}"
    export START_TABLE="${START_TABLE:-user}"
    export FILTER_DATES="${FILTER_DATES:-1}"
    export APPLY_WORKERS="${APPLY_WORKERS:-24}"
    export APPLY_BATCH="${APPLY_BATCH:-1000}"
    "$HERE/apply_reconcile_plans.sh"
    ;;
  all)
    # 全量对账：load → plan → apply；新 plan 落盘为当天日期
    export PLAN_DATE="${PLAN_DATE:-$(date +%Y%m%d)}"
    export DRY_RUN="${DRY_RUN:-0}"
    "$HERE/run_reconcile_all.sh"
    ;;
  *)
    log "unknown MODE=$MODE (use apply|all)"
    exit 1
    ;;
esac

rc=$?
log "======== reconcile cron finished rc=$rc ========"
exit "$rc"
