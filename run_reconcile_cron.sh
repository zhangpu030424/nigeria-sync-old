#!/usr/bin/env bash
# 对账入口：单次或循环跑（上一轮结束后立刻开下一轮）
#
# 环境变量：
#   MODE=apply|all     默认 apply；循环对账请用 MODE=all
#   SINCE_DAYS         默认 30；MODE=all 时若未显式设 SINCE_DATE，则用「今天-N天」
#   SINCE_DATE         显式覆盖窗口起点（优先级高于 SINCE_DAYS）
#   LOOP=1             循环：一轮结束后马上开下一轮（默认 0=只跑一次）
#   SLEEP_SECS         两轮之间额外休眠秒数，默认 0
#   FAIL_SLEEP_SECS    失败后休眠秒数再重试，默认 60（仅 LOOP=1）
#   PLAN_DATE / TABLES / START_TABLE / FILTER_DATES / APPLY_* / DRY_RUN / FROM_CACHE ...
#   LOCK_FILE          默认 /tmp/reconcile_cron.lock
#   CRON_LOG           默认 /tmp/reconcile_logs/reconcile_cron.log
#
# 最近一个月 + 一直跑（推荐 nohup，不要用每天定点 cron）:
#   nohup env MODE=all LOOP=1 SINCE_DAYS=30 ./run_reconcile_cron.sh >/dev/null 2>&1 &
#
# 只跑一轮（最近 30 天）:
#   MODE=all SINCE_DAYS=30 ./run_reconcile_cron.sh
#
# 机器重启后自动拉起（可选，crontab）:
#   @reboot cd /opt/ng-migration-old/nigeria-sync-old && MODE=all LOOP=1 SINCE_DAYS=30 ./run_reconcile_cron.sh
#
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

LOG_DIR="${LOG_DIR:-/tmp/reconcile_logs}"
CRON_LOG="${CRON_LOG:-$LOG_DIR/reconcile_cron.log}"
LOCK_FILE="${LOCK_FILE:-/tmp/reconcile_cron.lock}"
MODE="${MODE:-apply}"
LOOP="${LOOP:-0}"
SLEEP_SECS="${SLEEP_SECS:-0}"
FAIL_SLEEP_SECS="${FAIL_SLEEP_SECS:-60}"
SINCE_DAYS="${SINCE_DAYS:-30}"

mkdir -p "$LOG_DIR"

# 同时写入 cron 总日志
exec >>"$CRON_LOG" 2>&1

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

days_ago_ymd() {
  local days="$1"
  if date -d "${days} days ago" +%F >/dev/null 2>&1; then
    date -d "${days} days ago" +%F
  elif date -v-"${days}"d +%F >/dev/null 2>&1; then
    date -v-"${days}"d +%F
  else
    # 兜底：python
    python3 -c "from datetime import date,timedelta; print((date.today()-timedelta(days=int('${days}'))).isoformat())"
  fi
}

resolve_since_date() {
  # 显式 SINCE_DATE 优先；否则按 SINCE_DAYS 滚动窗口
  if [[ -n "${SINCE_DATE:-}" ]]; then
    echo "$SINCE_DATE"
    return
  fi
  days_ago_ymd "$SINCE_DAYS"
}

run_once() {
  local since
  case "$MODE" in
    apply)
      export PLAN_DATE="${PLAN_DATE:-latest}"
      export TABLES="${TABLES:-user user_info user_bankcard user_product application loan}"
      export START_TABLE="${START_TABLE:-user}"
      export FILTER_DATES="${FILTER_DATES:-1}"
      export APPLY_WORKERS="${APPLY_WORKERS:-24}"
      export APPLY_BATCH="${APPLY_BATCH:-1000}"
      log "round MODE=apply PLAN_DATE=$PLAN_DATE"
      "$HERE/apply_reconcile_plans.sh"
      ;;
    all)
      since="$(resolve_since_date)"
      export SINCE_DATE="$since"
      export PLAN_DATE="${PLAN_DATE:-$(date +%Y%m%d)}"
      export DRY_RUN="${DRY_RUN:-0}"
      # 循环跑时每轮从 user 开始，避免 START_TABLE 残留
      if [[ "$LOOP" == "1" ]]; then
        export START_TABLE="${START_TABLE:-user}"
      else
        export START_TABLE="${START_TABLE:-user}"
      fi
      log "round MODE=all SINCE_DATE=$SINCE_DATE SINCE_DAYS=$SINCE_DAYS PLAN_DATE=$PLAN_DATE"
      "$HERE/run_reconcile_all.sh"
      ;;
    *)
      log "unknown MODE=$MODE (use apply|all)"
      return 1
      ;;
  esac
}

# flock：已有任务在跑则直接退出（循环模式整段持锁）
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  log "skip: another reconcile cron is running (lock=$LOCK_FILE)"
  exit 0
fi

log "======== reconcile cron start MODE=$MODE LOOP=$LOOP SINCE_DAYS=$SINCE_DAYS PWD=$HERE ========"

round=0
rc=0
while true; do
  round=$((round + 1))
  log "-------- round=$round begin --------"
  t0=$(date +%s)
  set +e
  run_once
  rc=$?
  set -e
  elapsed=$(( $(date +%s) - t0 ))
  log "-------- round=$round end rc=$rc elapsed=${elapsed}s --------"

  if [[ "$LOOP" != "1" ]]; then
    break
  fi

  if [[ "$rc" -ne 0 ]]; then
    log "round failed; sleep ${FAIL_SLEEP_SECS}s then retry"
    sleep "$FAIL_SLEEP_SECS"
  elif [[ "$SLEEP_SECS" -gt 0 ]]; then
    log "sleep ${SLEEP_SECS}s before next round"
    sleep "$SLEEP_SECS"
  else
    log "restart next round immediately"
  fi
done

log "======== reconcile cron finished rounds=$round last_rc=$rc ========"
exit "$rc"
