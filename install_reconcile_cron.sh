#!/usr/bin/env bash
# 安装 / 更新 crontab 条目，调用 run_reconcile_cron.sh
#
# Usage:
#   ./install_reconcile_cron.sh                  # 默认每天 12:00，MODE=apply
#   ./install_reconcile_cron.sh --schedule '0 3 * * *'
#   MODE=all ./install_reconcile_cron.sh         # 定时全量对账
#   ./install_reconcile_cron.sh --remove         # 删除本脚本写入的条目
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CRON_SH="$HERE/run_reconcile_cron.sh"
MARKER="# nigeria-sync-reconcile-cron"
SCHEDULE="0 12 * * *"
DO_REMOVE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --schedule)
      SCHEDULE="${2:?}"
      shift 2
      ;;
    --remove)
      DO_REMOVE=1
      shift
      ;;
    -h|--help)
      sed -n '2,12p' "$0"
      exit 0
      ;;
    *)
      echo "unknown arg: $1" >&2
      exit 1
      ;;
  esac
done

if [[ ! -x "$CRON_SH" ]]; then
  chmod +x "$CRON_SH" "$HERE/apply_reconcile_plans.sh" "$HERE/run_reconcile_all.sh" 2>/dev/null || true
fi
chmod +x "$CRON_SH"

MODE_PREFIX=""
if [[ -n "${MODE:-}" ]]; then
  MODE_PREFIX="MODE=${MODE} "
fi
# 可选透传常用变量到 cron 行
ENV_PREFIX="$MODE_PREFIX"
for k in TABLES START_TABLE FILTER_DATES APPLY_WORKERS APPLY_BATCH DRY_RUN SINCE_DATE LOG_DIR PLAN_DATE; do
  if [[ -n "${!k:-}" ]]; then
    ENV_PREFIX+="${k}=${!k} "
  fi
done

LINE="${SCHEDULE} ${ENV_PREFIX}${CRON_SH} ${MARKER}"

existing="$(crontab -l 2>/dev/null || true)"
# 去掉旧标记行
filtered="$(printf '%s\n' "$existing" | grep -vF "$MARKER" || true)"

if [[ "$DO_REMOVE" == "1" ]]; then
  trimmed="$(printf '%s' "$filtered" | tr -d '[:space:]')"
  if [[ -z "$trimmed" ]]; then
    crontab -r 2>/dev/null || true
  else
    printf '%s\n' "$filtered" | crontab -
  fi
  echo "removed reconcile cron (${MARKER})"
  crontab -l 2>/dev/null || echo "(crontab empty)"
  exit 0
fi

{
  printf '%s\n' "$filtered"
  printf '%s\n' "$LINE"
} | grep -v '^$' | crontab -

echo "installed:"
echo "  $LINE"
echo
echo "current crontab:"
crontab -l
