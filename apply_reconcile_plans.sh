#!/usr/bin/env bash
# 只用已有 /tmp/reconcile_*_plan_YYYYMMDD.jsonl 写库（不再 load/plan）
#
# Usage:
#   ./apply_reconcile_plans.sh
#   PLAN_DATE=20260714 ./apply_reconcile_plans.sh   # 指定某日；默认 latest=取最新 dated
#   START_TABLE=loan ./apply_reconcile_plans.sh
#   FILTER_DATES=1 ./apply_reconcile_plans.sh   # apply 前先清洗 application/loan 假日期 diff
#   TABLES="loan application" ./apply_reconcile_plans.sh
#
# 解析顺序：PLAN_DATE 指定日 → 否则最新 reconcile_{table}_plan_YYYYMMDD.jsonl → 无日期旧文件
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

resolve_python() {
  if [[ -n "${PYTHON:-}" ]]; then
    echo "$PYTHON"
    return
  fi
  local c
  for c in python3.11 python3.10 python3.9 python3.8 python3.7 python3; do
    if command -v "$c" >/dev/null 2>&1; then
      if "$c" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 6) else 1)' 2>/dev/null; then
        echo "$c"
        return
      fi
    fi
  done
  echo "python3"
}

PYTHON="$(resolve_python)"
ENV="${ENV:-$HERE/ng_migration.env}"
LOG_DIR="${LOG_DIR:-/tmp/reconcile_logs}"
APPLY_WORKERS="${APPLY_WORKERS:-24}"
APPLY_BATCH="${APPLY_BATCH:-1000}"
START_TABLE="${START_TABLE:-user}"
FILTER_DATES="${FILTER_DATES:-1}"
PLAN_DATE="${PLAN_DATE:-latest}"
MASTER_LOG="${MASTER_LOG:-$LOG_DIR/reconcile_apply_master.log}"

DEFAULT_TABLES=(user user_info user_bankcard user_product application loan)
if [[ -n "${TABLES:-}" ]]; then
  # shellcheck disable=SC2206
  TABLES_ARR=($TABLES)
else
  TABLES_ARR=("${DEFAULT_TABLES[@]}")
fi

if [[ ! -f "$ENV" ]]; then
  echo "env not found: $ENV" >&2
  exit 1
fi
mkdir -p "$LOG_DIR"

log() {
  local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $*"
  echo "$msg" | tee -a "$MASTER_LOG"
}

resolve_plan() {
  local table="$1"
  local dated legacy latest
  legacy="/tmp/reconcile_${table}_plan.jsonl"

  # 显式指定日期（非 latest）
  if [[ -n "${PLAN_DATE}" && "${PLAN_DATE}" != "latest" ]]; then
    dated="/tmp/reconcile_${table}_plan_${PLAN_DATE}.jsonl"
    if [[ -f "$dated" ]]; then
      echo "$dated"
      return 0
    fi
    if [[ -f "$legacy" ]]; then
      echo "$legacy"
      return 0
    fi
    echo "$dated"
    return 1
  fi

  # 默认：按文件名日期取最新（YYYYMMDD 字典序）
  latest="$(
    ls -1 /tmp/reconcile_"${table}"_plan_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9].jsonl 2>/dev/null \
      | sort | tail -n 1 || true
  )"
  if [[ -n "$latest" && -f "$latest" ]]; then
    echo "$latest"
    return 0
  fi
  if [[ -f "$legacy" ]]; then
    echo "$legacy"
    return 0
  fi
  echo "/tmp/reconcile_${table}_plan_(latest).jsonl"
  return 1
}

filter_one() {
  local table="$1"
  local plan
  plan="$(resolve_plan "$table" || true)"
  if [[ ! -f "$plan" ]]; then
    return 0
  fi
  if [[ "$table" != "loan" && "$table" != "application" ]]; then
    return 0
  fi
  log "${table}: filter false date diffs → ${plan}"
  "$PYTHON" "$HERE/filter_reconcile_plan_false_dates.py" --in "$plan" --inplace
}

apply_one() {
  local table="$1"
  local plan
  if ! plan="$(resolve_plan "$table")"; then
    log "${table}: skip (no plan ${plan} nor legacy undated)"
    return 0
  fi
  if [[ ! -f "$plan" ]]; then
    log "${table}: skip (no plan ${plan})"
    return 0
  fi
  local n
  n=$(wc -l < "$plan" | tr -d ' ')
  if [[ "$n" == "0" ]]; then
    log "${table}: skip (empty plan)"
    return 0
  fi
  log "${table}: APPLY plan=${plan} rows=${n} workers=${APPLY_WORKERS} batch=${APPLY_BATCH}"
  "$PYTHON" "$HERE/reconcile_tables.py" \
    --env "$ENV" \
    --table "$table" \
    --phase apply \
    --apply \
    --plan-file "$plan" \
    --log-dir "$LOG_DIR" \
    --apply-workers "$APPLY_WORKERS" \
    --apply-batch "$APPLY_BATCH"
  log "${table}: APPLY done"
}

log "apply_reconcile_plans start PYTHON=$PYTHON ENV=$ENV START_TABLE=$START_TABLE FILTER_DATES=$FILTER_DATES PLAN_DATE=$PLAN_DATE"
log "APPLY_WORKERS=$APPLY_WORKERS APPLY_BATCH=$APPLY_BATCH tables=${TABLES_ARR[*]}"

started=0
for table in "${TABLES_ARR[@]}"; do
  if [[ "$started" == "0" ]]; then
    if [[ "$table" != "$START_TABLE" ]]; then
      log "skip table=${table} (before START_TABLE=${START_TABLE})"
      continue
    fi
    started=1
  fi
  if [[ "$FILTER_DATES" == "1" ]]; then
    filter_one "$table"
  fi
  apply_one "$table"
done

log "apply_reconcile_plans finished OK"
