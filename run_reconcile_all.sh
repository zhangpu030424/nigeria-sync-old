#!/usr/bin/env bash
# 按业务顺序逐表对账：load-target → plan → apply
#
# 顺序：user → user_info → user_bankcard → user_product → application → loan
#
# Usage:
#   ./run_reconcile_all.sh
#   ENV=./ng_migration.env SINCE_DATE=2026-01-01 ./run_reconcile_all.sh
#   DRY_RUN=1 ./run_reconcile_all.sh          # 只 load + plan，不写库
#   START_TABLE=application ./run_reconcile_all.sh   # 从某表续跑
#
# 环境变量（均可覆盖）：
#   ENV              ng_migration.env 路径
#   SINCE_DATE       源库/目标时间窗口，默认 2026-01-01
#   LOG_DIR          日志目录，默认 /tmp/reconcile_logs
#   LOAD_WORKERS     目标库并行加载线程，默认 8
#   APPLY_WORKERS    apply 并行线程，默认 15
#   APPLY_BATCH      apply 每批行数，默认 500
#   PAGE_SIZE        目标库分页，默认 50000
#   SOURCE_BATCH     源库 id 批次，默认 5000
#   MAX_TARGET_USER_ID  user 系表 user_id 上限，默认 100000000
#   DRY_RUN          1=跳过 apply
#   START_TABLE      从指定表开始（含该表）
#   MASTER_LOG       总控日志；默认 ${LOG_DIR}/reconcile_all_master.log
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

ENV="${ENV:-$HERE/ng_migration.env}"
SINCE_DATE="${SINCE_DATE:-2026-01-01}"
LOG_DIR="${LOG_DIR:-/tmp/reconcile_logs}"
LOAD_WORKERS="${LOAD_WORKERS:-8}"
APPLY_WORKERS="${APPLY_WORKERS:-15}"
APPLY_BATCH="${APPLY_BATCH:-500}"
PAGE_SIZE="${PAGE_SIZE:-50000}"
SOURCE_BATCH="${SOURCE_BATCH:-5000}"
MAX_TARGET_USER_ID="${MAX_TARGET_USER_ID:-100000000}"
DRY_RUN="${DRY_RUN:-0}"
START_TABLE="${START_TABLE:-user}"
MASTER_LOG="${MASTER_LOG:-$LOG_DIR/reconcile_all_master.log}"

TABLES=(user user_info user_bankcard user_product application loan)

if [[ ! -f "$ENV" ]]; then
  echo "env not found: $ENV" >&2
  exit 1
fi

mkdir -p "$LOG_DIR"

log() {
  local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $*"
  echo "$msg" | tee -a "$MASTER_LOG"
}

common_args=(
  --env "$ENV"
  --since-date "$SINCE_DATE"
  --log-dir "$LOG_DIR"
  --load-workers "$LOAD_WORKERS"
  --page-size "$PAGE_SIZE"
  --source-batch "$SOURCE_BATCH"
  --max-target-user-id "$MAX_TARGET_USER_ID"
  --apply-workers "$APPLY_WORKERS"
  --apply-batch "$APPLY_BATCH"
)

run_one_table() {
  local table="$1"
  local cache="/tmp/reconcile_${table}_target.jsonl"
  local plan="/tmp/reconcile_${table}_plan.jsonl"
  local t0
  t0=$(date +%s)

  log "========== BEGIN table=${table} =========="

  log "${table}: phase load-target → ${cache}"
  python3 "$HERE/reconcile_tables.py" \
    "${common_args[@]}" \
    --table "$table" \
    --phase load-target \
    --target-cache "$cache"

  log "${table}: phase plan → ${plan}"
  python3 "$HERE/reconcile_tables.py" \
    "${common_args[@]}" \
    --table "$table" \
    --phase plan \
    --from-cache \
    --target-cache "$cache" \
    --plan-file "$plan"

  local plan_lines=0
  if [[ -f "$plan" ]]; then
    plan_lines=$(wc -l < "$plan" | tr -d ' ')
  fi
  log "${table}: plan rows=${plan_lines}"

  if [[ "$DRY_RUN" == "1" ]]; then
    log "${table}: skip apply (DRY_RUN=1)"
  elif [[ "$plan_lines" == "0" ]]; then
    log "${table}: skip apply (empty plan)"
  else
    log "${table}: phase apply (${APPLY_WORKERS} workers, batch=${APPLY_BATCH})"
    python3 "$HERE/reconcile_tables.py" \
      "${common_args[@]}" \
      --table "$table" \
      --phase apply \
      --apply \
      --plan-file "$plan"
  fi

  log "========== DONE table=${table} elapsed=$(( $(date +%s) - t0 ))s =========="
}

log "reconcile_all start ENV=$ENV SINCE_DATE=$SINCE_DATE DRY_RUN=$DRY_RUN START_TABLE=$START_TABLE"
log "LOG_DIR=$LOG_DIR APPLY_WORKERS=$APPLY_WORKERS APPLY_BATCH=$APPLY_BATCH"

started=0
for table in "${TABLES[@]}"; do
  if [[ "$started" == "0" ]]; then
    if [[ "$table" != "$START_TABLE" ]]; then
      log "skip table=${table} (before START_TABLE=${START_TABLE})"
      continue
    fi
    started=1
  fi
  run_one_table "$table"
done

log "reconcile_all finished OK"
