#!/usr/bin/env bash
# 部分表同步：user_info → application + loan + id_mapping
# application 单 worker，降低目标库死锁概率
set -eo pipefail

cd "$(dirname "$0")"
set -a
set +u
# shellcheck disable=SC1091
source ./ng_migration.env
set -u
set +a

export DROP_MAT_ON_START=0
export PROGRESS_FILE=/tmp/ng_mig_partial_progress.env
export LOG_FILE=/tmp/ng_mig_partial.log
export LOG_EVERY="${LOG_EVERY:-3}"
export WORKERS="${WORKERS:-10}"
export USER_BATCH="${USER_BATCH:-20000}"
export USER_INSERT_BATCH="${USER_INSERT_BATCH:-10000}"
export APP_WORKERS=1
export APP_BATCH="${APP_BATCH:-50000}"
export APP_INSERT_BATCH="${APP_INSERT_BATCH:-5000}"
export LOOKUP_PARALLEL="${LOOKUP_PARALLEL:-2}"
export VT_PRELOAD=1
export LUP_PRELOAD=1
export DEADLOCK_MAX_RETRIES="${DEADLOCK_MAX_RETRIES:-8}"

phase_log() {
  local msg="$1"
  printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$msg" | tee -a "$LOG_FILE"
}

rm -f "$PROGRESS_FILE"
: > "$LOG_FILE"

phase_log "======== PARTIAL MIGRATION START host=$(hostname) ========"
phase_log "TABLES=user_info,application,loan,id_mapping APP_WORKERS=$APP_WORKERS"
phase_log "PROGRESS_FILE=$PROGRESS_FILE LOG_FILE=$LOG_FILE"

t0=$(date +%s)
phase_log "======== PHASE user_info START ========"
python3 ng_migration_run.py user_info
t1=$(date +%s)
phase_log "======== PHASE user_info END elapsed=$((t1 - t0))s ========"

phase_log "======== PHASE application+loan+id_mapping START ========"
python3 ng_migration_run.py application
t2=$(date +%s)
phase_log "======== PHASE application END elapsed=$((t2 - t1))s ========"

phase_log "======== VERIFY ========"
python3 ng_migration_run.py verify | tee -a "$LOG_FILE"

phase_log "======== PARTIAL MIGRATION DONE total=$((t2 - t0))s ========"
