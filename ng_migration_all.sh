#!/usr/bin/env bash
# 全量：源库按批灌 user 系 + application/loan + verify（无需 step0 / dt_mig_*）
# 日志：LOG_FILE（结构化耗时/进度）+ 本脚本 stdout
set -eo pipefail

cd "$(dirname "$0")"
set -a
set +u
# shellcheck disable=SC1091
source ./ng_migration.env
set -u
set +a

export LOG_FILE="${LOG_FILE:-/tmp/ng_mig_all.log}"
export PROGRESS_FILE="${PROGRESS_FILE:-/tmp/ng_mig_all_progress.env}"
export LOG_EVERY="${LOG_EVERY:-3}"
# DROP_MAT_ON_START=1 时清理遗留 dt_mig_* 并重置进度；续跑请设 0
export DROP_MAT_ON_START="${DROP_MAT_ON_START:-1}"

# 全量跑使用优化参数（覆盖 ng_migration.env 里的旧值）
export USER_BATCH=20000
export USER_INSERT_BATCH=20000
export WORKERS=10
export APP_BATCH=100000
export APP_INSERT_BATCH=10000
export ID_MAPPING_INSERT_BATCH=25000
export APP_WORKER_BALANCE=count
export APP_WORKERS=8
export LOOKUP_PARALLEL=4
export VT_PRELOAD=1
export LUP_PAIR_CHUNK=400
export PROGRESS_SAVE_EVERY="${PROGRESS_SAVE_EVERY:-3}"
export MAX_WORKER_SLOTS=64

phase_log() {
  local msg="$1"
  printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$msg" | tee -a "$LOG_FILE"
}

if [[ "${DROP_MAT_ON_START}" == "1" ]]; then
  : > "$LOG_FILE"
  phase_log "======== ALL MIGRATION START (fresh) host=$(hostname) ========"
else
  phase_log "======== ALL MIGRATION RESUME host=$(hostname) ========"
  if [[ -f "$PROGRESS_FILE" ]]; then
    phase_log "PROGRESS $(tr '\n' ' ' < "$PROGRESS_FILE")"
  fi
fi
phase_log "CONFIG DROP_MAT_ON_START=$DROP_MAT_ON_START LOG_FILE=$LOG_FILE WORKERS=$WORKERS APP_WORKERS=$APP_WORKERS"
phase_log "NOTE 源库按批直接写正式表；启动时可选清理遗留 dt_mig_*"
phase_log "CONFIG USER_BATCH=$USER_BATCH USER_INSERT_BATCH=$USER_INSERT_BATCH APP_BATCH=$APP_BATCH APP_INSERT_BATCH=$APP_INSERT_BATCH LOOKUP_PARALLEL=$LOOKUP_PARALLEL"

phase_log "======== PHASE full (user + application) START ========"
t0=$(date +%s)
python3 ng_migration_run.py full
t1=$(date +%s)
phase_log "======== PHASE full END elapsed=$((t1 - t0))s ($(( (t1 - t0) / 60 )) min) ========"

phase_log "======== ALL MIGRATION DONE total=$((t1 - t0))s ($(( (t1 - t0) / 60 )) min) ========"
phase_log "LOG_FILE=$LOG_FILE PROGRESS_FILE=$PROGRESS_FILE"
