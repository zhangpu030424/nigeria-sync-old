#!/usr/bin/env bash
# 全量高速迁移（INLINE_MAT=1 边物化边灌，无需单独 step0）
set -eo pipefail

cd "$(dirname "$0")"
set -a
set +u
# shellcheck disable=SC1091
source ./ng_migration.env
set -u
set +a

export USER_BATCH="${USER_BATCH:-20000}"
export USER_INSERT_BATCH="${USER_INSERT_BATCH:-20000}"
export WORKERS="${WORKERS:-10}"
export APP_BATCH="${APP_BATCH:-100000}"
export APP_INSERT_BATCH="${APP_INSERT_BATCH:-10000}"
export ID_MAPPING_INSERT_BATCH="${ID_MAPPING_INSERT_BATCH:-25000}"
export APP_WORKER_BALANCE="${APP_WORKER_BALANCE:-count}"
export APP_WORKERS="${APP_WORKERS:-8}"
export LOOKUP_PARALLEL="${LOOKUP_PARALLEL:-4}"
export VT_PRELOAD="${VT_PRELOAD:-1}"
export LUP_PAIR_CHUNK="${LUP_PAIR_CHUNK:-400}"
export MAX_WORKER_SLOTS="${MAX_WORKER_SLOTS:-64}"
export PROGRESS_FILE="${PROGRESS_FILE:-/tmp/ng_mig_full_progress.env}"
export LOG_FILE="${LOG_FILE:-/tmp/ng_mig_full.log}"
export LOG_EVERY="${LOG_EVERY:-5}"
export INLINE_MAT="${INLINE_MAT:-1}"

rm -f "$PROGRESS_FILE"
echo "== ng_migration FULL START $(date '+%F %T') =="
echo "WORKERS=$WORKERS APP_WORKERS=$APP_WORKERS USER_BATCH=$USER_BATCH APP_BATCH=$APP_BATCH"
time python3 ng_migration_run.py full
echo "== ng_migration FULL END $(date '+%F %T') =="
