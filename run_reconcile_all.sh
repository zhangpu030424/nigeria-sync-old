#!/usr/bin/env bash
# 按业务顺序逐表对账（单进程）：load-target → plan → [apply]
# VT / LUP 只预加载一次，整次 run 常驻内存，跨表不释放、不重扫。
# plan 源端按 id 区间多线程并行（SOURCE_WORKERS）。
#
# 顺序：user → user_info → user_bankcard → user_product → application → loan
#
# Usage:
#   ./run_reconcile_all.sh
#   ENV=./ng_migration.env SINCE_DATE=2026-01-01 ./run_reconcile_all.sh
#   DRY_RUN=1 ./run_reconcile_all.sh          # 只 load + plan，不写库
#   START_TABLE=application ./run_reconcile_all.sh   # 从某表续跑
#   FROM_CACHE=1 START_TABLE=user_info ./run_reconcile_all.sh  # 复用已有 target cache
#
# 环境变量（均可覆盖；下面为 32 核 / 大内存机推荐默认）：
#   ENV / SINCE_DATE / LOG_DIR / MASTER_LOG
#   LOAD_WORKERS     目标库并行加载，默认 16
#   SOURCE_WORKERS   plan 源 id 并行，默认 8
#   SOURCE_BATCH     plan 每批 id 跨度，默认 20000
#   PAGE_SIZE        目标库分页，默认 100000
#   APPLY_WORKERS    apply 并行，默认 24
#   APPLY_BATCH      apply 每批，默认 1000
#   LOOKUP_PARALLEL  批内关联表并行读（写入 env），默认 5
#   MAX_TARGET_USER_ID  默认 100000000
#   DRY_RUN          1=跳过 apply
#   START_TABLE      从指定表开始（含）
#   FROM_CACHE       1=plan 复用已有 /tmp/reconcile_*_target.jsonl
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
PY_VER="$("$PYTHON" -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
if ! "$PYTHON" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 6) else 1)'; then
  echo "需要 Python >= 3.6，当前: $PYTHON ($PY_VER)" >&2
  exit 1
fi

ENV="${ENV:-$HERE/ng_migration.env}"
SINCE_DATE="${SINCE_DATE:-2026-01-01}"
LOG_DIR="${LOG_DIR:-/tmp/reconcile_logs}"
LOAD_WORKERS="${LOAD_WORKERS:-16}"
SOURCE_WORKERS="${SOURCE_WORKERS:-8}"
APPLY_WORKERS="${APPLY_WORKERS:-24}"
APPLY_BATCH="${APPLY_BATCH:-1000}"
PAGE_SIZE="${PAGE_SIZE:-100000}"
SOURCE_BATCH="${SOURCE_BATCH:-20000}"
MAX_TARGET_USER_ID="${MAX_TARGET_USER_ID:-100000000}"
LOOKUP_PARALLEL="${LOOKUP_PARALLEL:-5}"
DRY_RUN="${DRY_RUN:-0}"
START_TABLE="${START_TABLE:-user}"
FROM_CACHE="${FROM_CACHE:-0}"
MASTER_LOG="${MASTER_LOG:-$LOG_DIR/reconcile_all_master.log}"

if [[ ! -f "$ENV" ]]; then
  echo "env not found: $ENV" >&2
  exit 1
fi

mkdir -p "$LOG_DIR"
export LOOKUP_PARALLEL

log() {
  local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $*"
  echo "$msg" | tee -a "$MASTER_LOG"
}

common_args=(
  --env "$ENV"
  --since-date "$SINCE_DATE"
  --log-dir "$LOG_DIR"
  --load-workers "$LOAD_WORKERS"
  --source-workers "$SOURCE_WORKERS"
  --page-size "$PAGE_SIZE"
  --source-batch "$SOURCE_BATCH"
  --max-target-user-id "$MAX_TARGET_USER_ID"
  --apply-workers "$APPLY_WORKERS"
  --apply-batch "$APPLY_BATCH"
  --all-tables
  --start-table "$START_TABLE"
  --phase all
)

if [[ "$FROM_CACHE" == "1" ]]; then
  common_args+=(--from-cache)
fi

if [[ "$DRY_RUN" != "1" ]]; then
  common_args+=(--apply)
fi

log "reconcile_all start PYTHON=$PYTHON ($PY_VER) ENV=$ENV SINCE_DATE=$SINCE_DATE DRY_RUN=$DRY_RUN START_TABLE=$START_TABLE FROM_CACHE=$FROM_CACHE"
log "LOAD_WORKERS=$LOAD_WORKERS SOURCE_WORKERS=$SOURCE_WORKERS SOURCE_BATCH=$SOURCE_BATCH PAGE_SIZE=$PAGE_SIZE LOOKUP_PARALLEL=$LOOKUP_PARALLEL"
log "APPLY_WORKERS=$APPLY_WORKERS APPLY_BATCH=$APPLY_BATCH (single-process, VT/LUP kept, plan parallel)"

"$PYTHON" "$HERE/reconcile_tables.py" "${common_args[@]}"
rc=$?

if [[ "$rc" -eq 0 ]]; then
  log "reconcile_all finished OK"
else
  log "reconcile_all failed rc=$rc"
fi
exit "$rc"
