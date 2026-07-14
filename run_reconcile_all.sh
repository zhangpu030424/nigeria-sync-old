#!/usr/bin/env bash
# 按业务顺序逐表对账（单进程）：load-target → plan → [apply]
# VT / LUP 只预加载一次，整次 run 常驻内存，跨表不释放、不重扫。
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

# 优先使用较新的 python3；服务器默认 python3 可能是 3.6
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
# 本脚本仍支持 3.6；若未来依赖 3.7+ 语法再提高下限
if ! "$PYTHON" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 6) else 1)'; then
  echo "需要 Python >= 3.6，当前: $PYTHON ($PY_VER)" >&2
  exit 1
fi

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
  --all-tables
  --start-table "$START_TABLE"
  --phase all
)

if [[ "$DRY_RUN" != "1" ]]; then
  common_args+=(--apply)
fi

log "reconcile_all start PYTHON=$PYTHON ($PY_VER) ENV=$ENV SINCE_DATE=$SINCE_DATE DRY_RUN=$DRY_RUN START_TABLE=$START_TABLE"
log "LOG_DIR=$LOG_DIR APPLY_WORKERS=$APPLY_WORKERS APPLY_BATCH=$APPLY_BATCH (single-process, VT/LUP kept)"

"$PYTHON" "$HERE/reconcile_tables.py" "${common_args[@]}"
rc=$?

if [[ "$rc" -eq 0 ]]; then
  log "reconcile_all finished OK"
else
  log "reconcile_all failed rc=$rc"
fi
exit "$rc"
