#!/usr/bin/env bash
# Daily one-month window UPSERT sync (source read-only → target merge).
# Default: nohup background (safe to exit SSH). Keeps validate_and_repair as fallback.
set -eo pipefail

cd "$(dirname "$0")"

set -a
set +u
# shellcheck disable=SC1091
source ./ng_migration.env
set -u
set +a

ts="$(date '+%Y%m%d_%H%M%S')"
export LOG_FILE="${LOG_FILE:-/tmp/ng_window_upsert_last_month_${ts}.log}"
export REPORTS_DIR="${REPORTS_DIR:-$(pwd)/reports}"
export NOHUP_LOG="${NOHUP_LOG:-/tmp/ng_window_upsert_last_month_${ts}.nohup.log}"
BACKGROUND="${BACKGROUND:-1}"
job_status="starting"
t0=0

phase_log() {
  local msg="$1"
  printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$msg" | tee -a "$LOG_FILE"
}

send_feishu_notice() {
  local title="$1"
  local body="$2"
  local webhook="${FEISHU_WEBHOOK:-}"
  if [[ -z "$webhook" ]]; then
    return 0
  fi
  FEISHU_WEBHOOK="$webhook" FEISHU_TITLE="$title" FEISHU_BODY="$body" python3 - <<'PY'
import json
import os
import sys
from urllib import request

webhook = os.environ.get("FEISHU_WEBHOOK", "")
title = os.environ.get("FEISHU_TITLE", "")
body = os.environ.get("FEISHU_BODY", "")
payload = json.dumps({
    "msg_type": "text",
    "content": {"text": f"{title}\n{body}"},
}, ensure_ascii=False).encode("utf-8")
try:
    req = request.Request(webhook, data=payload, headers={"Content-Type": "application/json"})
    request.urlopen(req, timeout=10).read()
except Exception as exc:
    print(f"feishu_notice_failed: {type(exc).__name__}: {exc}", file=sys.stderr)
PY
}

on_error() {
  local exit_code=$?
  local end_ts
  local elapsed=0
  end_ts="$(date '+%Y-%m-%d %H:%M:%S')"
  if [[ "$t0" -gt 0 ]]; then
    elapsed=$(($(date +%s) - t0))
  fi
  phase_log "======== WINDOW UPSERT ERROR status=$job_status exit=$exit_code elapsed=${elapsed}s ========"
  send_feishu_notice \
    "NG window upsert failed" \
    "time=${end_ts}
host=$(hostname)
status=${job_status}
exit_code=${exit_code}
elapsed=${elapsed}s
log=${LOG_FILE}
nohup=${NOHUP_LOG}
reports_dir=${REPORTS_DIR}"
  exit "$exit_code"
}

trap on_error ERR

DRY_RUN="${DRY_RUN:-0}"
APPLY_FLAG="--dry-run"
if [[ "$DRY_RUN" == "0" ]]; then
  APPLY_FLAG="--apply"
fi

phase_log "======== WINDOW UPSERT START host=$(hostname) dry_run=${DRY_RUN} background=${BACKGROUND} ========"
phase_log "CONFIG LOG_FILE=$LOG_FILE NOHUP_LOG=$NOHUP_LOG REPORTS_DIR=$REPORTS_DIR"
phase_log "FALLBACK: ng_validate_repair_last_month.sh (validate+repair) unchanged"
phase_log "TAIL: tail -f $LOG_FILE"

run_python() {
  python3 window_upsert.py \
    --date-window last-month \
    $APPLY_FLAG \
    --tables all \
    --app-validate-batch "${APP_VALIDATE_BATCH:-20000}" \
    --user-insert-batch "${USER_INSERT_BATCH:-5000}" \
    --app-insert-batch "${APP_INSERT_BATCH:-5000}" \
    --id-mapping-insert-batch "${ID_MAPPING_INSERT_BATCH:-10000}" \
    --log-file "$LOG_FILE" \
    --reports-dir "$REPORTS_DIR" \
    --feishu-webhook "${FEISHU_WEBHOOK:-}" \
    --report-base-url "${REPORT_BASE_URL:-}"
}

t0=$(date +%s)

if [[ "$BACKGROUND" == "1" ]]; then
  job_status="starting background window_upsert.py"
  : > "$NOHUP_LOG"
  nohup python3 window_upsert.py \
    --date-window last-month \
    $APPLY_FLAG \
    --tables all \
    --app-validate-batch "${APP_VALIDATE_BATCH:-20000}" \
    --user-insert-batch "${USER_INSERT_BATCH:-5000}" \
    --app-insert-batch "${APP_INSERT_BATCH:-5000}" \
    --id-mapping-insert-batch "${ID_MAPPING_INSERT_BATCH:-10000}" \
    --log-file "$LOG_FILE" \
    --reports-dir "$REPORTS_DIR" \
    --feishu-webhook "${FEISHU_WEBHOOK:-}" \
    --report-base-url "${REPORT_BASE_URL:-}" \
    >> "$NOHUP_LOG" 2>&1 &
  pid=$!
  phase_log "background started pid=${pid} log=${LOG_FILE} nohup=${NOHUP_LOG}"
  phase_log "check: ps -p ${pid} -o pid,cmd"
  phase_log "======== WINDOW UPSERT LAUNCHED (detach ok) ========"
  exit 0
fi

job_status="running window_upsert.py foreground"
run_python 2>&1 | tee -a "$LOG_FILE"
job_status="completed"
t1=$(date +%s)

phase_log "======== WINDOW UPSERT END elapsed=$((t1 - t0))s ($(((t1 - t0) / 60)) min) ========"
phase_log "LOG_FILE=$LOG_FILE REPORTS_DIR=$REPORTS_DIR"
send_feishu_notice \
  "NG window upsert completed" \
  "time=$(date '+%Y-%m-%d %H:%M:%S')
host=$(hostname)
dry_run=${DRY_RUN}
elapsed=$((t1 - t0))s
log=${LOG_FILE}
reports_dir=${REPORTS_DIR}"
