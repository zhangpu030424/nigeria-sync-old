#!/usr/bin/env bash
# Daily source-window validation and repair for the previous month.
set -eo pipefail

cd "$(dirname "$0")"

set -a
set +u
# shellcheck disable=SC1091
source ./ng_migration.env
set -u
set +a

ts="$(date '+%Y%m%d_%H%M%S')"
export LOG_FILE="${LOG_FILE:-/tmp/ng_validate_repair_last_month_${ts}.log}"
export REPAIR_LOG="${REPAIR_LOG:-/tmp/ng_validate_repair_last_month_repair_${ts}.log}"
export REPORTS_DIR="${REPORTS_DIR:-$(pwd)/reports}"
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
  phase_log "======== LAST MONTH VALIDATE+REPAIR ERROR status=$job_status exit=$exit_code elapsed=${elapsed}s ========"
  send_feishu_notice \
    "NG migration last-month validate+repair failed" \
    "time=${end_ts}
host=$(hostname)
status=${job_status}
exit_code=${exit_code}
elapsed=${elapsed}s
log=${LOG_FILE}
repair_log=${REPAIR_LOG}
reports_dir=${REPORTS_DIR}"
  exit "$exit_code"
}

trap on_error ERR

phase_log "======== LAST MONTH VALIDATE+REPAIR START host=$(hostname) ========"
phase_log "CONFIG LOG_FILE=$LOG_FILE REPAIR_LOG=$REPAIR_LOG REPORTS_DIR=$REPORTS_DIR"
phase_log "CONFIG FEISHU_WEBHOOK=${FEISHU_WEBHOOK:+set} REPORT_BASE_URL=${REPORT_BASE_URL:-}"
send_feishu_notice \
  "NG migration last-month validate+repair started" \
  "time=$(date '+%Y-%m-%d %H:%M:%S')
host=$(hostname)
log=${LOG_FILE}
repair_log=${REPAIR_LOG}
reports_dir=${REPORTS_DIR}
report_base_url=${REPORT_BASE_URL:-}"

t0=$(date +%s)
job_status="running validate_and_repair.py"
python3 validate_and_repair.py \
  --mode repair \
  --apply \
  --tables all \
  --date-window last-month \
  --repair-lookup-chunk 50 \
  --app-validate-batch 3000 \
  --field-diff-chunk 50 \
  --reports-dir "$REPORTS_DIR" \
  --repair-log "$REPAIR_LOG" \
  --feishu-webhook "${FEISHU_WEBHOOK:-}" \
  --report-base-url "${REPORT_BASE_URL:-}" \
  2>&1 | tee -a "$LOG_FILE"
job_status="completed"
t1=$(date +%s)

phase_log "======== LAST MONTH VALIDATE+REPAIR END elapsed=$((t1 - t0))s ($(((t1 - t0) / 60)) min) ========"
phase_log "LOG_FILE=$LOG_FILE REPAIR_LOG=$REPAIR_LOG REPORTS_DIR=$REPORTS_DIR"
send_feishu_notice \
  "NG migration last-month validate+repair completed" \
  "time=$(date '+%Y-%m-%d %H:%M:%S')
host=$(hostname)
elapsed=$((t1 - t0))s
log=${LOG_FILE}
repair_log=${REPAIR_LOG}
reports_dir=${REPORTS_DIR}
report_base_url=${REPORT_BASE_URL:-}"
