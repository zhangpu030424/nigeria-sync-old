#!/usr/bin/env bash
# 流水线起始时间戳：增量 CDC 从该时刻起补 binlog（与宽表重建耗时无关）
# 由 sync-pipeline-auto.sh 在第一步调用；写入 logs/bulk-start-ms.env
#
# 优先从源 MySQL（Africa/Lagos）取 UNIX 毫秒，与 binlog 事件时间轴一致；
# 避免运维本机（如北京时间）时钟偏差。值为 UTC epoch 毫秒，不是「北京时刻」编码。

bulk_start_ms_from_source_mysql() {
  local host="${SOURCE_MYSQL_HOST:-}"
  local port="${SOURCE_MYSQL_PORT:-3306}"
  local user="${SOURCE_MYSQL_USER:-}"
  local pass="${SOURCE_MYSQL_PASSWORD:-}"
  [[ -z "$host" || -z "$user" ]] && return 1
  local ms
  ms=$(MYSQL_PWD="$pass" mysql -h "$host" -P "$port" -u "$user" -N -e \
    "SET time_zone = 'Africa/Lagos'; SELECT CAST(ROUND(UNIX_TIMESTAMP(NOW(3)) * 1000) AS UNSIGNED);" \
    2>/dev/null) || return 1
  ms="$(echo "$ms" | tr -d '[:space:]')"
  [[ "$ms" =~ ^[0-9]+$ ]] || return 1
  echo "$ms"
}

bulk_start_ms_from_host() {
  python3 -c 'import time; print(int(time.time()*1000))' 2>/dev/null || echo "$(($(date +%s)*1000))"
}

bulk_start_ms_now() {
  local ms=""
  local src="host"
  if ms=$(bulk_start_ms_from_source_mysql); then
    src="source_mysql"
  else
    ms=$(bulk_start_ms_from_host)
    echo ">> WARN: 无法从源库取时间戳，回退本机 epoch（请检查 SOURCE_MYSQL_* 与 mysql 客户端）" >&2
  fi
  BULK_START_MS="$ms"
  BULK_START_SOURCE="$src"
}

bulk_start_iso_from_ms() {
  # mode: wat | utc | host — 把同一 Unix epoch 毫秒格式化成可读时刻
  local ms="$1"
  local mode="$2"
  local label="$3"
  python3 -c "
from datetime import datetime, timezone, timedelta
ms = int('${ms}')
mode = '${mode}'
label = '${label}'
dt_utc = datetime.fromtimestamp(ms / 1000, timezone.utc)
if mode == 'wat':
    try:
        from zoneinfo import ZoneInfo
        dt = dt_utc.astimezone(ZoneInfo('Africa/Lagos'))
    except Exception:
        dt = dt_utc.astimezone(timezone(timedelta(hours=1)))
elif mode == 'utc':
    dt = dt_utc
else:
    dt = datetime.fromtimestamp(ms / 1000).astimezone()
print(dt.strftime('%Y-%m-%d %H:%M:%S') + ' ' + label)
" 2>/dev/null || echo "${ms}ms"
}

record_bulk_start_ms() {
  local log_dir="${1:-logs}"
  mkdir -p "$log_dir"
  local f="${log_dir}/bulk-start-ms.env"
  bulk_start_ms_now
  export BULK_START_MS BULK_START_SOURCE
  BULK_START_ISO_NG="$(bulk_start_iso_from_ms "$BULK_START_MS" "wat" "WAT")"
  BULK_START_ISO_UTC="$(bulk_start_iso_from_ms "$BULK_START_MS" "utc" "UTC")"
  BULK_START_ISO_HOST="$(bulk_start_iso_from_ms "$BULK_START_MS" "host" "$(date +%Z 2>/dev/null || echo HOST)")"
  cat > "$f" <<EOF
# 流水线锁定时刻（毫秒 Unix epoch），用于日志对齐与 CDC_STARTUP_MODE=timestamp 时注入。
# 默认增量为 initial（先快照补漏写再追 binlog）；bulk-start-ms 记录宽表重建起点供排查。
# 优先 source_mysql：源库 NOW() @ Africa/Lagos → UNIX_TIMESTAMP，与 binlog 时间轴对齐。
BULK_START_MS=${BULK_START_MS}
BULK_START_SOURCE=${BULK_START_SOURCE}
BULK_START_ISO_NG='${BULK_START_ISO_NG}'
BULK_START_ISO_UTC='${BULK_START_ISO_UTC}'
BULK_START_ISO_HOST='${BULK_START_ISO_HOST}'
EOF
  echo ">> 锁定 bulk-start-ms=${BULK_START_MS}（来源: ${BULK_START_SOURCE}）"
  echo ">>   尼日利亚(WAT): ${BULK_START_ISO_NG}  ← CDC/binlog 对齐看这一行"
  echo ">>   UTC:           ${BULK_START_ISO_UTC}"
  if [[ "${BULK_START_SOURCE}" == "host" ]]; then
    echo ">>   运维机时钟:    ${BULK_START_ISO_HOST}（仅 epoch 来源为 host 时的参考，非 CDC 时区）"
  fi
  echo ">> 已写入 ${f}"
}

load_bulk_start_ms() {
  local f="${1:-logs/bulk-start-ms.env}"
  if [[ -f "$f" ]]; then
    # shellcheck source=scripts/lib/load-env.sh
    source "$(dirname "${BASH_SOURCE[0]}")/load-env.sh"
    set -a
    load_env_file "$f"
    set +a
    if [[ -n "${BULK_START_MS:-}" ]]; then
      echo ">> 读取 bulk-start-ms=${BULK_START_MS} (WAT ${BULK_START_ISO_NG:-${BULK_START_ISO:-}}) ← ${f}"
      return 0
    fi
  fi
  return 1
}

resolve_bulk_start_ms() {
  local explicit="${1:-}"
  if [[ -n "$explicit" ]]; then
    export BULK_START_MS="$explicit"
    return 0
  fi
  if load_bulk_start_ms; then
    return 0
  fi
  bulk_start_ms_now
  export BULK_START_MS BULK_START_SOURCE
  echo ">> WARN: 无 logs/bulk-start-ms.env，临时使用 bulk-start-ms=${BULK_START_MS}（来源: ${BULK_START_SOURCE}）"
}
