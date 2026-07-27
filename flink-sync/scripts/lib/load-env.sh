#!/usr/bin/env bash
# 安全加载 KEY=VALUE 环境文件（跳过纯注释/中文说明行；值可含空格）
# 用法: load_env_file /path/to/.env

load_env_file() {
  local file="$1"
  [[ -f "$file" ]] || return 1
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%%#*}"
    line="$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ -z "$line" ]] && continue
    [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue
    local key="${line%%=*}"
    local val="${line#*=}"
    val="$(echo "$val" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    # 去掉首尾引号（若存在）
    if [[ "$val" =~ ^\".*\"$ ]]; then
      val="${val:1:${#val}-2}"
    elif [[ "$val" =~ ^\'.*\'$ ]]; then
      val="${val:1:${#val}-2}"
    fi
    [[ -n "${!key:-}" ]] && continue
    export "${key}=${val}"
  done < "$file"
}
