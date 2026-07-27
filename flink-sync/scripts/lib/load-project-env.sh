#!/usr/bin/env bash
# 加载项目 .env（trim 值、忽略行内 # 注释）
# 用法: source scripts/lib/load-project-env.sh

load_project_env() {
  local root="${1:-.}"
  local env_file="${root}/.env"
  [[ -f "$env_file" ]] || { echo "请先: cp .env.example .env" >&2; return 1; }
  # shellcheck source=scripts/lib/load-env.sh
  source "${root}/scripts/lib/load-env.sh"
  set -a
  load_env_file "$env_file"
  set +a
  return 0
}
