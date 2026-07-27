#!/usr/bin/env bash
# 从 config/sync-jobs.conf 解析 ENABLED=1 的 Job 列表
# 用法: source scripts/lib/sync-jobs.sh && sync_jobs_load "user,user_info"
#
# 行格式（| 分隔，固定 9 列；计数 SQL 可含 |，如 REGEXP '567|568'，须从右向左解析）:
#   KEY|DESC|FULL_SQL|INCR_SQL|FULL_RUNNER|SRC_CNT_SQL|TGT_CNT_SQL|MONITOR_TABLE|ENABLED

# 解析单行 → SYNC_JOB_*（前 5 列固定无 |；计数 SQL 列勿含 |；末列 ENABLED）
sync_job_parse_line() {
  local line="$1"
  SYNC_JOB_ENABLED="${line##*|}"
  line="${line%|*}"
  SYNC_JOB_MONITOR_TABLE="${line##*|}"
  line="${line%|*}"
  SYNC_JOB_KEY="${line%%|*}"
  line="${line#*|}"
  SYNC_JOB_DESC="${line%%|*}"
  line="${line#*|}"
  SYNC_JOB_FULL_SQL="${line%%|*}"
  line="${line#*|}"
  SYNC_JOB_INCR_SQL="${line%%|*}"
  line="${line#*|}"
  SYNC_JOB_FULL_RUNNER="${line%%|*}"
  line="${line#*|}"
  # 剩余 line = SRC_CNT_SQL|TGT_CNT_SQL；TGT 恒以 SELECT COUNT 开头
  if [[ "$line" == *"|SELECT COUNT"* ]]; then
    SYNC_JOB_SRC_CNT_SQL="${line%%|SELECT COUNT*}"
    SYNC_JOB_TGT_CNT_SQL="SELECT COUNT${line#*|SELECT COUNT}"
  else
    SYNC_JOB_SRC_CNT_SQL="${line%%|*}"
    SYNC_JOB_TGT_CNT_SQL="${line#*|}"
  fi
}

sync_jobs_load() {
  local filter="${1:-}"
  local conf="${SYNC_JOBS_CONF:-config/sync-jobs.conf}"
  SYNC_ENABLED_JOBS=()

  [[ -f "$conf" ]] || { echo "ERR: 缺少 $conf" >&2; return 1; }

  while IFS= read -r row || [[ -n "$row" ]]; do
    row="${row%%#*}"
    row="$(echo "$row" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ -z "$row" ]] && continue
    local key="${row%%|*}"
    local enabled="${row##*|}"
    [[ "$enabled" != "1" ]] && continue
    if [[ -n "$filter" ]]; then
      echo ",${filter}," | grep -q ",${key}," || continue
    fi
    SYNC_ENABLED_JOBS+=("$key")
  done < "$conf"

  if [[ ${#SYNC_ENABLED_JOBS[@]} -eq 0 ]]; then
    echo "ERR: 无 ENABLED=1 的 Job（filter=${filter:-all}）" >&2
    return 1
  fi
  return 0
}

sync_jobs_print_plan() {
  local phase="$1"
  echo ">> ${phase} Job 顺序 (${#SYNC_ENABLED_JOBS[@]}): ${SYNC_ENABLED_JOBS[*]}"
}

# 按 Job 解析并行度
# 增量默认（本集群 TM=16 slot，建议峰值 ≤14 留余量）：
#   user / user_product / user_bankcard / user_info / id_mapping → FLINK_PARALLELISM_INCR_LIGHT(2)
#   application / loan → FLINK_PARALLELISM_INCR_HEAVY(2)  【16 slot 勿用 4/8，易 NoResourceAvailable】
# 可用 FLINK_PARALLELISM_INCR_<JOB>（大写，如 FLINK_PARALLELISM_INCR_APPLICATION=2）覆盖
# 用法: sync_job_parallelism user_info incr
sync_job_parallelism() {
  local job_key="$1"
  local mode="${2:-incr}"
  local incr_default="${FLINK_PARALLELISM_INCR:-2}"
  local bulk_default="${FLINK_PARALLELISM_BULK:-${FLINK_PARALLELISM:-8}}"
  local job_env
  job_env="$(echo "$job_key" | tr '[:lower:]' '[:upper:]')"
  job_env="FLINK_PARALLELISM_INCR_${job_env}"

  if [[ "$mode" == "bulk" ]]; then
    if [[ "$job_key" == "user_info" ]]; then
      if [[ -n "${FLINK_PARALLELISM_USER_INFO_BULK:-}" ]]; then
        echo "${FLINK_PARALLELISM_USER_INFO_BULK}"
      elif [[ -n "${FLINK_PARALLELISM_USER_INFO:-}" ]]; then
        echo "${FLINK_PARALLELISM_USER_INFO}"
      else
        echo "$bulk_default"
      fi
      return 0
    fi
    echo "$bulk_default"
    return 0
  fi

  # 增量：优先 per-job 环境变量
  if [[ -n "${!job_env:-}" ]]; then
    echo "${!job_env}"
    return 0
  fi

  # 兼容旧变量
  if [[ "$job_key" == "user_info" ]]; then
    if [[ -n "${FLINK_PARALLELISM_USER_INFO_INCR:-}" ]]; then
      echo "${FLINK_PARALLELISM_USER_INFO_INCR}"
      return 0
    fi
    if [[ -n "${FLINK_PARALLELISM_USER_INFO:-}" ]]; then
      echo "${FLINK_PARALLELISM_USER_INFO}"
      return 0
    fi
  fi

  case "$job_key" in
    application|loan)
      echo "${FLINK_PARALLELISM_INCR_HEAVY:-2}"
      ;;
    user|user_product|user_bankcard|user_info|id_mapping)
      echo "${FLINK_PARALLELISM_INCR_LIGHT:-2}"
      ;;
    *)
      echo "$incr_default"
      ;;
  esac
}

# 估算多 Job 增量峰值 slot（user_info 按专用并行度计）
sync_jobs_peak_incr_slots() {
  local jobs=("$@")
  local total=0
  local job par
  for job in "${jobs[@]}"; do
    par=$(sync_job_parallelism "$job" incr)
    total=$((total + par))
  done
  echo "$total"
}
