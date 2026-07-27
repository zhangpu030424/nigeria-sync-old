#!/usr/bin/env bash
# 取消所有 Running Flink Job，并清理 checkpoint（从根上避免 restore 已 purge 的 binlog 位点）
# 用法: bash scripts/cancel-flink-jobs.sh --yes
#       bash scripts/cancel-flink-jobs.sh --yes --keep-checkpoints
set -euo pipefail
cd "$(dirname "$0")/.."

JM="${FLINK_JOBMANAGER_CONTAINER:-ng-sync-flink-jobmanager}"
YES=0
PURGE_CKPT=1
for arg in "$@"; do
  case "$arg" in
    --yes|-y) YES=1 ;;
    --keep-checkpoints) PURGE_CKPT=0 ;;
  esac
done

if ! docker ps --format '{{.Names}}' | grep -qx "$JM"; then
  echo "ERR: ${JM} 未运行"
  exit 1
fi

mapfile -t JIDS < <(docker exec "$JM" ./bin/flink list 2>/dev/null \
  | grep -oE '[a-f0-9]{32}' | sort -u || true)

if [[ ${#JIDS[@]} -eq 0 ]]; then
  echo ">> 无 Running Job"
else
  echo ">> 将取消 ${#JIDS[@]} 个 Job:"
  docker exec "$JM" ./bin/flink list 2>/dev/null || true
  if [[ "$YES" -ne 1 ]]; then
    read -r -p "确认 cancel? [y/N] " ans
    [[ "${ans,,}" == "y" || "${ans,,}" == "yes" ]] || { echo "已取消"; exit 0; }
  fi
  for jid in "${JIDS[@]}"; do
    echo ">> cancel $jid"
    docker exec "$JM" ./bin/flink cancel "$jid" 2>/dev/null || true
  done
  sleep 3
  echo ">> 剩余 Job:"
  docker exec "$JM" ./bin/flink list 2>/dev/null || true
fi

if [[ "$PURGE_CKPT" -eq 1 ]]; then
  bash scripts/purge-flink-checkpoints.sh
else
  echo ">> 保留 checkpoint（--keep-checkpoints）"
fi

bash scripts/check-flink-slots.sh || true
