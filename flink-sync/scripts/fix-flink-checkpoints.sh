#!/usr/bin/env bash
# 修复 checkpoint 目录权限（服务器上 Job 报 Failed to create directory for shared state 时执行）
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p data/flink-checkpoints data/flink-savepoints
chmod -R a+rwx data/flink-checkpoints data/flink-savepoints

JM="${FLINK_JOBMANAGER_CONTAINER:-ng-sync-flink-jobmanager}"
TM="${FLINK_TASKMANAGER_CONTAINER:-ng-sync-flink-taskmanager}"

for c in "$JM" "$TM"; do
  if docker ps --format '{{.Names}}' | grep -qx "$c"; then
    echo ">> 修复容器 $c 内 /tmp/flink-checkpoints"
    docker exec -u 0 "$c" bash -c \
      'mkdir -p /tmp/flink-checkpoints /tmp/flink-savepoints && chown -R flink:flink /tmp/flink-checkpoints /tmp/flink-savepoints && chmod -R a+rwx /tmp/flink-checkpoints /tmp/flink-savepoints'
    docker exec -u flink "$c" bash -c 'touch /tmp/flink-checkpoints/.write_test && rm -f /tmp/flink-checkpoints/.write_test && echo OK'
  else
    echo ">> 跳过（未运行）: $c"
  fi
done

echo ">> 宿主机目录:"
ls -la data/flink-checkpoints data/flink-savepoints 2>/dev/null | head -5
echo ">> 完成。若仍失败请: docker compose down && ./scripts/up.sh --build"
