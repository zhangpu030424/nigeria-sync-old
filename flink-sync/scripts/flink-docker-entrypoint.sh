#!/usr/bin/env bash
# 容器启动前确保 checkpoint 目录对 flink 用户可写（bind mount / named volume 均适用）
set -euo pipefail

mkdir -p /tmp/flink-checkpoints /tmp/flink-savepoints

if id flink &>/dev/null; then
  chown -R flink:flink /tmp/flink-checkpoints /tmp/flink-savepoints 2>/dev/null || true
fi
chmod -R a+rwx /tmp/flink-checkpoints /tmp/flink-savepoints 2>/dev/null || true

exec /docker-entrypoint.sh "$@"
