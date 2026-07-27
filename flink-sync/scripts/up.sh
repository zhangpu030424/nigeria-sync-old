#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo "请先复制配置: cp .env.example .env 并填写数据库连接"
  exit 1
fi

# Flink 进程以 flink 用户运行；checkpoint 目录须可写（named volume 常为 root 导致 mkdir 失败）
mkdir -p data/flink-checkpoints data/flink-savepoints
chmod 777 data/flink-checkpoints data/flink-savepoints

docker compose --env-file .env up -d --build
echo ">> 校验 checkpoint 目录可写..."
sleep 3
./scripts/fix-flink-checkpoints.sh || true
echo "Flink 已启动，Web UI: http://$(hostname -I 2>/dev/null | awk '{print $1}' || echo localhost):${FLINK_WEB_PORT:-8089}"
echo "TaskManager: slots=${FLINK_TASK_SLOTS:-16} memory=${FLINK_TM_MEMORY:-40960m}"
