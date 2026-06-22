#!/usr/bin/env bash
# 部署到迁移机并后台启动全量同步
# FRESH_START=1（默认）全新全量；FRESH_START=0 断点续跑（保留进度与 DROP_MAT_ON_START=0）
set -eo pipefail

REMOTE_HOST="${REMOTE_HOST:-165.154.176.95}"
REMOTE_USER="${REMOTE_USER:-root}"
REMOTE_DIR="${REMOTE_DIR:-/opt/ng-migration-runner}"
FRESH_START="${FRESH_START:-1}"
START_MIGRATION="${START_MIGRATION:-1}"
HERE="$(cd "$(dirname "$0")" && pwd)"

if [[ -z "${SSHPASS:-}" && -f "$HERE/.deploy_ssh_pass" ]]; then
  SSHPASS="$(tr -d '\n' < "$HERE/.deploy_ssh_pass")"
  export SSHPASS
fi

if [[ -z "${SSHPASS:-}" ]]; then
  echo "请设置 SSHPASS 或创建 $HERE/.deploy_ssh_pass" >&2
  exit 1
fi

if [[ "$FRESH_START" == "1" ]]; then
  DROP_MAT_ON_START=1
  echo "== 模式: 全新全量 (FRESH_START=1, DROP_MAT_ON_START=1) =="
else
  DROP_MAT_ON_START=0
  echo "== 模式: 断点续跑 (FRESH_START=0, DROP_MAT_ON_START=0) =="
fi

SSH_OPTS=(
  -o StrictHostKeyChecking=accept-new
  -o ConnectTimeout=30
  -o PreferredAuthentications=password
  -o PubkeyAuthentication=no
)
RSYNC_SSH="ssh ${SSH_OPTS[*]}"

echo "== 1. 同步代码到 ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR} =="
SSHPASS="$SSHPASS" sshpass -e ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${REMOTE_HOST}" \
  "mkdir -p '${REMOTE_DIR}' && chmod 755 '${REMOTE_DIR}'"

SSHPASS="$SSHPASS" sshpass -e rsync -avz \
  --exclude '.git' \
  --exclude 'ng_migration.env' \
  --exclude '.deploy_ssh_pass' \
  --exclude '_*.py' \
  -e "$RSYNC_SSH" \
  "$HERE/" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/"

echo "== 2. 写入远程 ng_migration.env =="
# 密码含 $ 须转义；DROP_MAT_ON_START 需本地展开故 heredoc 不加引号
SSHPASS="$SSHPASS" sshpass -e ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${REMOTE_HOST}" "cat > '${REMOTE_DIR}/ng_migration.env'" <<ENV
SOURCE_HOST=10.52.139.200
SOURCE_PORT=3306
SOURCE_USER=root
SOURCE_PASSWORD='4-3kM^B0\$I2s'

TARGET_HOST=101.47.15.219
TARGET_PORT=8001
TARGET_USER=ng-export
TARGET_PASSWORD='babzubfvkv6y43xbA'
TARGET_DB=ng

MAX_USER_ID=9153604
PROGRESS_FILE=/tmp/ng_mig_all_progress.env
LOG_FILE=/tmp/ng_mig_all.log
SKIP_LOG_FILE=/tmp/ng_mig_all.skip.log
DROP_MAT_ON_START=${DROP_MAT_ON_START}
VT_TOKEN_ENABLE=1
VT_TOKEN_CHUNK=2000
VT_TOKEN_DB=ng_loan_market
VT_PRELOAD=1
LUP_PRELOAD=1
WORKERS=10
APP_WORKERS=8
LOOKUP_PARALLEL=4
USER_BATCH=20000
USER_INSERT_BATCH=20000
APP_BATCH=100000
APP_INSERT_BATCH=10000
ID_MAPPING_INSERT_BATCH=25000
APP_WORKER_BALANCE=count
DEADLOCK_MAX_RETRIES=8
INSERT_ROW_RETRIES=3
PROGRESS_SAVE_EVERY=3
LOG_EVERY=20
ENV

SSHPASS="$SSHPASS" sshpass -e ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${REMOTE_HOST}" \
  "chmod 600 '${REMOTE_DIR}/ng_migration.env'"

echo "== 3. 安装依赖 =="
SSHPASS="$SSHPASS" sshpass -e ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${REMOTE_HOST}" \
  "pip3 install -q pymysql 2>/dev/null || pip3 install --user -q pymysql; \
   pip3 install -q 'orjson>=3.0' 2>/dev/null || pip3 install --user -q 'orjson>=3.0' 2>/dev/null || true"

echo "== 4. 停止旧任务并启动全量 =="
if [[ "$START_MIGRATION" == "0" ]]; then
  echo "== 跳过启动 (START_MIGRATION=0，仅部署代码与 env) =="
else
SSHPASS="$SSHPASS" sshpass -e ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${REMOTE_HOST}" bash <<REMOTE
set -eo pipefail
cd '${REMOTE_DIR}'
chmod +x ng_migration_all.sh ng_migration_fast.sh
pkill -f 'ng_migration_run.py' 2>/dev/null || true
pkill -f 'ng_migration_all.sh' 2>/dev/null || true
pkill -f 'ng_migration_partial.sh' 2>/dev/null || true
sleep 2
FRESH_START='${FRESH_START}'
if [[ "\$FRESH_START" == "1" ]]; then
  rm -f /tmp/ng_mig_all_progress.env
  : > /tmp/ng_mig_all.log
  : > /tmp/ng_mig_all.skip.log
  echo "已清理进度与日志（全新全量）"
else
  echo "保留进度文件续跑:"
  cat /tmp/ng_mig_all_progress.env 2>/dev/null || echo "(无进度文件，从头发)"
fi
: > /tmp/ng_mig_all.nohup
nohup ./ng_migration_all.sh >> /tmp/ng_mig_all.nohup 2>&1 &
echo "started pid=\$!"
sleep 5
pgrep -af ng_migration || true
tail -20 /tmp/ng_mig_all.log 2>/dev/null || true
REMOTE
fi

echo "== 完成 =="
echo "  查看进度: ssh ${REMOTE_USER}@${REMOTE_HOST} 'tail -f /tmp/ng_mig_all.log'"
echo "  进度文件: /tmp/ng_mig_all_progress.env"
echo "  续跑部署: FRESH_START=0 START_MIGRATION=0 ./deploy_and_start.sh"
echo "  续跑启动: ssh ${REMOTE_USER}@${REMOTE_HOST} 'cd ${REMOTE_DIR} && DROP_MAT_ON_START=0 nohup ./ng_migration_all.sh >> /tmp/ng_mig_all.nohup 2>&1 &'"
