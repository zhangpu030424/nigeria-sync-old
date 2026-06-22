#!/usr/bin/env bash
# 尼日迁移分批灌数
#
# 【跨机迁移】源库与目标库不在同一实例时，请用 Python 脚本：
#   cd docs/migration/ng-migration-runner && pip install pymysql
#   cp ng_migration.env.example ng_migration.env   # 填密码
#   python3 ng_migration_run.py user
#
# 【同机迁移】源库 ng_loan_market 与目标库 id 在同一 MySQL 时可本脚本循环
#
set -euo pipefail

MYSQL="${MYSQL:-mysql -h127.0.0.1 -uroot}"
DB="${DB:-id}"
USER_BATCH="${USER_BATCH:-5000}"
MAX_USER_ID="${MAX_USER_ID:-9153604}"
PROGRESS="${PROGRESS:-/tmp/ng_mig_progress.env}"
LO="${LO:-}"
HI="${HI:-}"

run_sql() {
  "$MYSQL" "$DB" -e "$1"
}

load_progress() {
  if [[ -f "$PROGRESS" ]]; then
    # shellcheck disable=SC1090
    source "$PROGRESS"
  fi
}

save_progress() {
  local key=$1 val=$2
  touch "$PROGRESS"
  if grep -q "^${key}=" "$PROGRESS" 2>/dev/null; then
    sed -i.bak "s/^${key}=.*/${key}=${val}/" "$PROGRESS"
  else
    echo "${key}=${val}" >> "$PROGRESS"
  fi
}

migrate_user_batch() {
  local lo=$1 hi=$2
  echo "== user batch (${lo}, ${hi}] $(date '+%F %T') =="

  run_sql "SET SESSION unique_checks=0; SET SESSION foreign_key_checks=0;
SET @lo:=${lo}; SET @hi:=${hi};

INSERT INTO \`user\` (user_id,app_id,group_user_id,info_user_id,mobile,closed_time,reg_device_uuid,reg_time,test_flag,utm_source,utm_medium,utm_campaign,utm_content,utm_term,campaign_id,ad_group_id,advertiser_id)
SELECT u.id,u.appId,u.id,u.id,
CASE WHEN u.mobile LIKE '+234%' THEN u.mobile WHEN u.mobile LIKE '234%' THEN CONCAT('+',u.mobile) WHEN u.mobile LIKE '0%' THEN CONCAT('+234',SUBSTRING(u.mobile,2)) ELSE CONCAT('+234',u.mobile) END,
CASE WHEN u.isCancel IN (1,'1') THEN UNIX_TIMESTAMP(u.updated)*1000 ELSE 0 END,
IFNULL(CAST(u.deviceId AS CHAR),''),UNIX_TIMESTAMP(u.created)*1000,0,
CASE UPPER(dac.channel) WHEN 'ORGANIC' THEN 'organic' WHEN 'FB' THEN 'facebook' WHEN 'TT' THEN 'tiktok' WHEN 'GG' THEN 'google' ELSE NULL END,
NULL,NULL,NULL,NULL,
CASE dac.channel WHEN 'GG' THEN dac.google_ads_campaign_id WHEN 'FB' THEN dac.fb_install_referrer_campaign_id ELSE NULL END,
CASE dac.channel WHEN 'GG' THEN dac.google_ads_adgroup_id WHEN 'FB' THEN dac.fb_install_referrer_campaign_group_id ELSE NULL END,
NULL
FROM ng_loan_market.\`user\` u
LEFT JOIN (
  SELECT dac1.deviceId, dac1.channel,
         dac1.google_ads_campaign_id, dac1.google_ads_adgroup_id,
         dac1.fb_install_referrer_campaign_id, dac1.fb_install_referrer_campaign_group_id
  FROM ng_loan_market.device_ad_channel dac1
  INNER JOIN (
    SELECT deviceId, MAX(id) AS max_id
    FROM ng_loan_market.device_ad_channel WHERE deviceId > 0
    GROUP BY deviceId
  ) t ON t.max_id = dac1.id
) dac ON dac.deviceId = u.deviceId AND u.deviceId > 0
WHERE u.id>@lo AND u.id<=@hi;

COMMIT;"
}

migrate_user_all() {
  load_progress
  local lo="${LO:-${user_lo:-0}}"
  local max="${MAX_USER_ID}"
  [[ -n "$HI" ]] && max="$HI"
  while [[ "$lo" -lt "$max" ]]; do
    local hi=$((lo + USER_BATCH))
    [[ "$hi" -gt "$max" ]] && hi=$max
    migrate_user_batch "$lo" "$hi"
    lo=$hi
    save_progress "user_lo" "$lo"
  done
  echo "user 系完成，进度: user_lo=$lo"
}

case "${1:-user}" in
  user) migrate_user_all ;;
  *)
    echo "usage: $0 user"
    echo "跨机请用: python3 ng_migration_run.py"
    exit 1
    ;;
esac
