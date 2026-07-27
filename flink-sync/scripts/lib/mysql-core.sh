#!/usr/bin/env bash
# ng_loan_core 源库 MySQL 客户端
# 依赖: CORE_MYSQL_HOST/PORT/USER/PASSWORD/DATABASE

mysql_core_cmd() {
  local connect_timeout="${CORE_MYSQL_CONNECT_TIMEOUT:-15}"
  MYSQL_PWD="${CORE_MYSQL_PASSWORD}" mysql --connect-timeout="${connect_timeout}" \
    -h "${CORE_MYSQL_HOST}" -P "${CORE_MYSQL_PORT:-3306}" \
    -u "${CORE_MYSQL_USER}" "${CORE_MYSQL_DATABASE}" "$@"
}

mysql_core_query() {
  mysql_core_cmd -N -e "$1"
}
