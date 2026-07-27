#!/usr/bin/env bash
# ng_loan_market 源库 MySQL 客户端
# 依赖: MARKET_MYSQL_HOST/PORT/USER/PASSWORD/DATABASE

mysql_market_cmd() {
  local args=("$@")
  local connect_timeout="${MARKET_MYSQL_CONNECT_TIMEOUT:-15}"
  MYSQL_PWD="${MARKET_MYSQL_PASSWORD}" mysql --connect-timeout="${connect_timeout}" \
    -h "${MARKET_MYSQL_HOST}" -P "${MARKET_MYSQL_PORT:-3306}" \
    -u "${MARKET_MYSQL_USER}" "${MARKET_MYSQL_DATABASE}" "${args[@]}"
}

mysql_market_query() {
  mysql_market_cmd -N -e "$1"
}
