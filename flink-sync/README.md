# Nigeria Sync — Flink 实时增量同步

将 `ng_loan_market` + `ng_loan_core` 的 binlog 变更实时同步到目标库 `ng`，字段口径与 `ng_migration_run.py` / `reconcile_tables.py` 对齐。

架构参考 [nigeria-flink-sync](file:///Users/zhangpu/Documents/Java/nigeria-flink-sync)：**CDC 触发 + MySQL Lookup 视图组装 + JDBC Sink UPSERT**。

## 同步表

| 顺序 | 目标表 | CDC 触发源 | 组装 |
|------|--------|-----------|------|
| 1 | user | user, log_user_password, device_ad_channel | `user_incr_lookup` |
| 2 | user_info | user, user_data, user_reg_ip | `user_info_incr_bundle_lookup` |
| 3 | user_bankcard | user_data | `user_bankcard_incr_lookup` + 雪花 ID |
| 4 | user_product | application | `user_product_incr_lookup` |
| 5 | application | market.application, core.application, user_data | `application_incr_bundle_lookup` |
| 6 | loan | repay_plan, repay_record, market.application | `loan_incr_bundle_lookup` |

## 与批处理的关系

```
历史全量 / 大窗口修复          实时增量 (本目录)           兜底对账 (上级目录)
─────────────────────    ─────────────────────    ─────────────────────
ng_migration_run.py   →   Flink CDC Jobs      →   reconcile_tables.py
window_upsert.py          (秒级延迟)              run_reconcile_cron.sh
```

- **首次上线**：目标库若尚无数据，先用上级目录 `ng_migration_all.sh` 跑全量，再启动 Flink 增量。
- **已全量**：可直接 `./scripts/sync-incr-auto.sh`（`CDC_STARTUP_MODE=initial` 会先快照补漏再追 binlog）。
- **兜底**：保留 `run_reconcile_cron.sh` 滚动对账（建议缩到 7 天窗口）。

## 快速开始

### 1. 源库授权

在 MySQL 为 CDC 账号授权（示例）：

```sql
CREATE USER 'flink_cdc'@'%' IDENTIFIED BY '***';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'flink_cdc'@'%';
GRANT SELECT ON ng_loan_market.* TO 'flink_cdc'@'%';
GRANT SELECT ON ng_loan_core.* TO 'flink_cdc'@'%';
FLUSH PRIVILEGES;
```

### 2. 配置

```bash
cd flink-sync
cp .env.example .env
# 填写 MARKET_MYSQL_* / CORE_MYSQL_* / TARGET_MYSQL_*
```

### 3. 启动 Flink

```bash
./scripts/up.sh
# Web UI: http://<服务器>:8090
```

### 4. 部署 Lookup 视图 + 提交增量 Job

```bash
./scripts/sync-incr-auto.sh
```

仅提交部分表：

```bash
./scripts/sync-incr-auto.sh --jobs user,application,loan
```

全量已覆盖、只追新变更：

```bash
./scripts/sync-incr-auto.sh --startup-mode latest-offset --keep-jobs
```

## 目录结构

```
flink-sync/
├── config/sync-jobs.conf      # Job 编排
├── sql/
│   ├── ddl/market_lookup_views.sql   # 源库 Lookup 视图（核心）
│   └── 02_sync_*_incr.sql            # 各表增量 Flink SQL
├── scripts/
│   ├── up.sh / down.sh
│   ├── deploy-source-ddl.sh
│   ├── sync-incr-auto.sh             # 一键提交增量
│   └── run-sql.sh
├── udf/                       # VT / 雪花 ID UDF（与 nigeria-flink-sync 同源）
├── docker-compose.yml
└── Dockerfile
```

## 关键设计

1. **双源库**：market 与 core 分别 CDC；跨库 JOIN 在 MySQL 视图内完成（同实例 `ng_loan_core.application`）。
2. **VT 令牌化**：优先读 `vt_token_cache`；miss 时 UDF `vt_tokenize` 调 `/v2t`（与批处理一致，未命中则跳过）。
3. **单号规则**：`application_no = ng{appId:04d}-{market.applicationNo}`；`loan_no = ng-{core_sn}-01000`。
4. **状态映射**：application / loan status 与 `ng_migration_run._map_*` 一致。
5. **server-id**：`run-sql.sh` 按并行度自动分配 CDC server-id 段，避免多 Job 冲突。

## 运维

```bash
# 查看 Running Jobs
docker exec ng-sync-flink-jobmanager ./bin/flink list -r

# 取消全部 Job
./scripts/cancel-flink-jobs.sh --yes

# 强制重建 Lookup 视图（须先 cancel Job）
./scripts/deploy-source-ddl.sh --force

# 停止集群
./scripts/down.sh
```

## 注意事项

- `application` 需 market 与 core 均建档（有 `core_sn`）才写入；core 晚到会由 core.application CDC 触发补全。
- `user_info.info` JSON 当前为简化版；复杂 emergency_contacts 等可后续在视图层补全。
- 生产建议配合 `run_reconcile_cron.sh` 作延迟一致性兜底，而非单独依赖 Flink。
