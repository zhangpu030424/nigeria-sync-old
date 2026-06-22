# ng-migration-runner

尼日老库 → 目标库 跨机迁移脚本（Python + pymysql）

每批从源库 SELECT，Python 组装后直接 bulk 写入目标库正式表（`user` / `user_info` / `application` / `loan` 等），**不使用** `dt_mig_*` 物化表。

## 快速开始

```bash
cp ng_migration.env.example ng_migration.env
# 编辑 ng_migration.env 填写 SOURCE_* / TARGET_*

pip install pymysql
# 可选，加速 user_info / application JSON 组装
pip install orjson

# 推荐：全量同步 user + application
./ng_migration_all.sh
# 或
python3 ng_migration_run.py full

# 分步
python3 ng_migration_run.py user
python3 ng_migration_run.py application
python3 ng_migration_run.py verify

# 手动删除目标库遗留 dt_mig_* 表
python3 ng_migration_run.py drop_staging
```

## 启动前清理（脚本内自动）

`python3 ng_migration_run.py full` 或 `user` 时，**Python 内部**自动：

1. `DROP` 目标库全部 `dt_mig_*` 遗留表（**不删正式表**）
2. 重置 `PROGRESS_FILE`（全新全量）

默认 `DROP_MAT_ON_START=1`。断点续跑设 `DROP_MAT_ON_START=0`。

## 性能日志（PERF）

每步输出统一格式，便于 `grep PERF` 分析瓶颈：

```
[W0 batch (0,50000]] PERF table=user phase=target_insert rows=50000 elapsed=8.50s (5882.4 rows/s)
== PERF SUMMARY [FULL] ... 按耗时排序各表各阶段
```

## 高速全量

```bash
chmod +x ng_migration_all.sh ng_migration_fast.sh
./ng_migration_all.sh
```

默认（已调优）：

| 参数 | 值 | 说明 |
|------|-----|------|
| `WORKERS` | 10 | user 并行 worker |
| `APP_WORKERS` | 8 | application 并行 worker（略降并发减死锁） |
| `USER_BATCH` | 20000 | 单批 user 数量 |
| `USER_INSERT_BATCH` | 20000 | 目标库多行 INSERT 批次 |
| `APP_INSERT_BATCH` | 10000 | application/loan 写入批次 |
| `ID_MAPPING_INSERT_BATCH` | 25000 | id_mapping UPSERT 批次（mapping 行多，单独加大） |
| `APP_WORKER_BALANCE` | count | application worker 分段：`count` 按订单量均分，`id` 按 id 等分 |
| `LOOKUP_PARALLEL` | 4 | 每批内并行读源库路数 |
| `PROGRESS_SAVE_EVERY` | 3 | 每 N 批写一次进度文件 |
| `DEADLOCK_MAX_RETRIES` | 8 | 死锁 1213 批级重试次数 |
| `LUP_PAIR_CHUNK` | 400 | password 按 (appId,mobile) IN 分块大小 |
| `VT_PRELOAD` | 1 | 启动时全量预加载 VT 字典到内存 |
| `VT_TOKEN_ENABLE` | 1 | 写入前用 token 替代明文 |
| `VT_TOKEN_CHUNK` | 2000 | VT 未预加载时每批 IN 查询条数 |
| `VT_TOKEN_DB` | ng_loan_market | 源库上 vt_token_cache 所在 schema |

脚本内优化（不改源/目标表结构）：先查 `user` 再 keyed lookup（lup/ud/uri/dac）、VT 内存预加载、长查询后目标连接 `ping` 重连、`orjson`（可选）、目标库 `unique_checks=0` / `foreign_key_checks=0`。

## VT 敏感字段加密

开启 `VT_TOKEN_ENABLE=1`（默认）时：

1. `VT_PRELOAD=1`：启动时一次性将 `vt_token_cache`（`status=1`）载入内存，批内 O(1) 查 token
2. 每批收集明文（手机号、BVN、银行卡号、gaid 等）后在内存字典查找
3. 命中则写入 `token`，**未命中则跳过该条数据**（不写明文；日志 `vt=preload` 或 hit/miss）

涉及字段：`user.mobile`、`user_info.id_number`、`user_info.info.emergency_contacts[].mobile`、`user_bankcard.bank_account_number`、`application.mobile` / `gaid_idfa` / `bank_account_number` / `id_number`。

## id_mapping

`application` 阶段同步写入 `id_mapping`（与 application/loan 同批）：

1. 源库按 `application.id ASC` 遍历
2. 每条申请按固定 type 顺序展开：`mobile` → `gaid_idfa` → `device_uuid` → `bank_account` → `id_number` → `id2`
3. 锚点 `id` = 手机号 VT token；`device_uuid` 保持明文；其余敏感字段走 `vt_token_cache`（未命中则跳过该行）
4. 主键冲突时 `ON DUPLICATE KEY UPDATE event_time`（源序靠后的申请覆盖）

## 多 worker 手动分段

```bash
LO=0        HI=3000000 WORKERS=1 PROGRESS_FILE=/tmp/mig_w1.env python3 ng_migration_run.py user &
LO=3000000  HI=6000000 WORKERS=1 PROGRESS_FILE=/tmp/mig_w2.env python3 ng_migration_run.py user &
wait
```

## 配置项

`SOURCE_*` / `TARGET_*` / `USER_BATCH` / `USER_INSERT_BATCH` / `WORKERS` / `APP_WORKERS` / `LOOKUP_PARALLEL` / `LUP_PAIR_CHUNK` / `VT_PRELOAD` / `VT_TOKEN_ENABLE` / `VT_TOKEN_CHUNK` / `VT_TOKEN_DB` / `DROP_MAT_ON_START` / `DEADLOCK_MAX_RETRIES` / `LOG_FILE`
