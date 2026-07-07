# 尼日利亚目标库（ng）数据问题排查与修复手册

> 基于迁移/backfill 过程中发现的问题整理。目标库默认 schema 为 `ng`。  
> 单号规范：
> - `loan_no` = `ng-{core_sn}-01000`（中间段约 12 位 core sn）
> - `application_no` = `ng{appId:04d}-{marketApplicationNo}`（如 `ng0515-178072863512023153`）
> - `loan` 主键：`(application_no, period, roll_sequence)`

---

## 修复顺序（推荐）

| 步骤 | 内容 | 脚本 |
|------|------|------|
| 0 | 恢复 `delete_dup` 误删（如有） | `restore_loan_from_market_delete_dup.py` |
| 1 | `application_no` 第一批：sn 对齐 | `repair_loan_app_no_from_application.py` |
| 2 | `application_no` 第二批：market 源库补 appId | `repair_loan_app_no_from_market.py` |
| 3 | `application.status` ← `loan.status` | `sync_application_status_from_loan.py` |
| 4 | `loan_no` 长号（market 号在中间段） | `repair_loan_long_sn.py` |
| 5 | 重复 `loan_no` / status 同步 | `repair_loan_status20_from_source.py` |
| 6 | 可选：标记 `is_test=1` | `mark_application_is_test.py` |

**注意：** 经 IDEA/8001 代理对大表 JOIN UPDATE、`application` 表 pymysql 写入易超时/2013。  
loan 表写入相对快；`application` 相关修复优先 `--sql-out` + mysql 客户端执行。

---

## 问题 0：delete_dup 误删（事故恢复）

### 现象
旧版 `repair_loan_app_no_from_market.py` 在主键冲突时 **删掉了 `loan_no` 正确、`application_no` 错误的那行**，留下 `loan_no` 错误、`application_no` 正确的行。

日志特征：`REPAIR ... delete_dup,ng-217807666927-01000,ng20518694-...,ng0515-...,ok`

### 排查 SQL

```sql
-- 从 REPAIR 日志挑一条 delete_dup，看当前库状态
SELECT loan_no, application_no, period, roll_sequence, status, due_date
FROM loan
WHERE loan_no = 'ng-217807666927-01000'          -- 日志里的 correct loan_no
   OR application_no = 'ng0515-178076668612049815';  -- 日志里的 good app

-- 误删后典型状态：correct loan_no 不存在，good app 挂在另一个 loan_no 上
```

### 修复

```bash
# dry-run 诊断（自动找 /tmp/repair_loan_app_no_market*.csv）
python3 restore_loan_from_market_delete_dup.py --env ./ng_migration.env --dry-run

# 导出 SQL（推荐 IDEA 执行）
python3 restore_loan_from_market_delete_dup.py --env ./ng_migration.env \
  --repair-log '/tmp/repair_loan_app_no_market*.csv' \
  --sql-out /tmp/restore_delete_dup.sql

# 或直接 apply
python3 restore_loan_from_market_delete_dup.py --env ./ng_migration.env \
  --repair-log '/tmp/repair_loan_app_no_market*.csv' --apply
```

单条修复 SQL 模板（`fix_loan_no`）：

```sql
START TRANSACTION;
UPDATE loan
SET loan_no = 'ng-217807666927-01000'              -- 日志 correct loan_no
WHERE loan_no = 'ng-178076668612049815-01000'      -- 当前占着 good app 的错误 loan_no
  AND application_no = 'ng0515-178076668612049815'
  AND period = 1 AND roll_sequence = 0;
COMMIT;
```

---

## 问题 1：loan.application_no 前缀异常（ng + 5 位以上数字）

### 现象
`application_no` 形如 `ng20515427-178072863512023153`，正确应为 `ng0515-178072863512023153`。  
`loan_no` 通常已是短号 `ng-217807286381-01000`。

### 排查 SQL

```sql
-- A1: 异常 application_no 前缀总数
SELECT COUNT(*) AS bad_prefix_cnt
FROM loan
WHERE application_no REGEXP '^ng[0-9]{5,}-'
  AND loan_no REGEXP '^[Nn][Gg]-[0-9]+-[0-9]{5}$';

-- A1 明细（抽样）
SELECT loan_no, application_no, period, roll_sequence, status, due_date
FROM loan
WHERE application_no REGEXP '^ng[0-9]{5,}-'
  AND loan_no REGEXP '^[Nn][Gg]-[0-9]+-[0-9]{5}$'
ORDER BY loan_no
LIMIT 50;

-- A2: 可用 sn 对齐修（目标 application 表有对应 sn）
SELECT COUNT(*) AS fixable_by_sn
FROM loan l
INNER JOIN application a
  ON a.sn = SUBSTRING_INDEX(SUBSTRING_INDEX(l.loan_no, '-', 2), '-', -1)
WHERE l.application_no <> a.application_no
  AND l.loan_no REGEXP '^[Nn][Gg]-[0-9]+-[0-9]{5}$'
  AND l.application_no REGEXP '^ng[0-9]{5,}-';

-- A2 明细
SELECT
  l.loan_no,
  l.application_no AS bad_application_no,
  a.application_no AS good_application_no
FROM loan l
INNER JOIN application a
  ON a.sn = SUBSTRING_INDEX(SUBSTRING_INDEX(l.loan_no, '-', 2), '-', -1)
WHERE l.application_no <> a.application_no
  AND l.loan_no REGEXP '^[Nn][Gg]-[0-9]+-[0-9]{5}$'
  AND l.application_no REGEXP '^ng[0-9]{5,}-'
ORDER BY l.loan_no
LIMIT 50;

-- A3: sn 对不上、需走 market 源库（目标 application 无 sn）
SELECT COUNT(*) AS need_market_lookup
FROM loan l
WHERE l.application_no REGEXP '^ng[0-9]{5,}-'
  AND l.loan_no REGEXP '^[Nn][Gg]-[0-9]+-[0-9]{5}$'
  AND NOT EXISTS (
    SELECT 1 FROM application a
    WHERE a.sn = SUBSTRING_INDEX(SUBSTRING_INDEX(l.loan_no, '-', 2), '-', -1)
  );
```

**历史规模：** sn 可对齐约 **4785** 条；market 第二批为 A1 减 A2 后的剩余。

### 修复

#### 第一批：sn 对齐（目标库自洽）

```bash
# 建 plan
python3 repair_loan_app_no_from_application.py --env ./ng_migration.env --dry-run \
  --load-all --plan-file /tmp/fix_app_no_plan.json

# 批更新（4 进程 × 每批 200）
python3 repair_loan_app_no_from_application.py --env ./ng_migration.env --apply-only \
  --plan-file /tmp/fix_app_no_plan.json --batch-size 200 --workers 4

# 或导出 SQL
python3 repair_loan_app_no_from_application.py --env ./ng_migration.env --dry-run \
  --plan-file /tmp/fix_app_no_plan.json --sql-out /tmp/fix_app_no.sql
```

等价修复 SQL（单条）：

```sql
UPDATE loan l
INNER JOIN application a
  ON a.sn = SUBSTRING_INDEX(SUBSTRING_INDEX(l.loan_no, '-', 2), '-', -1)
SET l.application_no = a.application_no
WHERE l.loan_no = 'ng-217807286381-01000'
  AND l.application_no = 'ng20515427-178072863512023153'
  AND l.application_no <> a.application_no;
```

批修复 SQL 模板（脚本内部合成）：

```sql
UPDATE loan l
INNER JOIN (
  SELECT 'ng-217807286381-01000' AS loan_no,
         'ng20515427-178072863512023153' AS bad_app,
         'ng0515-178072863512023153' AS good_app
  UNION ALL
  SELECT '...', '...', '...'
) x ON l.loan_no = x.loan_no AND l.application_no = x.bad_app
SET l.application_no = x.good_app;
```

#### 第二批：market 源库查 appId

后缀 = `application_no` 中第一个 `-` 之后的长号，如 `178072863512023153`。

```sql
-- 源库（ng_loan_market）查 appId
SELECT appId, applicationNo
FROM ng_loan_market.application
WHERE applicationNo = '178072863512023153';
-- 拼成 ng{appId:04d}-178072863512023153
```

```bash
python3 repair_loan_app_no_from_market.py --env ./ng_migration.env --dry-run \
  --plan-file /tmp/fix_app_no_market_plan.json

python3 repair_loan_app_no_from_market.py --env ./ng_migration.env --apply-only \
  --plan-file /tmp/fix_app_no_market_plan.json --batch-size 200 --workers 4
```

单条修复 SQL：

```sql
UPDATE loan
SET application_no = 'ng0515-178072863512023153'
WHERE loan_no = 'ng-217807286381-01000'
  AND application_no = 'ng20515427-178072863512023153';
```

**主键冲突：** 当前脚本 **只 skip 不 DELETE**。日志 `skip pk_conflict:good_app_taken_by:...` 需人工处理。

```sql
-- 排查主键冲突：good app 已被另一 loan_no 占用
SELECT l1.loan_no AS plan_loan_no, l1.application_no AS bad_app,
       l2.loan_no AS conflict_loan_no, l2.application_no AS good_app
FROM loan l1
JOIN loan l2
  ON l2.application_no = CONCAT('ng0515-', SUBSTRING_INDEX(l1.application_no, '-', -1))  -- 示例，实际 good 从 plan 来
 AND l2.period = l1.period AND l2.roll_sequence = l1.roll_sequence
 AND l2.loan_no <> l1.loan_no
WHERE l1.application_no REGEXP '^ng[0-9]{5,}-'
LIMIT 20;
```

---

## 问题 2：application.status 与 loan.status 不一致

### 现象
`loan` 已同步为 23/27/24 等终态，`application` 仍停在 **20**（进行中）。  
需在 `application_no` 修复完成、JOIN 能匹配后再做。

### 排查 SQL

```sql
-- B: status 不一致（app=20，loan 已变）
SELECT COUNT(*) AS status_mismatch_cnt
FROM loan l
INNER JOIN application a ON a.application_no = l.application_no
WHERE l.due_date <= '2026-07-05'
  AND a.status = 20
  AND l.status <> a.status;

-- 按 loan.status 分布
SELECT l.status AS loan_status, COUNT(*) AS cnt
FROM loan l
INNER JOIN application a ON a.application_no = l.application_no
WHERE l.due_date <= '2026-07-05'
  AND a.status = 20
  AND l.status <> a.status
GROUP BY l.status
ORDER BY cnt DESC;

-- 明细
SELECT l.loan_no, l.application_no, l.status AS loan_status,
       a.status AS app_status, l.due_date
FROM loan l
INNER JOIN application a ON a.application_no = l.application_no
WHERE l.due_date <= '2026-07-05'
  AND a.status = 20
  AND l.status <> a.status
ORDER BY l.loan_no
LIMIT 50;
```

**历史规模：** 约 **4290** 条，分布 `{27:2464, 23:1749, 24:77}`。

### 修复

```bash
python3 sync_application_status_from_loan.py --env ./ng_migration.env --dry-run \
  --due-before 2026-07-05 --app-status 20 \
  --plan-file /tmp/sync_app_status_plan.json

python3 sync_application_status_from_loan.py \
  --plan-file /tmp/sync_app_status_plan.json --sql-out /tmp/sync_app_status.sql

mysql -h<host> -P<port> -u<user> -p ng < /tmp/sync_app_status.sql
```

单条 / 批量修复 SQL：

```sql
START TRANSACTION;
UPDATE application SET status = 27
WHERE application_no = 'ng0515-178072863512023153'
  AND status = 20;
-- ... 每批 50~200 条
COMMIT;
```

JOIN 批量修复（小范围可用，大范围易超时）：

```sql
UPDATE application a
INNER JOIN loan l ON l.application_no = a.application_no
SET a.status = l.status
WHERE l.due_date <= '2026-07-05'
  AND a.status = 20
  AND l.status <> a.status;
```

---

## 问题 3：loan_no 中间段为 market 长号（15~18 位）

### 现象

```
错误: loan_no = ng-178076668612049815-01000   （18 位 market 号）
正确: loan_no = ng-217807666927-01000          （12 位 core sn）
application_no 可能已是 ng0515-178076668612049815（长号格式正确）
```

### 排查 SQL

```sql
-- C: loan_no 中间段 >= 15 位
SELECT COUNT(*) AS long_loan_no_cnt
FROM loan
WHERE loan_no REGEXP '^[Nn][Gg]-[0-9]{15,}-[0-9]{5}$';

SELECT loan_no, application_no, status, due_date
FROM loan
WHERE loan_no REGEXP '^[Nn][Gg]-[0-9]{15,}-[0-9]{5}$'
ORDER BY loan_no
LIMIT 50;

-- 能否从 application 表拿到 core sn
SELECT l.loan_no, l.application_no, a.sn AS core_sn,
       CONCAT('ng-', a.sn, '-', SUBSTRING_INDEX(l.loan_no, '-', -1)) AS should_be_loan_no
FROM loan l
LEFT JOIN application a ON a.application_no = l.application_no
WHERE l.loan_no REGEXP '^[Nn][Gg]-[0-9]{15,}-[0-9]{5}$'
LIMIT 50;
```

### 修复

```bash
python3 repair_loan_long_sn.py --env ./ng_migration.env --dry-run --min-sn-len 15
python3 repair_loan_long_sn.py --env ./ng_migration.env --apply --min-sn-len 15 --commit-every 20
```

单条修复 SQL（短号行不存在时）：

```sql
UPDATE loan l
INNER JOIN application a ON a.application_no = l.application_no
SET l.loan_no = CONCAT('ng-', a.sn, '-', SUBSTRING_INDEX(l.loan_no, '-', -1))
WHERE l.loan_no = 'ng-178076668612049815-01000'
  AND a.sn IS NOT NULL AND a.sn <> '';
```

若正确短号行已存在，需人工决定删长号行或合并（见问题 4）。

---

## 问题 4：同一 loan_no 多行 / application_no 后缀错误

### 现象
- 同一 `loan_no` 对应多条 `loan`，`application_no` 不同
- 有的后缀误用 core sn（如 `ng0564-217819556201`），应为 market 长号

### 排查 SQL

```sql
-- D: 重复 loan_no
SELECT loan_no, COUNT(*) AS cnt
FROM loan
GROUP BY loan_no
HAVING cnt > 1
ORDER BY cnt DESC
LIMIT 50;

-- 重复 loan_no 明细
SELECT loan_no, application_no, period, roll_sequence, status, due_date
FROM loan
WHERE loan_no IN (
  SELECT loan_no FROM loan GROUP BY loan_no HAVING COUNT(*) > 1
)
ORDER BY loan_no, application_no;

-- application_no 后缀像 core sn（12 位左右）而非 market 长号（15~18 位）
SELECT loan_no, application_no
FROM loan
WHERE application_no REGEXP '^ng[0-9]{4}-[0-9]{12}$'
LIMIT 50;
```

### 修复

```bash
python3 repair_loan_status20_from_source.py --env ./ng_migration.env --list-dup
python3 repair_loan_status20_from_source.py --env ./ng_migration.env --dry-run --plan-only
python3 repair_loan_status20_from_source.py --env ./ng_migration.env --apply --workers 1
```

原则：保留 **market canonical** 的 `application_no` 行，删/改后缀为 core sn 的错行（执行前务必 dry-run + 审计 CSV）。

---

## 问题 5：loan 与 application JOIN 不上

### 排查 SQL

```sql
-- loan 有 application_no 但 application 表无记录
SELECT COUNT(*) AS orphan_loan_cnt
FROM loan l
LEFT JOIN application a ON a.application_no = l.application_no
WHERE l.application_no IS NOT NULL AND l.application_no <> ''
  AND a.application_no IS NULL;

SELECT l.loan_no, l.application_no
FROM loan l
LEFT JOIN application a ON a.application_no = l.application_no
WHERE a.application_no IS NULL
  AND l.application_no IS NOT NULL AND l.application_no <> ''
LIMIT 50;
```

此类需回到增量同步或源库补数，无通用自动 SQL。

---

## 问题 6：可选 — 标记 is_test=1

### 排查 SQL

```sql
SELECT COUNT(*) AS would_mark_test
FROM application a
INNER JOIN (
  SELECT DISTINCT application_no
  FROM loan
  WHERE due_date < '2026-07-05' AND status = 20
    AND application_no IS NOT NULL AND application_no <> ''
) l ON a.application_no = l.application_no
WHERE a.is_test IS NULL OR a.is_test <> 1;
```

### 修复

```bash
python3 mark_application_is_test.py --env ./ng_migration.env --dry-run \
  --due-before 2026-07-05 --status 20
python3 mark_application_is_test.py --env ./ng_migration.env --apply \
  --due-before 2026-07-05 --status 20
```

```sql
UPDATE application a
INNER JOIN (
  SELECT DISTINCT application_no FROM loan
  WHERE due_date < '2026-07-05' AND status = 20
) l ON a.application_no = l.application_no
SET a.is_test = 1
WHERE a.is_test IS NULL OR a.is_test <> 1;
```

---

## 全量验收 SQL（修复后跑一遍）

```sql
-- 1. 异常 app 前缀应 → 0（或只剩 market 查不到的残留）
SELECT COUNT(*) AS remain_bad_prefix FROM loan
WHERE application_no REGEXP '^ng[0-9]{5,}-'
  AND loan_no REGEXP '^[Nn][Gg]-[0-9]+-[0-9]{5}$';

-- 2. sn 与 app 不一致应 → 0
SELECT COUNT(*) AS remain_sn_mismatch FROM loan l
INNER JOIN application a
  ON a.sn = SUBSTRING_INDEX(SUBSTRING_INDEX(l.loan_no, '-', 2), '-', -1)
WHERE l.application_no <> a.application_no
  AND l.loan_no REGEXP '^[Nn][Gg]-[0-9]+-[0-9]{5}$';

-- 3. status 不一致（app=20）应 → 0
SELECT COUNT(*) AS remain_status_mismatch FROM loan l
JOIN application a ON a.application_no = l.application_no
WHERE l.due_date <= '2026-07-05' AND a.status = 20 AND l.status <> a.status;

-- 4. 长号 loan_no 应 → 0
SELECT COUNT(*) AS remain_long_loan_no FROM loan
WHERE loan_no REGEXP '^[Nn][Gg]-[0-9]{15,}-[0-9]{5}$';

-- 5. 重复 loan_no 应 → 0
SELECT COUNT(*) AS dup_loan_no_groups FROM (
  SELECT loan_no FROM loan GROUP BY loan_no HAVING COUNT(*) > 1
) t;

-- 6. loan 孤儿行
SELECT COUNT(*) AS orphan_loan FROM loan l
LEFT JOIN application a ON a.application_no = l.application_no
WHERE l.application_no IS NOT NULL AND l.application_no <> '' AND a.application_no IS NULL;
```

---

## 脚本速查

| 脚本 | 用途 |
|------|------|
| `restore_loan_from_market_delete_dup.py` | 恢复 delete_dup 误删 |
| `repair_loan_app_no_from_application.py` | application_no 第一批（sn） |
| `repair_loan_app_no_from_market.py` | application_no 第二批（market） |
| `sync_application_status_from_loan.py` | application.status 对齐 loan |
| `repair_loan_long_sn.py` | loan_no 长号改短号 |
| `repair_loan_status20_from_source.py` | 重复 loan_no / 源库 status 同步 |
| `mark_application_is_test.py` | is_test=1 |
| `merge_audit_csv.py` | 合并审计 CSV |

---

## 环境与执行建议

1. **IDEA / 8001 代理**：避免单次大 JOIN UPDATE 全表；用 Python 脚本 `--batch-size 200` 或 `--sql-out` 分批。
2. **pymysql 写 application**：易卡死；status / is_test 优先 mysql 客户端。
3. **pymysql 写 loan**：可 `--workers 4` 批更新。
4. **所有 DELETE 操作**：market 修复已禁用自动删除；其他脚本删行前必须 dry-run + 审计。
5. **plan 文件**：`/tmp/fix_app_no_plan.json`、`/tmp/fix_app_no_market_plan.json`、`/tmp/sync_app_status_plan.json` 可复跑 `--apply-only`。

---

## 全量核对：已放款 application → loan 应有且仅有 1 条

以 **application 表为基准**（老系统已放款、排除新 app_id），在内存中对齐 loan 全表 + 源库 `repay_plan`。

```sql
-- 基准集（约数百万）
SELECT application_no, app_id, sn
FROM application
WHERE app_id NOT IN (567, 569, 568, 571, 572, 573)
  AND disbursed_time > 0;
```

期望 `loan_no`（market 后缀 → 源库 core → repay_plan）:

```sql
-- application_no: ng0562-177702748012033909 → ext_sn = 177702748012033909
SELECT ca.sn AS core_sn, rp.plan_sn
FROM ng_loan_core.application ca
INNER JOIN ng_loan_core.repay_plan rp ON rp.sn = ca.sn
INNER JOIN (
  SELECT sn, MAX(plan_sn) AS max_plan_sn
  FROM ng_loan_core.repay_plan
  WHERE sn = (SELECT sn FROM ng_loan_core.application WHERE ext_sn = '177702748012033909')
  GROUP BY sn
) pick ON rp.sn = pick.sn AND rp.plan_sn = pick.max_plan_sn
WHERE ca.ext_sn = '177702748012033909';

-- loan_no = ng-{plan_sn}-01000  （period=01, roll=000 → 后缀 01000）
-- **已确认：中间段必须用 repay_plan.plan_sn**（非 core application.sn）
```

**脚本:** `audit_loan_disbursed.py`

```bash
# 全量核对 → /tmp/loan_audit_issues.csv
python3 audit_loan_disbursed.py --env ./ng_migration.env

# 抽样 1 万
python3 audit_loan_disbursed.py --env ./ng_migration.env --work-limit 10000

# 导出可自动修的 loan_no UPDATE（按 plan_sn 拼期望 loan_no）
python3 audit_loan_disbursed.py --env ./ng_migration.env \\
  --plan-file /tmp/loan_audit_fix_plan.json --sql-out /tmp/loan_audit_fix.sql
```

**issue 类型:** `missing_loan` | `duplicate_loan` | `wrong_loan_no` | `wrong_loan_application_no` | `no_core_sn` | `no_repay_plan` | `orphan_loan`

---

*文档生成自 nigeria-sync-old 迁移修复对话与脚本，日期 2026-07-07。*
