-- =============================================================================
-- 目标库全量改号（不 join 源库）
-- 规则：后缀用旧 application_no（纯数字市场号）
--   application_no = ng + LPAD(app_id,4,'0') + '-' + 旧application_no
--   loan_no        = ng- + 旧application_no + -01000
-- 执行前：停 validate/repair；建议在服务器 mysql 命令行 + screen 里跑
-- =============================================================================

SET SESSION wait_timeout = 28800;
SET SESSION net_read_timeout = 3600;
SET SESSION net_write_timeout = 3600;
SET SESSION innodb_lock_wait_timeout = 600;

-- ========== 0. 摸底 ==========
SELECT 'application_待改' AS k, COUNT(*) AS c
FROM application
WHERE application_no REGEXP '^[0-9]+$'
  AND app_id IS NOT NULL;

SELECT 'loan_待改' AS k, COUNT(*) AS c
FROM loan l
INNER JOIN application a ON l.application_no = a.application_no
WHERE l.application_no REGEXP '^[0-9]+$'
  AND a.app_id IS NOT NULL;

SELECT MIN(l.loan_no) AS loan_min, MAX(l.loan_no) AS loan_max,
       MIN(l.application_no) AS app_no_min, MAX(l.application_no) AS app_no_max
FROM loan l
WHERE l.application_no REGEXP '^[0-9]+$';


-- ========== 1. 全量改 loan（先改 loan，仍用旧 application_no 关联）==========
-- MySQL 多表 UPDATE 不支持 LIMIT，请用下面「分批」写法

UPDATE loan l
INNER JOIN application a ON l.application_no = a.application_no
SET l.application_no = CONCAT('ng', LPAD(a.app_id, 4, '0'), '-', a.application_no),
    l.loan_no        = CONCAT('ng-', a.application_no, '-01000')
WHERE l.application_no REGEXP '^[0-9]+$'
  AND a.app_id IS NOT NULL;


-- ========== 1b. loan 分批（无 id：用 loan_no 主键，每批 50000，重复直到 0 rows）==========

UPDATE loan l
INNER JOIN application a ON l.application_no = a.application_no
INNER JOIN (
  SELECT l2.loan_no AS old_loan_no
  FROM loan l2
  WHERE l2.application_no REGEXP '^[0-9]+$'
  ORDER BY l2.loan_no
  LIMIT 50000
) batch ON batch.old_loan_no = l.loan_no
SET l.application_no = CONCAT('ng', LPAD(a.app_id, 4, '0'), '-', a.application_no),
    l.loan_no        = CONCAT('ng-', a.application_no, '-01000')
WHERE a.app_id IS NOT NULL;


-- ========== 1c. loan 分批（备选：按 application_no 数字范围，改 @from/@to 重复）==========
-- 例：1650...~1699...、1700...~1749... 按摸底 MIN/MAX 切几段

-- SET @from = '165000000000000000', @to = '170000000000000000';
-- UPDATE loan l
-- INNER JOIN application a ON l.application_no = a.application_no
-- SET l.application_no = CONCAT('ng', LPAD(a.app_id, 4, '0'), '-', a.application_no),
--     l.loan_no        = CONCAT('ng-', a.application_no, '-01000')
-- WHERE l.application_no REGEXP '^[0-9]+$'
--   AND l.application_no >= @from AND l.application_no < @to
--   AND a.app_id IS NOT NULL;


-- ========== 2. 全量改 application ==========

UPDATE application
SET application_no = CONCAT('ng', LPAD(app_id, 4, '0'), '-', application_no)
WHERE application_no REGEXP '^[0-9]+$'
  AND app_id IS NOT NULL;


-- ========== 2b. application 分批（每批 50000，重复执行直到 0 rows）==========

UPDATE application a
INNER JOIN (
  SELECT application_no AS old_no
  FROM application
  WHERE application_no REGEXP '^[0-9]+$'
    AND app_id IS NOT NULL
  ORDER BY application_no
  LIMIT 50000
) batch ON batch.old_no = a.application_no
SET a.application_no = CONCAT('ng', LPAD(a.app_id, 4, '0'), '-', a.application_no);


-- ========== 3. paid_time 秒 -> 毫秒（可选，按需执行）==========

-- SELECT COUNT(*) FROM loan WHERE paid_time > 0 AND paid_time < 1000000000000;

UPDATE loan
SET paid_time = paid_time * 1000
WHERE paid_time > 0 AND paid_time < 1000000000000;


-- ========== 4. 复核 ==========
SELECT 'application_剩余纯数字' AS k, COUNT(*) AS c
FROM application
WHERE application_no REGEXP '^[0-9]+$';

SELECT 'loan_剩余纯数字' AS k, COUNT(*) AS c
FROM loan
WHERE application_no REGEXP '^[0-9]+$';

-- 半改状态：loan 已是新号、application 还是旧号（不应 > 0）
SELECT COUNT(*) AS loan新_application旧
FROM loan l
WHERE l.application_no NOT REGEXP '^[0-9]+$'
  AND EXISTS (
    SELECT 1 FROM application a
    WHERE a.application_no REGEXP '^[0-9]+$'
      AND l.application_no = CONCAT('ng', LPAD(a.app_id, 4, '0'), '-', a.application_no)
  );
