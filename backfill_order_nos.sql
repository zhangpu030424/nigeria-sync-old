-- Backfill target order numbers (country=ng, period=1, roll=0 -> suffix 01000)
-- Old format: application_no is all digits (market applicationNo, e.g. 178158518012012589)
-- New format: ng{app_id:04d}-{sn} / ng-{sn}-01000
-- Prerequisite: application.sn is core_sn (NOT the same all-digit market no)
-- Run on TARGET DB (ng). Stop validate/repair jobs first.

-- ========== 0. 摸底 ==========
SELECT 'application_all_digit' AS k, COUNT(*) AS c
FROM application
WHERE application_no REGEXP '^[0-9]+$';

SELECT 'application_can_update' AS k, COUNT(*) AS c
FROM application
WHERE application_no REGEXP '^[0-9]+$'
  AND sn IS NOT NULL AND sn <> ''
  AND sn NOT REGEXP '^[0-9]+$';   -- sn 不能也是纯数字市场号

SELECT 'application_sn_is_market_no' AS k, COUNT(*) AS c
FROM application
WHERE application_no REGEXP '^[0-9]+$'
  AND sn REGEXP '^[0-9]+$';       -- sn=178... 需先 join 源库补 core_sn

SELECT 'loan_all_digit_app_no' AS k, COUNT(*) AS c
FROM loan
WHERE application_no REGEXP '^[0-9]+$';

-- 抽样预览
SELECT application_no AS old_no,
       CONCAT('ng', LPAD(app_id, 4, '0'), '-', sn) AS new_application_no,
       app_id, sn
FROM application
WHERE application_no REGEXP '^[0-9]+$'
  AND sn IS NOT NULL AND sn <> ''
  AND sn NOT REGEXP '^[0-9]+$'
LIMIT 20;


-- ========== 1. 先改 loan（仍用旧 application_no 关联）==========
-- 分批：AND l.application_no >= 'xxx' AND l.application_no < 'yyy'

UPDATE loan l
INNER JOIN application a ON l.application_no = a.application_no
LEFT JOIN loan x ON x.loan_no = CONCAT('ng-', a.sn, '-01000') AND x.loan_no <> l.loan_no
SET l.application_no = CONCAT('ng', LPAD(a.app_id, 4, '0'), '-', a.sn),
    l.loan_no        = CONCAT('ng-', a.sn, '-01000')
WHERE a.application_no REGEXP '^[0-9]+$'
  AND a.sn IS NOT NULL AND a.sn <> ''
  AND a.sn NOT REGEXP '^[0-9]+$'
  AND x.loan_no IS NULL;


-- ========== 2. 再改 application ==========

UPDATE application a
INNER JOIN (
  SELECT application_no AS old_no,
         CONCAT('ng', LPAD(app_id, 4, '0'), '-', sn) AS new_no
  FROM application
  WHERE application_no REGEXP '^[0-9]+$'
    AND sn IS NOT NULL AND sn <> ''
    AND sn NOT REGEXP '^[0-9]+$'
) m ON a.application_no = m.old_no
LEFT JOIN application x ON x.application_no = m.new_no AND x.application_no <> m.old_no
SET a.application_no = m.new_no
WHERE x.application_no IS NULL;


-- ========== 3. sn 也是纯数字时：需 join 源库补 core_sn（跨库示例）==========
-- UPDATE ng.application a
-- INNER JOIN ng_loan_market.application m ON m.applicationNo = a.application_no
-- INNER JOIN ng_loan_core.application c ON c.ext_sn = m.applicationNo
-- SET a.sn = c.sn,
--     a.application_no = CONCAT('ng', LPAD(a.app_id, 4, '0'), '-', c.sn)
-- WHERE a.application_no REGEXP '^[0-9]+$'
--   AND c.sn IS NOT NULL AND c.sn <> '';


-- ========== 4. paid_time 秒 -> 毫秒 ==========

-- SELECT COUNT(*) FROM loan WHERE paid_time > 0 AND paid_time < 1000000000000;

UPDATE loan
SET paid_time = paid_time * 1000
WHERE paid_time > 0 AND paid_time < 1000000000000;


-- ========== 5. 改后复核 ==========

SELECT 'application_remaining_all_digit' AS k, COUNT(*) AS c
FROM application
WHERE application_no REGEXP '^[0-9]+$';

SELECT 'loan_remaining_all_digit' AS k, COUNT(*) AS c
FROM loan
WHERE application_no REGEXP '^[0-9]+$';
