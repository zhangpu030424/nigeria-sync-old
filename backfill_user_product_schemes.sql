-- Backfill user_product.schemes on TARGET DB (ng)
-- New format: [{"schemeId":"PROD-001-D7","amountRange":[credit_amount]}]
-- Run on server mysql CLI; batch if table is large.

-- ========== 0. 摸底 ==========
SELECT COUNT(*) AS total FROM user_product;

SELECT COUNT(*) AS old_format
FROM user_product
WHERE schemes NOT LIKE '%"schemeId"%'
   OR schemes NOT LIKE '%PROD-001-D7%';

-- 抽样预览
SELECT group_user_id, product_id, credit_amount, schemes AS old_schemes,
       JSON_ARRAY(
           JSON_OBJECT(
               'schemeId', 'PROD-001-D7',
               'amountRange', JSON_ARRAY(credit_amount)
           )
       ) AS new_schemes
FROM user_product
LIMIT 5;


-- ========== 1. 全量更新（MySQL 5.7+ JSON 函数）==========

UPDATE user_product
SET schemes = JSON_ARRAY(
    JSON_OBJECT(
        'schemeId', 'PROD-001-D7',
        'amountRange', JSON_ARRAY(credit_amount)
    )
);


-- ========== 1b. 分批（按 group_user_id，改 @lo/@hi 重复直到 0 rows）==========

-- SET @lo = 0, @hi = 500000;
-- UPDATE user_product
-- SET schemes = JSON_ARRAY(
--     JSON_OBJECT(
--         'schemeId', 'PROD-001-D7',
--         'amountRange', JSON_ARRAY(credit_amount)
--     )
-- )
-- WHERE group_user_id > @lo AND group_user_id <= @hi;


-- ========== 1c. 无 JSON 函数时用 CONCAT（备选）==========

-- UPDATE user_product
-- SET schemes = CONCAT(
--     '[{"schemeId":"PROD-001-D7","amountRange":[',
--     credit_amount,
--     ']}]'
-- );


-- ========== 2. 只改旧格式行（可选）==========

-- UPDATE user_product
-- SET schemes = JSON_ARRAY(
--     JSON_OBJECT(
--         'schemeId', 'PROD-001-D7',
--         'amountRange', JSON_ARRAY(credit_amount)
--     )
-- )
-- WHERE schemes NOT LIKE '%"schemeId"%'
--    OR schemes NOT LIKE '%PROD-001-D7%';


-- ========== 3. 复核 ==========
SELECT COUNT(*) AS remaining_old
FROM user_product
WHERE schemes NOT LIKE '%"schemeId"%'
   OR schemes NOT LIKE '%PROD-001-D7%';

SELECT group_user_id, product_id, credit_amount, schemes
FROM user_product
LIMIT 5;
