-- ng_loan_market：Flink Lookup 点查优化（UNSIGNED id 无法被 CAST 下推时）
-- 生成列 + 索引，供 user_incr_lookup WHERE id=? 走索引（毫秒级）
-- 部署（需 ALTER 权限，deploy-source-ddl.sh --force-indexes 会调用）:
--   mysql ... ng_loan_market < sql/ddl/market_lookup_indexes.sql

SET SESSION lock_wait_timeout = 120;

-- user.id UNSIGNED → id_signed，Lookup 过滤列
SET @db = DATABASE();
SET @exists = (
    SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = @db AND TABLE_NAME = 'user' AND COLUMN_NAME = 'id_signed'
);
SET @sql = IF(@exists = 0,
    'ALTER TABLE `user` ADD COLUMN id_signed BIGINT AS (CAST(id AS SIGNED)) STORED, ADD INDEX idx_user_id_signed (id_signed)',
    'SELECT ''user.id_signed already exists'' AS msg'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
