package com.nigeria.flink.udf;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * user_bankcard 增量 id：查目标库已有 id（兼容 group_user_id UNSIGNED→BigInteger），无则雪花发号。
 * TaskManager 需配置 TARGET_MYSQL_* 环境变量（见 docker-compose.yml）。
 */
public class UserBankcardIdResolveFunction extends ScalarFunction {

    private static final Logger LOG = LoggerFactory.getLogger(UserBankcardIdResolveFunction.class);
    private static final int CACHE_MAX = 200_000;
    private static final int JDBC_CONNECT_TIMEOUT_SEC = 60;
    private static final int JDBC_SOCKET_TIMEOUT_MS = 600_000;
    private static final String LOOKUP_SQL =
            "SELECT id FROM user_bankcard WHERE group_user_id = ? AND bank_account_number = ? LIMIT 1";

    private transient String jdbcUrl;
    private transient String jdbcUser;
    private transient String jdbcPassword;
    private transient Connection connection;
    private transient PreparedStatement lookupStmt;
    private transient SnowflakeIdGenerator snowflake;
    private transient Map<String, Long> cache;

    @Override
    public void open(FunctionContext context) {
        String host = requireEnv("TARGET_MYSQL_HOST");
        String port = envOr("TARGET_MYSQL_PORT", "3306");
        jdbcUser = requireEnv("TARGET_MYSQL_USER");
        jdbcPassword = requireEnv("TARGET_MYSQL_PASSWORD");
        String database = requireEnv("TARGET_MYSQL_DATABASE");

        jdbcUrl = String.format(
                "jdbc:mysql://%s:%s/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos"
                        + "&connectTimeout=%d&socketTimeout=%d&tcpKeepAlive=true",
                host, port, database, JDBC_CONNECT_TIMEOUT_SEC * 1000, JDBC_SOCKET_TIMEOUT_MS);

        try {
            openJdbc();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to open target JDBC for user_bankcard id lookup", e);
        }

        snowflake = SnowflakeIdFunction.newGenerator(context);
        cache = new ConcurrentHashMap<>();
        LOG.info("UserBankcardIdResolveFunction ready target={}@{}:{}/{}", jdbcUser, host, port, database);
    }

    public long eval(Long groupUserId, String bankAccountNumber) {
        if (groupUserId == null || bankAccountNumber == null) {
            return snowflake.nextId();
        }
        String account = bankAccountNumber.trim();
        if (account.isEmpty()) {
            return snowflake.nextId();
        }

        String cacheKey = groupUserId + "\0" + account;
        Long cached = cache.get(cacheKey);
        if (cached != null) {
            return cached;
        }

        Long existing = lookupExistingId(groupUserId, account);
        if (existing != null && existing != 0L) {
            putCache(cacheKey, existing);
            return existing;
        }

        long newId = snowflake.nextId();
        putCache(cacheKey, newId);
        return newId;
    }

    private Long lookupExistingId(long groupUserId, String bankAccountNumber) {
        SQLException last = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            try {
                ensureLookupStatement();
                lookupStmt.setObject(1, groupUserId);
                lookupStmt.setString(2, bankAccountNumber);
                try (ResultSet rs = lookupStmt.executeQuery()) {
                    if (!rs.next()) {
                        return null;
                    }
                    return readLongColumn(rs, 1);
                }
            } catch (SQLException e) {
                last = e;
                if (attempt == 0 && isStaleConnection(e)) {
                    LOG.warn("user_bankcard id lookup JDBC stale, reconnecting: {}", e.toString());
                    closeJdbcQuietly();
                    continue;
                }
                break;
            }
        }
        throw new RuntimeException(
                "user_bankcard id lookup failed group_user_id=" + groupUserId + " account=" + bankAccountNumber,
                last);
    }

    private void openJdbc() throws SQLException {
        connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
        lookupStmt = connection.prepareStatement(LOOKUP_SQL);
    }

    private void ensureLookupStatement() throws SQLException {
        if (connection != null && connection.isValid(2) && lookupStmt != null && !lookupStmt.isClosed()) {
            return;
        }
        closeJdbcQuietly();
        openJdbc();
    }

    private static boolean isStaleConnection(SQLException e) {
        Throwable t = e;
        while (t != null) {
            String name = t.getClass().getName();
            if (name.contains("CommunicationsException") || name.contains("EOFException")) {
                return true;
            }
            String msg = t.getMessage();
            if (msg != null && (msg.contains("Communications link failure")
                    || msg.contains("Connection is closed")
                    || msg.contains("No operations allowed after connection closed"))) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    private static Long readLongColumn(ResultSet rs, int columnIndex) throws SQLException {
        Object value = rs.getObject(columnIndex);
        if (value == null) {
            return null;
        }
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof Integer) {
            return ((Integer) value).longValue();
        }
        if (value instanceof BigInteger) {
            return ((BigInteger) value).longValue();
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.parseLong(value.toString());
    }

    private void putCache(String key, long id) {
        if (cache.size() >= CACHE_MAX) {
            cache.clear();
        }
        cache.put(key, id);
    }

    @Override
    public void close() {
        closeJdbcQuietly();
    }

    private void closeJdbcQuietly() {
        if (lookupStmt != null) {
            try {
                lookupStmt.close();
            } catch (SQLException ignored) {
                // ignore
            }
            lookupStmt = null;
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignored) {
                // ignore
            }
            connection = null;
        }
    }

    private static String requireEnv(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing env for UserBankcardIdResolveFunction: " + key);
        }
        return value.trim();
    }

    private static String envOr(String key, String defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value.trim();
    }
}
