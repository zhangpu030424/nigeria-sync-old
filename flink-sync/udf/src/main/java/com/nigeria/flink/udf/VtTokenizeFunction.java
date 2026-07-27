package com.nigeria.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * Lookup miss 或全量阶段 2 兜底：单条调用 VT POST /v2t。
 * 主路径仍是 vt_token_cache 预加载 + 阶段 1 宽表 mobile_token；
 * 阶段 2 见 sql/02_sync_user_fast_vt_miss.sql；增量见 02_sync_user_incr.sql COALESCE 兜底。
 * 环境变量 VT_BASE_URL（TaskManager 容器内需可访问）。
 */
public class VtTokenizeFunction extends ScalarFunction {

    private static final Logger LOG = LoggerFactory.getLogger(VtTokenizeFunction.class);
    private static final int MAX_RETRIES = 3;

    private transient VtBatchClient client;
    private transient String baseUrl;

    @Override
    public void open(org.apache.flink.table.functions.FunctionContext context) {
        baseUrl = System.getenv().getOrDefault("VT_BASE_URL", "http://101.47.27.225");
        client = new VtBatchClient(baseUrl, MAX_RETRIES, Duration.ofSeconds(15));
        LOG.info("VtTokenizeFunction initialized, VT_BASE_URL={}", baseUrl);
    }

    public String eval(String raw) {
        if (raw == null) {
            return null;
        }
        String value = raw.trim();
        if (value.isEmpty()) {
            return null;
        }

        List<String> tokens = client.tokenizeBatch(List.of(value));
        String token = tokens.isEmpty() ? null : tokens.get(0);
        if (token == null || token.isEmpty()) {
            String msg = String.format("VT /v2t empty token, value=%s, url=%s/v2t", mask(value), baseUrl);
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
        return token;
    }

    private static String mask(String value) {
        if (value == null || value.length() < 8) {
            return "***";
        }
        return value.substring(0, Math.min(5, value.length())) + "****"
                + value.substring(value.length() - 4);
    }
}
