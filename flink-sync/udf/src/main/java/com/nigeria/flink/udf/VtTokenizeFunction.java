package com.nigeria.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * Lookup miss 或全量阶段 2 兜底：单条调用 VT POST /v2t（带进程内 LRU）。
 * application 增量优先用 {@link VtTokenizeAppFieldsFunction} 一行一批。
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

        String cached = VtLocalCache.get(value);
        if (cached != null && !cached.isEmpty()) {
            return cached;
        }

        List<String> tokens = client.tokenizeBatch(List.of(value));
        String token = tokens.isEmpty() ? null : tokens.get(0);
        if (token == null || token.isEmpty()) {
            String msg = String.format("VT /v2t empty token, value=%s, url=%s/v2t", mask(value), baseUrl);
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
        VtLocalCache.put(value, token);
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
