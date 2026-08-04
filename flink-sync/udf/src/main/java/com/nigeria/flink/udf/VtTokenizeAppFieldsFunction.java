package com.nigeria.flink.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * application 增量：一行内 mobile/id_number/gaid/bank 合并为一次 /v2t 批量请求。
 * 已有 token 直接用；miss 先查进程缓存，再批量 HTTP。
 */
@FunctionHint(
        output = @DataTypeHint("ROW<mobile STRING, id_number STRING, gaid_idfa STRING, bank_account STRING>")
)
public class VtTokenizeAppFieldsFunction extends ScalarFunction {

    private static final Logger LOG = LoggerFactory.getLogger(VtTokenizeAppFieldsFunction.class);
    private static final int MAX_RETRIES = 3;

    private transient VtBatchClient client;

    @Override
    public void open(org.apache.flink.table.functions.FunctionContext context) {
        String baseUrl = System.getenv().getOrDefault("VT_BASE_URL", "http://101.47.27.225");
        client = new VtBatchClient(baseUrl, MAX_RETRIES, Duration.ofSeconds(15));
        LOG.info("VtTokenizeAppFieldsFunction initialized, VT_BASE_URL={}", baseUrl);
    }

    public Row eval(
            String mobileToken,
            String mobileRaw,
            String idToken,
            String idRaw,
            String gaidToken,
            String gaidRaw,
            String bankToken,
            String bankRaw) {
        String[] prefer = {nz(mobileToken), nz(idToken), nz(gaidToken), nz(bankToken)};
        String[] raws = {nz(mobileRaw), nz(idRaw), nz(gaidRaw), nz(bankRaw)};
        boolean[] emptyAsNull = {false, false, true, false}; // gaid 保持 NULL

        String[] out = new String[4];
        List<String> toSend = new ArrayList<>(4);
        List<Integer> sendIdx = new ArrayList<>(4);

        for (int i = 0; i < 4; i++) {
            if (prefer[i] != null) {
                out[i] = prefer[i];
                continue;
            }
            if (raws[i] == null) {
                out[i] = emptyAsNull[i] ? null : "";
                continue;
            }
            String cached = VtLocalCache.get(raws[i]);
            if (cached != null && !cached.isEmpty()) {
                out[i] = cached;
                continue;
            }
            toSend.add(raws[i]);
            sendIdx.add(i);
        }

        if (!toSend.isEmpty()) {
            List<String> tokens = client.tokenizeBatch(toSend);
            if (tokens.size() != toSend.size()) {
                throw new RuntimeException(String.format(
                        "VT /v2t token count mismatch: sent=%d got=%d", toSend.size(), tokens.size()));
            }
            for (int j = 0; j < sendIdx.size(); j++) {
                int i = sendIdx.get(j);
                String token = tokens.get(j);
                if (token == null || token.isEmpty()) {
                    throw new RuntimeException("VT /v2t empty token for field index=" + i);
                }
                VtLocalCache.put(raws[i], token);
                out[i] = token;
            }
        }

        for (int i = 0; i < 4; i++) {
            if (out[i] == null && !emptyAsNull[i]) {
                out[i] = "";
            }
        }
        return Row.of(out[0], out[1], out[2], out[3]);
    }

    private static String nz(String s) {
        if (s == null) {
            return null;
        }
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }
}
