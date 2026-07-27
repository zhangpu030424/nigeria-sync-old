package com.nigeria.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * emergency_contacts：Lookup/宽表 mobile 优先 token，否则为 +234 明文 → 调 VT /v2t。
 * 入参为 JSON 数组，或完整 info JSON 对象（自动处理 emergency_contacts 字段）。
 */
public class VtTokenizeEmergencyContactsFunction extends ScalarFunction {

    private static final Logger LOG = LoggerFactory.getLogger(VtTokenizeEmergencyContactsFunction.class);

    private transient VtBatchClient client;

    @Override
    public void open(org.apache.flink.table.functions.FunctionContext context) {
        String baseUrl = System.getenv().getOrDefault("VT_BASE_URL", "http://101.47.27.225");
        client = new VtBatchClient(baseUrl, 3, Duration.ofSeconds(15));
        LOG.info("VtTokenizeEmergencyContactsFunction initialized, VT_BASE_URL={}", baseUrl);
    }

    public String eval(String payload) {
        if (payload == null) {
            return "[]";
        }
        String trimmed = payload.trim();
        if (trimmed.isEmpty() || "null".equalsIgnoreCase(trimmed)) {
            return "[]";
        }
        return EmergencyContactsVtHelper.processPayload(trimmed, client);
    }

    /** 增量：info JSON（无 emergency_contacts）+ Lookup 数组 → 合并并 tokenize 明文手机号 */
    public String eval(String infoJson, String emergencyContactsJson) {
        String contacts = emergencyContactsJson == null ? "[]" : emergencyContactsJson.trim();
        if (contacts.isEmpty() || "null".equalsIgnoreCase(contacts)) {
            contacts = "[]";
        }
        String processed = EmergencyContactsVtHelper.processContactsArray(contacts, client);
        return EmergencyContactsVtHelper.mergeIntoInfoJson(infoJson, processed);
    }
}
