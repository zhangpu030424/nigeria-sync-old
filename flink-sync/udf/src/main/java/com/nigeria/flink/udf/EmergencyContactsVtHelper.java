package com.nigeria.flink.udf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * emergency_contacts：
 * 1) 源库 [[name, relation, mobile], ...] → [{name, mobile, relation}, ...]
 * 2) mobile 明文调 VT /v2t；未命中写 null（不写明文，对齐批处理）
 */
final class EmergencyContactsVtHelper {

    private static final Logger LOG = LoggerFactory.getLogger(EmergencyContactsVtHelper.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Pattern MOBILE_FIELD =
            Pattern.compile("\"mobile\"\\s*:\\s*(null|\"((?:\\\\.|[^\"\\\\])*)\")", Pattern.CASE_INSENSITIVE);
    private static final Pattern EMERGENCY_IN_INFO =
            Pattern.compile("\"emergency_contacts\"\\s*:\\s*(\\[(?:[^\\[\\]]|\\[[^\\[\\]]*])*])", Pattern.CASE_INSENSITIVE);

    private EmergencyContactsVtHelper() {
    }

    static String mergeIntoInfoJson(String infoJson, String emergencyContactsArray) {
        if (infoJson == null || infoJson.trim().isEmpty()) {
            return "{\"emergency_contacts\":" + (emergencyContactsArray == null ? "[]" : emergencyContactsArray) + "}";
        }
        String info = infoJson.trim();
        String contacts = emergencyContactsArray == null || emergencyContactsArray.trim().isEmpty()
                ? "[]" : emergencyContactsArray.trim();
        if (!info.endsWith("}")) {
            return info;
        }
        String body = info.substring(0, info.length() - 1);
        if (body.endsWith("{")) {
            return "{" + "\"emergency_contacts\":" + contacts + "}";
        }
        return body + ",\"emergency_contacts\":" + contacts + "}";
    }

    static String processPayload(String payload, VtBatchClient client) {
        if (payload == null) {
            return null;
        }
        String trimmed = payload.trim();
        if (trimmed.isEmpty() || "null".equalsIgnoreCase(trimmed)) {
            return emptyContactsJson();
        }
        if (trimmed.startsWith("[[") || isSourceTupleArray(trimmed)) {
            return formatFromSource(trimmed, client);
        }
        if (trimmed.startsWith("[")) {
            // 已是对象数组则只做 VT；若仍是裸数组也走 format
            if (!trimmed.contains("\"mobile\"") && !trimmed.contains("\"name\"")) {
                return formatFromSource(trimmed, client);
            }
            return processContactsArray(trimmed, client);
        }
        if (trimmed.startsWith("{")) {
            return processInfoJson(trimmed, client);
        }
        return emptyContactsJson();
    }

    /** 源库 emergencyContact → 标准对象数组并 VT。 */
    static String formatFromSource(String raw, VtBatchClient client) {
        if (raw == null || raw.trim().isEmpty() || "null".equalsIgnoreCase(raw.trim())) {
            return emptyContactsJson();
        }
        try {
            JsonNode root = MAPPER.readTree(raw.trim());
            if (!root.isArray() || root.size() == 0) {
                return emptyContactsJson();
            }

            List<ContactDraft> drafts = new ArrayList<>();
            List<String> toSend = new ArrayList<>();
            List<Integer> sendIndexes = new ArrayList<>();

            for (JsonNode item : root) {
                ContactDraft draft = parseItem(item);
                if (draft == null) {
                    continue;
                }
                drafts.add(draft);
                if (draft.mobileRaw != null && !draft.mobileRaw.isEmpty() && looksLikePhone(draft.mobileRaw)) {
                    toSend.add(MobileNormalizer.normalize(draft.mobileRaw));
                    sendIndexes.add(drafts.size() - 1);
                }
            }

            if (drafts.isEmpty()) {
                return emptyContactsJson();
            }

            if (!toSend.isEmpty() && client != null) {
                try {
                    List<String> tokens = client.tokenizeBatch(toSend);
                    for (int i = 0; i < sendIndexes.size(); i++) {
                        String token = i < tokens.size() ? tokens.get(i) : null;
                        ContactDraft d = drafts.get(sendIndexes.get(i));
                        // 未命中不写明文
                        d.mobileToken = (token != null && !token.isEmpty()) ? token : null;
                    }
                } catch (Exception e) {
                    LOG.warn("VT tokenize emergency contacts failed: {}", e.toString());
                    for (int idx : sendIndexes) {
                        drafts.get(idx).mobileToken = null;
                    }
                }
            }

            ArrayNode out = MAPPER.createArrayNode();
            for (ContactDraft d : drafts) {
                ObjectNode obj = MAPPER.createObjectNode();
                if (d.name == null || d.name.isEmpty()) {
                    obj.putNull("name");
                } else {
                    obj.put("name", d.name);
                }
                if (d.mobileToken == null || d.mobileToken.isEmpty()) {
                    obj.putNull("mobile");
                } else {
                    obj.put("mobile", d.mobileToken);
                }
                if (d.relation == null) {
                    obj.putNull("relation");
                } else if (d.relation instanceof Number) {
                    obj.put("relation", ((Number) d.relation).longValue());
                } else {
                    obj.put("relation", String.valueOf(d.relation));
                }
                out.add(obj);
            }
            return MAPPER.writeValueAsString(out);
        } catch (Exception e) {
            LOG.warn("format emergencyContact failed, raw={}, err={}", mask(raw), e.toString());
            return emptyContactsJson();
        }
    }

    private static ContactDraft parseItem(JsonNode item) {
        if (item == null || item.isNull()) {
            return null;
        }
        if (item.isArray() && item.size() >= 3) {
            ContactDraft d = new ContactDraft();
            d.name = textOrNull(item.get(0));
            d.relation = scalar(item.get(1));
            d.mobileRaw = textOrNull(item.get(2));
            return d;
        }
        if (item.isObject()) {
            ContactDraft d = new ContactDraft();
            d.name = firstText(item, "name", "contactName", "contact_name");
            JsonNode rel = firstNode(item, "relation", "contactRelationship", "contact_relationship");
            d.relation = scalar(rel);
            d.mobileRaw = firstText(item, "mobile", "contactNumber", "contact_number");
            return d;
        }
        return null;
    }

    private static boolean isSourceTupleArray(String trimmed) {
        // ["a",1,"b"] 单层也视为源格式
        return trimmed.startsWith("[") && !trimmed.contains("\"mobile\"") && !trimmed.contains("\"name\"");
    }

    static String emptyContactsJson() {
        return "[{\"name\":null,\"mobile\":null,\"relation\":null}]";
    }

    static String processInfoJson(String infoJson, VtBatchClient client) {
        Matcher matcher = EMERGENCY_IN_INFO.matcher(infoJson);
        if (!matcher.find()) {
            return infoJson;
        }
        String arrayJson = matcher.group(1);
        String processed = processContactsArray(arrayJson, client);
        return matcher.replaceFirst("\"emergency_contacts\":" + Matcher.quoteReplacement(processed));
    }

    static String processContactsArray(String arrayJson, VtBatchClient client) {
        // 若仍是元组数组，先规范化
        if (arrayJson != null && (arrayJson.trim().startsWith("[[") || isSourceTupleArray(arrayJson.trim()))) {
            return formatFromSource(arrayJson, client);
        }
        List<MobileSlot> slots = new ArrayList<>();
        Matcher matcher = MOBILE_FIELD.matcher(arrayJson);
        while (matcher.find()) {
            String raw = matcher.group(1);
            if ("null".equalsIgnoreCase(raw)) {
                continue;
            }
            String mobile = unescapeJson(matcher.group(2));
            if (mobile == null || mobile.isEmpty()) {
                continue;
            }
            slots.add(new MobileSlot(matcher.start(2) - 1, matcher.end(2) + 1, mobile));
        }
        if (slots.isEmpty()) {
            return arrayJson;
        }

        List<String> toSend = new ArrayList<>();
        List<Integer> sendIndexes = new ArrayList<>();
        for (int i = 0; i < slots.size(); i++) {
            MobileSlot slot = slots.get(i);
            if (looksLikePhone(slot.mobile)) {
                toSend.add(MobileNormalizer.normalize(slot.mobile));
                sendIndexes.add(i);
            }
        }

        if (!toSend.isEmpty() && client != null) {
            try {
                List<String> tokens = client.tokenizeBatch(toSend);
                for (int i = 0; i < sendIndexes.size(); i++) {
                    String token = i < tokens.size() ? tokens.get(i) : null;
                    if (token == null || token.isEmpty()) {
                        // 未命中：写成 null（对齐批处理，不抛异常）
                        slots.get(sendIndexes.get(i)).mobile = null;
                        slots.get(sendIndexes.get(i)).toNull = true;
                    } else {
                        slots.get(sendIndexes.get(i)).mobile = token;
                    }
                }
            } catch (Exception e) {
                LOG.warn("VT tokenize emergency contacts array failed: {}", e.toString());
                for (int idx : sendIndexes) {
                    slots.get(idx).mobile = null;
                    slots.get(idx).toNull = true;
                }
            }
        }

        StringBuilder out = new StringBuilder(arrayJson);
        for (int i = slots.size() - 1; i >= 0; i--) {
            MobileSlot slot = slots.get(i);
            if (slot.toNull || slot.mobile == null) {
                out.replace(slot.start, slot.end, "null");
            } else {
                out.replace(slot.start, slot.end, "\"" + escapeJson(slot.mobile) + "\"");
            }
        }
        return out.toString();
    }

    static boolean looksLikePhone(String value) {
        if (value == null) {
            return false;
        }
        String v = value.trim();
        if (v.isEmpty()) {
            return false;
        }
        if (v.startsWith("+") || v.startsWith("0") || v.startsWith("234")) {
            return true;
        }
        return v.chars().allMatch(Character::isDigit) && v.length() >= 7 && v.length() <= 16;
    }

    private static String textOrNull(JsonNode n) {
        if (n == null || n.isNull()) {
            return null;
        }
        String s = n.asText("").trim();
        return s.isEmpty() ? null : s;
    }

    private static Object scalar(JsonNode n) {
        if (n == null || n.isNull()) {
            return null;
        }
        if (n.isNumber()) {
            return n.numberValue();
        }
        String s = n.asText("").trim();
        return s.isEmpty() ? null : s;
    }

    private static String firstText(JsonNode obj, String... keys) {
        JsonNode n = firstNode(obj, keys);
        return textOrNull(n);
    }

    private static JsonNode firstNode(JsonNode obj, String... keys) {
        for (String k : keys) {
            if (obj.has(k) && !(obj.get(k) instanceof NullNode) && !obj.get(k).isNull()) {
                return obj.get(k);
            }
        }
        return null;
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private static String unescapeJson(String s) {
        if (s == null) {
            return null;
        }
        return s.replace("\\\"", "\"")
                .replace("\\\\", "\\")
                .replace("\\n", "\n")
                .replace("\\r", "\r")
                .replace("\\t", "\t");
    }

    private static String mask(String value) {
        if (value == null || value.length() < 12) {
            return "***";
        }
        return value.substring(0, 8) + "...";
    }

    private static final class ContactDraft {
        String name;
        Object relation;
        String mobileRaw;
        String mobileToken;
    }

    private static final class MobileSlot {
        final int start;
        final int end;
        String mobile;
        boolean toNull;

        MobileSlot(int start, int end, String mobile) {
            this.start = start;
            this.end = end;
            this.mobile = mobile;
        }
    }
}
