package com.nigeria.flink.udf;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * emergency_contacts JSON：mobile 已是 token 则保留，像手机号则调 VT /v2t。
 */
final class EmergencyContactsVtHelper {

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
            return "[]";
        }
        if (trimmed.startsWith("[")) {
            return processContactsArray(trimmed, client);
        }
        if (trimmed.startsWith("{")) {
            return processInfoJson(trimmed, client);
        }
        return payload;
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

        if (!toSend.isEmpty()) {
            List<String> tokens = client.tokenizeBatch(toSend);
            for (int i = 0; i < sendIndexes.size(); i++) {
                String token = tokens.get(i);
                if (token == null || token.isEmpty()) {
                    throw new RuntimeException("VT /v2t empty token for emergency contact mobile");
                }
                slots.get(sendIndexes.get(i)).mobile = token;
            }
        }

        StringBuilder out = new StringBuilder(arrayJson);
        for (int i = slots.size() - 1; i >= 0; i--) {
            MobileSlot slot = slots.get(i);
            out.replace(slot.start, slot.end, "\"" + escapeJson(slot.mobile) + "\"");
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

    private static final class MobileSlot {
        final int start;
        final int end;
        String mobile;

        MobileSlot(int start, int end, String mobile) {
            this.start = start;
            this.end = end;
            this.mobile = mobile;
        }
    }
}
