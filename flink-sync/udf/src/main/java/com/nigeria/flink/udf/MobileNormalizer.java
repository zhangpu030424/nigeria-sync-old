package com.nigeria.flink.udf;

/**
 * 与 SQL CASE 一致的 mobile 规范化（+234...）。
 */
public final class MobileNormalizer {

    private MobileNormalizer() {
    }

    public static String normalize(String mobile) {
        if (mobile == null) {
            return null;
        }
        String trimmed = mobile.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        if (trimmed.startsWith("+")) {
            return trimmed;
        }
        if (trimmed.startsWith("234")) {
            return "+" + trimmed;
        }
        if (trimmed.startsWith("0")) {
            return "+234" + trimmed.substring(1);
        }
        return "+234" + trimmed;
    }
}
