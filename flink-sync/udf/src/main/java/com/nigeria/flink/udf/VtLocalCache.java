package com.nigeria.flink.udf;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * TaskManager 进程内 VT token LRU，跨 UDF 实例共享（同 ClassLoader）。
 */
public final class VtLocalCache {

    private static final int DEFAULT_MAX = 200_000;
    private static final Object LOCK = new Object();
    private static LinkedHashMap<String, String> CACHE;

    private VtLocalCache() {
    }

    private static LinkedHashMap<String, String> cache() {
        if (CACHE == null) {
            synchronized (LOCK) {
                if (CACHE == null) {
                    int max = DEFAULT_MAX;
                    String env = System.getenv("VT_LOCAL_CACHE_MAX");
                    if (env != null && !env.isBlank()) {
                        try {
                            max = Math.max(1000, Integer.parseInt(env.trim()));
                        } catch (NumberFormatException ignored) {
                            // keep default
                        }
                    }
                    final int maxEntries = max;
                    CACHE = new LinkedHashMap<>(Math.min(4096, maxEntries), 0.75f, true) {
                        @Override
                        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                            return size() > maxEntries;
                        }
                    };
                }
            }
        }
        return CACHE;
    }

    public static String get(String raw) {
        if (raw == null) {
            return null;
        }
        synchronized (LOCK) {
            return cache().get(raw);
        }
    }

    public static void put(String raw, String token) {
        if (raw == null || token == null || token.isEmpty()) {
            return;
        }
        synchronized (LOCK) {
            cache().put(raw, token);
        }
    }
}
