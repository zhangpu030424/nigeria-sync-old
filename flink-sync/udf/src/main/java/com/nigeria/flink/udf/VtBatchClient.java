package com.nigeria.flink.udf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * VT POST /v2t 批量令牌化，请求体为字符串 JSON 数组，响应 tokens 与输入一一对应。
 */
public class VtBatchClient {

    private static final Logger LOG = LoggerFactory.getLogger(VtBatchClient.class);
    private static final Pattern TOKEN_ITEM = Pattern.compile("\"([^\"\\\\]*(?:\\\\.[^\"\\\\]*)*)\"");

    private final HttpClient httpClient;
    private final String baseUrl;
    private final int maxRetries;
    private final Duration requestTimeout;

    public VtBatchClient(String baseUrl, int maxRetries, Duration requestTimeout) {
        String url = baseUrl == null ? "http://101.47.27.225" : baseUrl.trim();
        if (url.endsWith("/")) {
            url = url.substring(0, url.length() - 1);
        }
        this.baseUrl = url;
        this.maxRetries = Math.max(1, maxRetries);
        this.requestTimeout = requestTimeout == null ? Duration.ofSeconds(300) : requestTimeout;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        LOG.info("VtBatchClient ready, baseUrl={}, timeout={}s, maxRetries={}",
                this.baseUrl, this.requestTimeout.getSeconds(), this.maxRetries);
    }

    /**
     * 批量 tokenize；null/空串位置直接返回 null，不发给 VT。
     */
    public List<String> tokenizeBatch(List<String> values) {
        if (values == null || values.isEmpty()) {
            return List.of();
        }

        List<String> results = new ArrayList<>(values.size());
        List<String> toSend = new ArrayList<>();
        List<Integer> sendIndexes = new ArrayList<>();

        for (int i = 0; i < values.size(); i++) {
            String value = values.get(i);
            if (value == null || value.isEmpty()) {
                results.add(null);
            } else {
                results.add(null);
                toSend.add(value);
                sendIndexes.add(i);
            }
        }

        if (toSend.isEmpty()) {
            return results;
        }

        List<String> tokens = callVtWithRetry(toSend);
        if (tokens.size() != toSend.size()) {
            throw new RuntimeException(String.format(
                    "VT /v2t token count mismatch: sent=%d got=%d", toSend.size(), tokens.size()));
        }
        for (int i = 0; i < sendIndexes.size(); i++) {
            results.set(sendIndexes.get(i), tokens.get(i));
        }
        return results;
    }

    private List<String> callVtWithRetry(List<String> values) {
        String lastError = "unknown";
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                return callVtOnce(values);
            } catch (Exception e) {
                lastError = e.getMessage();
                LOG.warn("VT /v2t batch attempt {}/{} failed, size={}: {}",
                        attempt, maxRetries, values.size(), lastError);
                if (attempt < maxRetries) {
                    sleepQuietly(500L * attempt);
                }
            }
        }
        throw new RuntimeException(String.format(
                "VT /v2t batch failed after %d retries (%s), size=%d, url=%s/v2t",
                maxRetries, lastError, values.size(), baseUrl));
    }

    private List<String> callVtOnce(List<String> values) throws Exception {
        String body = buildJsonArray(values);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/v2t"))
                .timeout(requestTimeout)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new RuntimeException("HTTP " + response.statusCode()
                    + " body=" + truncate(response.body(), 500));
        }
        return parseTokens(response.body(), values.size());
    }

    static String buildJsonArray(List<String> values) {
        StringBuilder sb = new StringBuilder(values.size() * 20);
        sb.append('[');
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('"').append(escapeJson(values.get(i))).append('"');
        }
        sb.append(']');
        return sb.toString();
    }

    static List<String> parseTokens(String body, int expectedSize) {
        if (body == null || body.isEmpty()) {
            throw new RuntimeException("empty VT response");
        }
        int tokensIdx = body.indexOf("\"tokens\"");
        if (tokensIdx < 0) {
            throw new RuntimeException("no tokens field: " + truncate(body, 500));
        }
        int arrayStart = body.indexOf('[', tokensIdx);
        int arrayEnd = body.indexOf(']', arrayStart);
        if (arrayStart < 0 || arrayEnd < 0) {
            throw new RuntimeException("invalid tokens array: " + truncate(body, 500));
        }
        String arrayBody = body.substring(arrayStart + 1, arrayEnd);
        List<String> tokens = new ArrayList<>(expectedSize);
        Matcher matcher = TOKEN_ITEM.matcher(arrayBody);
        while (matcher.find()) {
            tokens.add(unescapeJson(matcher.group(1)));
        }
        if (tokens.size() != expectedSize) {
            throw new RuntimeException(String.format(
                    "parsed token count %d != expected %d, body=%s",
                    tokens.size(), expectedSize, truncate(body, 500)));
        }
        return tokens;
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private static String unescapeJson(String s) {
        return s.replace("\\\"", "\"")
                .replace("\\\\", "\\")
                .replace("\\n", "\n")
                .replace("\\r", "\r")
                .replace("\\t", "\t");
    }

    private static String truncate(String s, int max) {
        if (s == null) {
            return "";
        }
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
