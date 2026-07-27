package com.nigeria.flink.udf;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

/**
 * 全量/增量写入 user_bankcard.id 时发号（无参，每行一个新 ID）。
 * 与中台对齐：在 TaskManager 容器环境变量配置 SNOWFLAKE_*（见 .env.example）。
 * workerId 默认 = (SNOWFLAKE_WORKER_ID_BASE + subtaskIndex) % 2^workerBits（5bit 时仅 0~31）。
 * 默认 BASE=16，留给中台 Java 0~15；并行度>16 时会回绕并打 WARN。
 */
public class SnowflakeIdFunction extends ScalarFunction {

    private static final Logger LOG = LoggerFactory.getLogger(SnowflakeIdFunction.class);

    private static final long DEFAULT_EPOCH_MS = 1288834974657L;
    private static final long DEFAULT_WORKER_ID_BASE = 16L;

    private transient SnowflakeIdGenerator generator;

    @Override
    public void open(FunctionContext context) {
        long epoch = parseLongEnv("SNOWFLAKE_EPOCH_MS", DEFAULT_EPOCH_MS);
        int datacenterId = (int) parseLongEnv("SNOWFLAKE_DATACENTER_ID", 0L);
        int workerIdBits = (int) parseLongEnv("SNOWFLAKE_WORKER_ID_BITS", 5L);
        int datacenterIdBits = (int) parseLongEnv("SNOWFLAKE_DATACENTER_ID_BITS", 5L);
        int sequenceBits = (int) parseLongEnv("SNOWFLAKE_SEQUENCE_BITS", 12L);
        int workerId = resolveWorkerId(context, workerIdBits);
        generator = new SnowflakeIdGenerator(
                epoch, datacenterId, workerId, workerIdBits, datacenterIdBits, sequenceBits);
        LOG.info(
                "SnowflakeIdFunction ready epoch={} datacenterId={} workerId={} bits=w{}d{}s{}",
                epoch, datacenterId, workerId, workerIdBits, datacenterIdBits, sequenceBits);
    }

    /** 供 UserBankcardIdResolveFunction 等复用同一套 SNOWFLAKE_* 配置。 */
    static SnowflakeIdGenerator newGenerator(FunctionContext context) {
        long epoch = parseLongEnv("SNOWFLAKE_EPOCH_MS", DEFAULT_EPOCH_MS);
        int datacenterId = (int) parseLongEnv("SNOWFLAKE_DATACENTER_ID", 0L);
        int workerIdBits = (int) parseLongEnv("SNOWFLAKE_WORKER_ID_BITS", 5L);
        int datacenterIdBits = (int) parseLongEnv("SNOWFLAKE_DATACENTER_ID_BITS", 5L);
        int sequenceBits = (int) parseLongEnv("SNOWFLAKE_SEQUENCE_BITS", 12L);
        int workerId = resolveWorkerId(context, workerIdBits);
        return new SnowflakeIdGenerator(
                epoch, datacenterId, workerId, workerIdBits, datacenterIdBits, sequenceBits);
    }

    /** 每行调用一次，返回新的雪花 ID。 */
    public long eval() {
        return generator.nextId();
    }

    private static int resolveWorkerId(FunctionContext context, int workerIdBits) {
        int maxWorkerId = (int) (~(-1L << workerIdBits));
        String fixed = System.getenv("SNOWFLAKE_WORKER_ID");
        if (fixed != null && !fixed.isBlank()) {
            int workerId = (int) Long.parseLong(fixed.trim());
            if (workerId < 0 || workerId > maxWorkerId) {
                throw new IllegalArgumentException(
                        "workerId out of range: " + workerId + ", max=" + maxWorkerId);
            }
            return workerId;
        }
        int base = (int) parseLongEnv("SNOWFLAKE_WORKER_ID_BASE", DEFAULT_WORKER_ID_BASE);
        int subtask = subtaskIndex(context);
        int raw = base + subtask;
        if (raw > maxWorkerId) {
            LOG.warn(
                    "SNOWFLAKE_WORKER_ID_BASE({}) + subtask({}) > max({}); workerId wraps to {}",
                    base,
                    subtask,
                    maxWorkerId,
                    raw % (maxWorkerId + 1));
        }
        return raw % (maxWorkerId + 1);
    }

    private static int subtaskIndex(FunctionContext context) {
        try {
            Field field = FunctionContext.class.getDeclaredField("context");
            field.setAccessible(true);
            RuntimeContext runtimeContext = (RuntimeContext) field.get(context);
            return runtimeContext.getIndexOfThisSubtask();
        } catch (ReflectiveOperationException e) {
            LOG.warn("Cannot read Flink subtask index, workerId offset uses 0", e);
            return 0;
        }
    }

    private static long parseLongEnv(String key, long defaultValue) {
        String raw = System.getenv(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        return Long.parseLong(raw.trim());
    }
}
