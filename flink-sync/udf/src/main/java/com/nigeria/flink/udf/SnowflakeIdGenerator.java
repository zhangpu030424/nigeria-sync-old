package com.nigeria.flink.udf;

/**
 * 64 位雪花 ID（timestamp | datacenter | worker | sequence），参数可通过环境变量配置。
 */
final class SnowflakeIdGenerator {

    private final long epoch;
    private final long workerId;
    private final long datacenterId;
    private final long workerIdShift;
    private final long datacenterIdShift;
    private final long timestampShift;
    private final long sequenceMask;

    private long sequence;
    private long lastTimestamp = -1L;

    SnowflakeIdGenerator(
            long epoch,
            long datacenterId,
            long workerId,
            int workerIdBits,
            int datacenterIdBits,
            int sequenceBits) {
        if (workerIdBits + datacenterIdBits + sequenceBits > 22) {
            throw new IllegalArgumentException("worker + datacenter + sequence bits must be <= 22");
        }
        long maxWorkerId = ~(-1L << workerIdBits);
        long maxDatacenterId = ~(-1L << datacenterIdBits);
        if (workerId > maxWorkerId || workerId < 0) {
            throw new IllegalArgumentException("workerId out of range: " + workerId + ", max=" + maxWorkerId);
        }
        if (datacenterId > maxDatacenterId || datacenterId < 0) {
            throw new IllegalArgumentException("datacenterId out of range: " + datacenterId + ", max=" + maxDatacenterId);
        }

        this.epoch = epoch;
        this.workerId = workerId;
        this.datacenterId = datacenterId;
        this.sequenceMask = ~(-1L << sequenceBits);
        this.workerIdShift = sequenceBits;
        this.datacenterIdShift = sequenceBits + workerIdBits;
        this.timestampShift = sequenceBits + workerIdBits + datacenterIdBits;
    }

    synchronized long nextId() {
        long timestamp = System.currentTimeMillis();
        if (timestamp < lastTimestamp) {
            throw new IllegalStateException(
                    "Clock moved backwards, refusing to generate id for " + (lastTimestamp - timestamp) + "ms");
        }
        if (timestamp == lastTimestamp) {
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                timestamp = waitNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0L;
        }
        lastTimestamp = timestamp;
        return ((timestamp - epoch) << timestampShift)
                | (datacenterId << datacenterIdShift)
                | (workerId << workerIdShift)
                | sequence;
    }

    private long waitNextMillis(long lastTs) {
        long ts = System.currentTimeMillis();
        while (ts <= lastTs) {
            ts = System.currentTimeMillis();
        }
        return ts;
    }
}
