package net.pedegie.stats.api.queue;

interface BatchFlushable
{
    boolean batchFlush();

    long flushIntervalMillis();

    long lastBatchFlushTimestamp();

    boolean isClosed();
}
