/*

----- UNCOMMENT AND THEN MAKE FLUSHER CLASS PUBLIC FOR BENCHMARKS ------

package io.github.pedegie.stats.jmh.profiler.flusher;

import io.github.pedegie.stats.api.queue.BatchFlushable;

class TestFlushable implements BatchFlushable
{
    private final long interval;
    private long lastBatchFlushTimestamp;

    TestFlushable(long interval)
    {
        this.interval = interval;
    }

    @Override
    public boolean batchFlush()
    {
        lastBatchFlushTimestamp = System.currentTimeMillis();
        return true;
    }

    @Override
    public long flushIntervalMillis()
    {
        return interval;
    }

    @Override
    public long lastBatchFlushTimestamp()
    {
        return lastBatchFlushTimestamp;
    }

    @Override
    public boolean isClosed()
    {
        return false;
    }
}
*/
