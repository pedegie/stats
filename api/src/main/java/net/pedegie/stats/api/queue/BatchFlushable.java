package net.pedegie.stats.api.queue;

interface BatchFlushable
{
    /**
     * @return true if successfully flushed and schedule flushing after next {@link #flushIntervalMillis()}
     * when false, {@link Flusher} will try few times, and then postpone flushing to next interval
     */
    boolean batchFlush();

    /**
     * @return interval in milliseconds informing {@link Flusher} how often it should invoke {@link #batchFlush()}
     */
    long flushIntervalMillis();

    /**
     * @return timestamp in milliseconds of last successfully {@link #batchFlush()}
     * {@link Flusher} may not be the only class which invokes {@link #batchFlush()} so he knows not to repeat flushing if was flushed recently
     */
    long lastBatchFlushTimestamp();

    /**
     * @return true if closed
     * Its information for {@link Flusher} to remove {@link BatchFlushable} from queue on next iteration
     */
    boolean isClosed();
}
