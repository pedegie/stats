package net.pedegie.stats.api.overflow;

import net.pedegie.stats.api.queue.MPMCQueueStats;
import net.pedegie.stats.api.tailer.Tailer;

import java.nio.file.Path;
import java.util.Queue;
import java.util.concurrent.atomic.LongAdder;

public class DroppedDecorator<T> extends MPMCQueueStats<T>
{
    private final LongAdder dropped = new LongAdder();
    private final LongAdder written = new LongAdder();

    public DroppedDecorator(Queue<T> queue, Path fileName, Tailer<Long, Integer> tailer)
    {
        super(queue, fileName, tailer);
    }

    @Override
    protected boolean write(int count, long nanoTime)
    {
        boolean written = super.write(count, nanoTime);
        if (written)
        {
            this.written.increment();
        } else
        {
            dropped.increment();
        }

        return written;
    }

    public DroppedRatio dropped()
    {
        return new DroppedRatio(dropped.sumThenReset(), written.sumThenReset());
    }

    public static class DroppedRatio
    {
        private final long dropped;
        private final long written;

        public DroppedRatio(long dropped, long written)
        {
            this.dropped = dropped;
            this.written = written;
        }

        public long getDropped()
        {
            return dropped;
        }

        public long getWritten()
        {
            return written;
        }
    }
}
