package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import net.openhft.chronicle.core.annotation.ForceInline;
import net.pedegie.stats.api.queue.fileaccess.FileAccess;
import net.pedegie.stats.api.queue.fileaccess.FileAccessStrategy;
import net.pedegie.stats.api.queue.fileaccess.PathDateFormatter;
import net.pedegie.stats.api.tailer.Tailer;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

@FieldDefaults(makeFinal = true, level = AccessLevel.PROTECTED)
public class MPMCQueueStats<T> implements Queue<T>, Closeable
{
    Path filePath;
    Queue<T> queue;
    AtomicInteger count = new AtomicInteger(0);
    ReentrantLock recycleLock = new ReentrantLock();
    long fileCycleDurationInMillis;
    int mmapSize;

    @NonFinal
    FileAccess fileAccess;
    @NonFinal
    long nextCycleMillisTimestamp;
    @NonFinal
    Clock fileCycleClock;

    @SneakyThrows
    protected MPMCQueueStats(Queue<T> queue, LogFileConfiguration logFileConfiguration, Tailer<Long, Integer> tailer)
    {
        LogFileConfigurationValidator.validate(logFileConfiguration);
        this.filePath = logFileConfiguration.getPath();
        this.queue = queue;
        this.fileCycleClock = logFileConfiguration.getFileCycleClock();
        this.fileCycleDurationInMillis = logFileConfiguration.getFileCycleDurationInMillis();
        this.mmapSize = logFileConfiguration.getMmapSize();

        if (recycleLock.tryLock())
        {
            try
            {
                initialize();
            } finally
            {
                recycleLock.unlock();
            }
        }
    }

    private void initialize()
    {
        var offsetDateTime = newFileOffset();
        var fileName = PathDateFormatter.appendDate(filePath, offsetDateTime);
        this.fileAccess = FileAccessStrategy.accept(mmapSize, fileName);
        this.nextCycleMillisTimestamp = offsetDateTime.toInstant().toEpochMilli() + fileCycleDurationInMillis;
    }

    private ZonedDateTime newFileOffset()
    {
        var time = Instant.now(fileCycleClock).toEpochMilli();
        var startIntervalTimestamp = time - (time % fileCycleDurationInMillis);
        return Instant.ofEpochMilli(startIntervalTimestamp)
                .atZone(fileCycleClock.getZone())
                .truncatedTo(ChronoUnit.MINUTES);
    }

    @Override
    public int size()
    {
        return count.get();
    }

    @Override
    public boolean isEmpty()
    {
        return count.get() == 0;
    }

    @Override
    public boolean contains(Object o)
    {
        return queue.contains(o);
    }

    @Override
    public Iterator<T> iterator()
    {
        return queue.iterator();
    }

    @Override
    public Object[] toArray()
    {
        return queue.toArray();
    }

    @Override
    public <T1> T1[] toArray(T1 @NotNull [] a)
    {
        return queue.toArray(a);
    }

    @Override
    public boolean add(T t)
    {
        boolean added = queue.add(t);
        if (added)
        {
            int count = this.count.incrementAndGet();
            long time = time();
            write(count, time);
        }
        return added;
    }

    @Override
    public boolean remove(Object o)
    {
        boolean removed = queue.remove(o);
        if (removed)
        {
            int count = this.count.decrementAndGet();
            long time = time();
            write(count, time);
        }
        return removed;
    }

    @Override
    public boolean containsAll(@NotNull Collection<?> c)
    {
        return queue.containsAll(c);
    }

    @Override
    public boolean addAll(@NotNull Collection<? extends T> c)
    {
        boolean added = queue.addAll(c);
        if (added)
        {
            int count = this.count.addAndGet(c.size());
            long time = time();
            write(count, time);
        }
        return added;
    }

    @Override
    public boolean removeAll(@NotNull Collection<?> c)
    {
        boolean removed = queue.removeAll(c);
        if (removed)
        {
            setAndWriteCurrentSize();
        }
        return removed;
    }

    @Override
    public boolean retainAll(@NotNull Collection<?> c)
    {
        boolean retained = queue.retainAll(c);
        if (retained)
        {
            setAndWriteCurrentSize();
        }
        return retained;
    }

    @ForceInline
    private void setAndWriteCurrentSize()
    {
        var currentSize = queue.size();
        count.set(currentSize);
        long time = time();
        write(currentSize, time);
    }

    @Override
    public void clear()
    {
        queue.clear();
        count.set(0);
        long time = time();
        write(0, time);
    }

    @Override
    public boolean offer(T t)
    {
        boolean offered = queue.offer(t);
        if (offered)
        {
            int count = this.count.incrementAndGet();
            long time = time();
            write(count, time);
        }
        return offered;
    }

    @Override
    public T remove()
    {
        T removed = queue.remove();
        int count = this.count.decrementAndGet();
        long time = time();
        write(count, time);
        return removed;
    }

    @Override
    public T poll()
    {
        T polled = queue.poll();
        if (polled != null)
        {
            int count = this.count.decrementAndGet();
            long time = time();
            write(count, time);
        }
        return polled;
    }

    private long time()
    {
        return Instant.now(fileCycleClock).toEpochMilli();
    }

    @Override
    public T element()
    {
        return queue.element();
    }

    @Override
    public T peek()
    {
        return queue.peek();
    }

    public static <T> QueueStatsBuilder<T> builder()
    {
        return new QueueStatsBuilder<>();
    }

    @Override
    @SneakyThrows
    public void close()
    {
        fileAccess.close();
    }

    protected void write(int count, long time)
    {
        if (time >= nextCycleMillisTimestamp)
        {
            if (recycleLock.tryLock())
            {
                try
                {
                    this.fileAccess.close();
                    initialize();
                } finally
                {
                    recycleLock.unlock();
                }
                write(count, time);
            }
            // drop writes during recycle
        } else
        {
            fileAccess.writeProbe(count, time);
        }
    }

    // only for testing
    public void setFileCycleClock(Clock fileCycleClock)
    {
        this.fileCycleClock = fileCycleClock;
    }

    @FieldDefaults(level = AccessLevel.PROTECTED)
    public static class QueueStatsBuilder<T>
    {
        Queue<T> queue;
        LogFileConfiguration logFileConfiguration;

        protected Tailer<Long, Integer> tailer;

        public QueueStatsBuilder<T> queue(Queue<T> queue)
        {
            this.queue = queue;
            return this;
        }

        public QueueStatsBuilder<T> logFileConfiguration(LogFileConfiguration logFileConfiguration)
        {
            this.logFileConfiguration = logFileConfiguration;
            return this;
        }

        public QueueStatsBuilder<T> tailer(Tailer<Long, Integer> tailer)
        {
            this.tailer = tailer;
            return this;
        }

        public MPMCQueueStats<T> build()
        {
            return new MPMCQueueStats<>(queue, logFileConfiguration, tailer);
        }

    }
}
