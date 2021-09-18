package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.core.annotation.ForceInline;
import net.pedegie.stats.api.tailer.Tailer;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

@FieldDefaults(makeFinal = true, level = AccessLevel.PROTECTED)
@Slf4j
public class MPMCQueueStats<T> implements Queue<T>, Closeable
{
    @NonFinal // its effectively final, mutable just for tests purposes which re-sets logFileConfiguration with new Clock
    LogFileConfiguration logFileConfiguration;
    Queue<T> queue;
    AtomicInteger count = new AtomicInteger(0);
    ReentrantLock recycleLock = new ReentrantLock();
    WriteFilter writeFilter;

    @NonFinal
    FileAccess fileAccess;
    @NonFinal
    long nextCycleMillisTimestamp;
    @NonFinal
    private volatile boolean disabledDueCrash;

    @Builder
    protected MPMCQueueStats(Queue<T> queue, LogFileConfiguration logFileConfiguration, WriteFilter writeFilter, Tailer<Long, Integer> tailer)
    {
        this.queue = queue;
        this.writeFilter = writeFilter == null ? WriteFilter.acceptAllFilter() : writeFilter;

        if (recycleLock.tryLock())
        {
            this.logFileConfiguration = logFileConfiguration;
            try
            {
                initializeFileAccess(logFileConfiguration);
            } finally
            {
                recycleLock.unlock();
            }
        }
    }

    private void initializeFileAccess(LogFileConfiguration logFileConfiguration)
    {
        this.fileAccess = FileAccessStrategy.accept(logFileConfiguration);
        this.nextCycleMillisTimestamp = fileAccess.getStartCycleMillis() + logFileConfiguration.getFileCycleDurationInMillis();
    }

    @Override
    public int size()
    {
        return queue.size();
    }

    @Override
    public boolean isEmpty()
    {
        return queue.isEmpty();
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
        if (!disabledDueCrash && added)
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
        if (!disabledDueCrash && removed)
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
        if (!disabledDueCrash && added)
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
        if (!disabledDueCrash && removed)
        {
            setAndWriteCurrentSize();
        }
        return removed;
    }

    @Override
    public boolean retainAll(@NotNull Collection<?> c)
    {
        boolean retained = queue.retainAll(c);
        if (!disabledDueCrash && retained)
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
        if (!disabledDueCrash)
        {
            count.set(0);
            long time = time();
            write(0, time);
        }
    }

    @Override
    public boolean offer(T t)
    {
        boolean offered = queue.offer(t);
        if (!disabledDueCrash && offered)
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
        if (!disabledDueCrash)
        {
            int count = this.count.decrementAndGet();
            long time = time();
            write(count, time);
        }
        return removed;
    }

    @Override
    public T poll()
    {
        T polled = queue.poll();
        if (!disabledDueCrash && polled != null)
        {
            int count = this.count.decrementAndGet();
            long time = time();
            write(count, time);
        }
        return polled;
    }

    private long time()
    {
        return Instant.now(logFileConfiguration.getFileCycleClock()).toEpochMilli();
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

    @Override
    public void close()
    {
        fileAccess.close();
    }

    private void write(int count, long time)
    {
        try
        {
            if (writeFilter.shouldWrite(count, time))
            {
                write(count, time, 1);
            }
        } catch (Exception e)
        {
            disabledDueCrash = true;
            log.error("Error occurred during writing to log file. Disabling writing to file.", e);
            fileAccess.close();
        }
    }

    protected void write(int count, long time, int tries)
    {
        if (time >= nextCycleMillisTimestamp)
        {
            if (recycleLock.tryLock())
            {
                try
                {
                    this.fileAccess.close();
                    initializeFileAccess(logFileConfiguration);
                } finally
                {
                    recycleLock.unlock();
                }
                if (tries > 3)
                {
                    throw new IllegalStateException("Cannot recycle file");
                }
                write(count, time, tries + 1);
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
        this.logFileConfiguration = logFileConfiguration.withFileCycleClock(fileCycleClock);
    }
}
