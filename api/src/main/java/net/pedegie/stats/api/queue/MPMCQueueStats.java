package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import net.openhft.chronicle.core.annotation.ForceInline;
import net.pedegie.stats.api.queue.fileaccess.FileAccess;
import net.pedegie.stats.api.queue.fileaccess.FileAccessStrategy;
import net.pedegie.stats.api.tailer.Tailer;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

@FieldDefaults(makeFinal = true, level = AccessLevel.PROTECTED)
public class MPMCQueueStats<T> implements Queue<T>, Closeable
{
    Queue<T> queue;
    AtomicInteger count = new AtomicInteger(0);
    FileAccess fileAccess;

    @SneakyThrows
    protected MPMCQueueStats(Queue<T> queue, LogFileConfiguration logFileConfiguration, Tailer<Long, Integer> tailer)
    {
        this.fileAccess = FileAccessStrategy.accept(logFileConfiguration);
        this.queue = queue;
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
            long nanoTime = System.nanoTime();
            write(count, nanoTime);
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
            long nanoTime = System.nanoTime();
            write(count, nanoTime);
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
            long nanoTime = System.nanoTime();
            write(count, nanoTime);
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
        long nanoTime = System.nanoTime();
        write(currentSize, nanoTime);
    }

    @Override
    public void clear()
    {
        queue.clear();
        count.set(0);
        long nanoTime = System.nanoTime();
        write(0, nanoTime);
    }

    @Override
    public boolean offer(T t)
    {
        boolean offered = queue.offer(t);
        if (offered)
        {
            int count = this.count.incrementAndGet();
            long nanoTime = System.nanoTime();
            write(count, nanoTime);
        }
        return offered;
    }

    @Override
    public T remove()
    {
        T removed = queue.remove();
        int count = this.count.decrementAndGet();
        long nanoTime = System.nanoTime();
        write(count, nanoTime);
        return removed;
    }

    @Override
    public T poll()
    {
        T polled = queue.poll();
        if (polled != null)
        {
            int count = this.count.decrementAndGet();
            long nanoTime = System.nanoTime();
            write(count, nanoTime);
        }
        return polled;
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

    protected void write(int count, long nanoTime)
    {
        fileAccess.writeProbe(count, nanoTime);
    }

    public void resetOffset()
    {
        fileAccess.resetOffset();
    }

    @FieldDefaults(level = AccessLevel.PROTECTED)
    public static class QueueStatsBuilder<T>
    {
        Queue<T> queue;
        LogFileConfiguration logFileConfiguration;
        int mmapSize;

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
