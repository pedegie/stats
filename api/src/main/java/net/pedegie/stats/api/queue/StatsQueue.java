package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.core.annotation.ForceInline;
import net.pedegie.stats.api.tailer.Tailer;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.time.Clock;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

@FieldDefaults(makeFinal = true, level = AccessLevel.PROTECTED)
@Slf4j
public class StatsQueue<T> implements Queue<T>, Closeable
{
    private static final FileAccessWorker fileAccessWorker = new FileAccessWorker();
    Clock fileCycleClock;
    Queue<T> queue;
    Counter count;
    Lock closeLock;
    WriteFilter writeFilter;
    AtomicBoolean closed;
    private final int fileAccessId;

    @Builder
    @SneakyThrows
    protected StatsQueue(Queue<T> queue, QueueConfiguration queueConfiguration, Tailer<Long, Integer> tailer)
    {
        QueueConfigurationValidator.validate(queueConfiguration);
        logConfiguration(queueConfiguration);
        this.queue = queue;
        this.writeFilter = queueConfiguration.getWriteFilter();
        this.count = queueConfiguration.getSynchronizer().newCounter();
        this.closeLock = queueConfiguration.getSynchronizer().newLock();
        this.fileCycleClock = queueConfiguration.getFileCycleClock();
        fileAccessWorker.start();
        var registerResult = fileAccessWorker.registerFile(queueConfiguration).get(30, TimeUnit.SECONDS);
        this.fileAccessId = registerResult.getA();
        this.closed = registerResult.getB();

    }

    private void logConfiguration(QueueConfiguration conf)
    {
        log.info("Initializing queue with:\n" +
                        "path: {}\n" +
                        "mmapSize: {} B\n" +
                        "fileCycleDurationInMillis: {}\n" +
                        "disableCompression: {}\n" +
                        "disableSynchronization: {}\n" +
                        "preTouchEnabled: {}\n" +
                        "unmapOnClose: {}",
                conf.getPath(), conf.getMmapSize(), conf.getFileCycleDurationInMillis(),
                conf.isDisableCompression(), conf.isDisableSynchronization(), conf.isPreTouch(), conf.isUnmapOnClose());
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
        if (!closed.get() && added)
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
        if (!closed.get() && removed)
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
        if (!closed.get() && added)
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
        if (!closed.get() && removed)
        {
            setAndWriteCurrentSize();
        }
        return removed;
    }

    @Override
    public boolean retainAll(@NotNull Collection<?> c)
    {
        boolean retained = queue.retainAll(c);
        if (!closed.get() && retained)
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
        if (!closed.get())
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
        if (!closed.get() && offered)
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
        if (!closed.get())
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
        if (!closed.get() && polled != null)
        {
            int count = this.count.decrementAndGet();
            long time = time();
            write(count, time);
        }
        return polled;
    }

    private long time()
    {
        return fileCycleClock.millis();
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

    private void write(int count, long time)
    {
        if (writeFilter.shouldWrite(count, time))
        {
            fileAccessWorker.writeProbe(new Probe(fileAccessId, count, time));
        }
    }

    @Override
    public void close()
    {
        if (closeLock.tryLock())
        {
            try
            {
                if (closed.get())
                    return;

                fileAccessWorker.close(fileAccessId);
            } finally
            {
                closeLock.unlock();
            }
        }
    }

    public void closeBlocking()
    {
        close();
        BusyWaiter.busyWait(() -> !closed.get(), "close blocking");
    }

    public boolean isClosed()
    {
        return closed.get();
    }

    public static void shutdown()
    {
        fileAccessWorker.shutdown();
    }

    public static void shutdownForce()
    {
        fileAccessWorker.shutdownForce();
    }
}
