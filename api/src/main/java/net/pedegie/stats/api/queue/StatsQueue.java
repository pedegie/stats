package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.Pretoucher;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.pedegie.stats.api.queue.probe.ProbeAccess;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.LongAdder;

@FieldDefaults(makeFinal = true, level = AccessLevel.PROTECTED)
@Slf4j
public class StatsQueue<T> implements Queue<T>, Closeable
{
    private static final ConcurrentHashMap<String, Boolean> queues = new ConcurrentHashMap<>();
    Queue<T> queue;
    WriteFilter writeFilter;
    SingleChronicleQueue chronicleQueue;
    ExcerptAppender appender;
    FileAccessErrorHandler accessErrorHandler;
    InternalFileAccess internalFileAccess;

    private static final int FREE = 0, BUSY = 1, CLOSING = 2, CLOSED = 3;
    private static final AtomicIntegerFieldUpdater<StatsQueue> STATE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(StatsQueue.class, "state");
    @NonFinal
    private volatile int state = FREE;
    FlushThreshold flushThreshold;
    @NonFinal
    long nextWriteTimestamp;
    ProbeAccess probeWriter;

    @NonFinal
    volatile LongAdder adder = new LongAdder();
    Thread appenderThread;

    @NonFinal
    boolean firstClose = true;

    @Builder
    @SneakyThrows
    protected StatsQueue(Queue<T> queue, QueueConfiguration queueConfiguration)
    {
        System.setProperty("disable.thread.safety", "true");
        if (queues.putIfAbsent(queueConfiguration.getPath().toString(), Boolean.TRUE) != null)
        {
            throw new IllegalArgumentException("Queue which appends to " + queueConfiguration.getPath() + " already exists");
        }
        try
        {
            QueueConfigurationValidator.validate(queueConfiguration);
            logConfiguration(queueConfiguration);
            this.queue = queue;
            this.writeFilter = queueConfiguration.getWriteFilter();
            this.chronicleQueue = SingleChronicleQueueBuilder
                    .binary(queueConfiguration.getPath())
                    .rollCycle(queueConfiguration.getRollCycle())
                    .blockSize(FileUtils.roundToPageSize(queueConfiguration.getMmapSize()))
                    .build();
            this.accessErrorHandler = queueConfiguration.getErrorHandler();

            if (queueConfiguration.isPreTouch())
            {
                new Pretoucher(chronicleQueue).execute();
            }
            this.appender = acquireAppender();
            this.appenderThread = Thread.currentThread();
            this.probeWriter = queueConfiguration.getProbeAccess();
            this.internalFileAccess = queueConfiguration.getInternalFileAccess();
            this.flushThreshold = queueConfiguration.getFlushThreshold();
            this.nextWriteTimestamp = time();
        } catch (Exception e)
        {
            queues.remove(queueConfiguration.getPath().toString());
            throw e;
        }
    }

    private void logConfiguration(QueueConfiguration conf)
    {
        log.info("Initializing queue with:\n" +
                        "path: {}\n" +
                        "mmapSize: {} B\n" +
                        "rollCycle: {}\n" +
                        "disableCompression: {}\n" +
                        "disableSynchronization: {}\n" +
                        "preTouchEnabled: {}",
                conf.getPath(), conf.getMmapSize(), conf.getRollCycle(),
                conf.isDisableCompression(), conf.isDisableSynchronization(), conf.isPreTouch());
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
        if (added)
        {
            adder.increment();
            write(1);
        }
        return added;
    }

    @Override
    public boolean remove(Object o)
    {
        boolean removed = queue.remove(o);
        if (removed)
        {
            adder.decrement();
            write(1);
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
            int size = c.size();
            adder.add(size);
            write(size);
        }
        return added;
    }

    @Override
    public boolean removeAll(@NotNull Collection<?> c)
    {
        var currentSize = queue.size();
        boolean removed = queue.removeAll(c);
        if (removed && currentSize > 0)
        {
            int negativeDiff = queue.size() - currentSize;
            if (negativeDiff != 0)
                adder.add(negativeDiff);

            write(-negativeDiff);
        }
        return removed;
    }

    @Override
    public boolean retainAll(@NotNull Collection<?> c)
    {
        var currentSize = queue.size();
        boolean retained = queue.retainAll(c);
        if (retained && currentSize > 0)
        {
            int negativeDiff = queue.size() - currentSize;
            if (negativeDiff != 0)
                adder.add(negativeDiff);

            write(-negativeDiff);
        }
        return retained;
    }

    @Override
    public void clear()
    {
        var difference = queue.size();
        queue.clear();
        adder = new LongAdder();
        write(difference);
    }

    @Override
    public boolean offer(T t)
    {
        boolean offered = queue.offer(t);
        if (offered)
        {
            adder.increment();
            write(1);
        }
        return offered;
    }

    @Override
    public T remove()
    {
        T removed = queue.remove();
        adder.decrement();
        write(1);
        return removed;
    }

    @Override
    public T poll()
    {
        T polled = queue.poll();
        if (polled != null)
        {
            adder.decrement();
            write(1);
        }
        return polled;
    }

    private long time()
    {
        return System.currentTimeMillis();
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

    private void write(int difference)
    {
        try
        {
            var time = time();
            if (time >= nextWriteTimestamp || difference >= flushThreshold.getMinSizeDifference())
            {
                if (STATE_UPDATER.compareAndSet(this, FREE, BUSY))
                {
                    try
                    {
                        write(time, appender);
                        nextWriteTimestamp = time + flushThreshold.getDelayBetweenWritesMillis();
                    } finally
                    {
                        state = FREE;
                    }
                }
            }
        } catch (Exception e)
        {
            if (accessErrorHandler.onError(e))
            {
                close();
            }
        }
    }

    private void write(long time, ExcerptAppender appender)
    {
        var count = adder.intValue();
        if (count > -1 && writeFilter.shouldWrite(count, time))
        {
            appender.writeBytes(bytes -> probeWriter.writeProbe(bytes, count, time));
        }
    }

    @Override
    public void close()
    {
        try
        {
            if (STATE_UPDATER.getAndSet(this, CLOSING) != CLOSED)
            {
                if (firstClose)
                {
                    flush();
                    firstClose = false;
                }
                internalFileAccess.close(chronicleQueue);
                queues.remove(chronicleQueue.file().getAbsolutePath());
                STATE_UPDATER.set(this, CLOSED);
            }
        } catch (Exception e)
        {
            accessErrorHandler.onError(e);
        }
    }

    private void flush()
    {
        write(time(), acquireAppender());
    }

    private ExcerptAppender acquireAppender()
    {
        if (appender == null || Thread.currentThread() != appenderThread)
            return chronicleQueue.acquireAppender();

        return appender;
    }
}
