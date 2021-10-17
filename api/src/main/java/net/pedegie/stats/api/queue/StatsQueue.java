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

    private static final int FREE = 0, BUSY = 1, CLOSED = 2;
    private static final AtomicIntegerFieldUpdater<StatsQueue> STATE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(StatsQueue.class, "state");
    @NonFinal
    private volatile int state = FREE;
    int delayBetweenWritesMillis;
    @NonFinal
    long nextWriteTimestamp;
    ProbeAccess probeWriter;

    @NonFinal
    volatile LongAdder adder = new LongAdder();
    Thread appenderThread;

    @Builder
    @SneakyThrows
    protected StatsQueue(Queue<T> queue, QueueConfiguration queueConfiguration)
    {
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
            this.delayBetweenWritesMillis = queueConfiguration.getDelayBetweenWritesMillis();
            this.nextWriteTimestamp = System.currentTimeMillis() + delayBetweenWritesMillis;
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
            write();
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
            write();
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
            adder.add(c.size());
            write();
        }
        return added;
    }

    @Override
    public boolean removeAll(@NotNull Collection<?> c)
    {
        var currentSize = queue.size();
        boolean removed = queue.removeAll(c);
        if (removed)
        {
            adder.add(queue.size() - currentSize);
            write();
        }
        return removed;
    }

    @Override
    public boolean retainAll(@NotNull Collection<?> c)
    {
        var currentSize = queue.size();
        boolean retained = queue.retainAll(c);
        if (retained)
        {
            adder.add(queue.size() - currentSize);
            write();
        }
        return retained;
    }

    @Override
    public void clear()
    {
        queue.clear();
        adder = new LongAdder();
        write();
    }

    @Override
    public boolean offer(T t)
    {
        boolean offered = queue.offer(t);
        if (offered)
        {
            adder.increment();
            write();
        }
        return offered;
    }

    @Override
    public T remove()
    {
        T removed = queue.remove();
        adder.decrement();
        write();
        return removed;
    }

    @Override
    public T poll()
    {
        T polled = queue.poll();
        if (polled != null)
        {
            adder.decrement();
            write();
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

    private void write()
    {
        try
        {
            var time = time();
            if (time >= nextWriteTimestamp)
            {
                if (STATE_UPDATER.compareAndSet(this, FREE, BUSY))
                {
                    try
                    {
                        write(time, appender);
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
        if (count > -1 && writeFilter.shouldWrite(count))
        {
            appender.writeBytes(bytes -> probeWriter.writeProbe(bytes, count, time));
            nextWriteTimestamp = time + delayBetweenWritesMillis;
        }
    }

    @Override
    public void close()
    {
        try
        {
            if (STATE_UPDATER.getAndSet(this, CLOSED) != CLOSED)
            {
                String path = chronicleQueue.file().getAbsolutePath();
                queues.remove(path);
                write(time(), acquireAppender());
                internalFileAccess.close(chronicleQueue);
            }
        } catch (Exception e)
        {
            accessErrorHandler.onError(e);
        }
    }

    private ExcerptAppender acquireAppender()
    {
        if (appender == null || Thread.currentThread() != appenderThread)
            return chronicleQueue.acquireAppender().disableThreadSafetyCheck(true);

        return appender;
    }
}
