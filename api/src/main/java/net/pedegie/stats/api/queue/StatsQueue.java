package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.Pretoucher;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.pedegie.stats.api.queue.probe.ProbeAccess;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import static net.pedegie.stats.api.queue.probe.ProbeHolder.PROBE_SIZE;

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
    boolean disableSync;
    StateUpdater stateUpdater;

    FlushThreshold flushThreshold;
    Adder dropped;
    @NonFinal
    long nextWriteTimestamp;
    ProbeAccess probeWriter;

    @NonFinal
    volatile Adder adder;
    Thread appenderThread;

    @NonFinal
    boolean firstClose = true;
    @SuppressWarnings("rawtypes")
    Bytes batchBytes;

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
                    .blockSize(queueConfiguration.getMmapSize())
                    .build(); // todo index spacing
            this.accessErrorHandler = queueConfiguration.getErrorHandler();

            if (queueConfiguration.isPreTouch())
            {
                new Pretoucher(chronicleQueue).execute();
            }
            this.disableSync = queueConfiguration.isDisableSynchronization();
            this.appender = acquireAppender();
            this.appenderThread = Thread.currentThread();
            this.probeWriter = queueConfiguration.getProbeAccess();
            this.internalFileAccess = queueConfiguration.getInternalFileAccess();
            this.flushThreshold = queueConfiguration.getFlushThreshold();
            this.nextWriteTimestamp = time();
            this.dropped = queueConfiguration.isCountDropped() ? newAdder() : null;
            this.adder = newAdder();
            this.stateUpdater = disableSync ? Synchronizer.NON_SYNCHRONIZED.newStateUpdater() : Synchronizer.CONCURRENT.newStateUpdater();
            this.batchBytes = Bytes.allocateDirect((long) queueConfiguration.getBatchSize() * PROBE_SIZE);
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
        adder = newAdder();
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
        var time = time();
        if (messagesNotComesTooFast(difference, time) && stateUpdater.intoBusy())
        {
            try
            {
                write(time, appender, false);
                nextWriteTimestamp = time + flushThreshold.getDelayBetweenWritesMillis();
                stateUpdater.intoFree();
            } catch (Exception e)
            {
                if (accessErrorHandler.onError(e))
                {
                    close();
                } else
                {
                    stateUpdater.intoFree();
                }
            }
        } else if (countDropped())
        {
            dropped.increment();
        }
    }

    private boolean messagesNotComesTooFast(int difference, long time)
    {
        return time >= nextWriteTimestamp || difference >= flushThreshold.getMinSizeDifference();
    }

    private void write(long time, ExcerptAppender appender, boolean flush)
    {
        var count = adder.intValue();
        if (count > -1 && writeFilter.shouldWrite(count, time))
        {
            probeWriter.writeProbe(batchBytes, count, time);

            if (batchBytes.realCapacity() - batchBytes.writePosition() == 0 || flush)
            {
                try (DocumentContext dc = appender.writingDocument())
                {
                    probeWriter.batchWrite(dc.wire().bytes(), batchBytes);
                }
                batchBytes.clear();
            }
        } else if (countDropped())
        {
            dropped.increment();
        }
    }

    @Override
    public void close()
    {
        try
        {
            if (stateUpdater.intoClosing())
            {
                if (firstClose)
                {
                    flush();
                    batchBytes.releaseLast();
                    firstClose = false;
                }
                internalFileAccess.close(chronicleQueue);
                queues.remove(chronicleQueue.file().getAbsolutePath());
                stateUpdater.intoClosed();
            }
        } catch (Exception e)
        {
            accessErrorHandler.onError(e);
        }
    }

    public long getDropped()
    {
        return countDropped() ? dropped.sum() : -1;
    }

    private boolean countDropped()
    {
        return dropped != null;
    }

    private void flush()
    {
        write(time(), acquireAppender(), true);
    }

    private ExcerptAppender acquireAppender()
    {
        if (appender == null || Thread.currentThread() != appenderThread)
            return chronicleQueue.acquireAppender();

        return appender;
    }

    private Adder newAdder()
    {
        return disableSync ? Synchronizer.NON_SYNCHRONIZED.newAdder() : Synchronizer.CONCURRENT.newAdder();
    }
}
