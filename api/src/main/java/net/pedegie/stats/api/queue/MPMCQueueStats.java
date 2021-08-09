package net.pedegie.stats.api.queue;

import net.openhft.chronicle.core.annotation.ForceInline;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import net.pedegie.stats.api.tailer.Tailer;
import net.pedegie.stats.api.tailer.Tailers;
import net.pedegie.stats.api.util.BlockSize;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

public class MPMCQueueStats<T> implements Queue<T>, Closeable
{
    protected final Queue<T> queue;
    protected final ChronicleQueue statsQueue;
    protected final AtomicInteger count = new AtomicInteger(0);

    protected MPMCQueueStats(Queue<T> queue, Path fileName, Tailer<Long, Integer> tailer)
    {
        this.queue = queue;
        this.statsQueue = SingleChronicleQueueBuilder
                .single(fileName.toAbsolutePath().toString())
                .wireType(WireType.FIELDLESS_BINARY)
                .readOnly(false)
                .blockSize(BlockSize.blockSize(fileName.getParent()))
                .build();

        if (tailer != null)
        {
            Tailers.addTailer(statsQueue.createTailer(), tailer);
        }
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
    public <T1> T1[] toArray(T1[] a)
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
    public boolean containsAll(Collection<?> c)
    {
        return queue.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c)
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
    public boolean removeAll(Collection<?> c)
    {
        boolean removed = queue.removeAll(c);
        if (removed)
        {
            setAndWriteCurrentSize();
        }
        return removed;
    }

    @Override
    public boolean retainAll(Collection<?> c)
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
    public void close()
    {
        statsQueue.close();
    }

    protected void write(int count, long nanoTime)
    {
        statsQueue.acquireAppender()
                .writeBytes(b -> b
                        .writeLong(nanoTime)
                        .writeInt(count));
    }

    public static class QueueStatsBuilder<T>
    {
        protected Queue<T> queue;
        protected Path fileName;

        protected Tailer<Long,Integer> tailer;

        public QueueStatsBuilder<T> queue(Queue<T> queue)
        {
            this.queue = queue;
            return this;
        }

        public QueueStatsBuilder<T> fileName(Path fileName)
        {
            this.fileName = fileName;
            return this;
        }

        public QueueStatsBuilder<T> tailer(Tailer<Long,Integer> tailer)
        {
            this.tailer = tailer;
            return this;
        }
        public MPMCQueueStats<T> build()
        {
            return new MPMCQueueStats<>(queue, fileName, tailer);
        }

    }
}
