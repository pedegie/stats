package net.pedegie.stats.api.queue;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class StatsBlockingQueue<T> extends StatsQueue<T> implements BlockingQueue<T>
{
    private final BlockingQueue<T> blockingQueue;

    protected StatsBlockingQueue(BlockingQueue<T> queue, QueueConfiguration queueConfiguration)
    {
        super(queue, queueConfiguration);
        this.blockingQueue = queue;
    }

    @Override
    public void put(@NotNull T t) throws InterruptedException
    {
        blockingQueue.put(t);
        adder.increment();
        write(1);
    }

    @Override
    public boolean offer(T t, long timeout, @NotNull TimeUnit unit) throws InterruptedException
    {
        boolean added = blockingQueue.offer(t, timeout, unit);
        if (added)
        {
            adder.increment();
            write(1);
        }
        return added;
    }

    @NotNull
    @Override
    public T take() throws InterruptedException
    {
        T elem = blockingQueue.take();
        adder.decrement();
        write(1);
        return elem;
    }

    @Nullable
    @Override
    public T poll(long timeout, @NotNull TimeUnit unit) throws InterruptedException
    {
        T elem = blockingQueue.poll(timeout, unit);
        if (elem != null)
        {
            adder.decrement();
            write(1);
        }
        return elem;
    }

    @Override
    public int remainingCapacity()
    {
        return blockingQueue.remainingCapacity();
    }

    @Override
    public int drainTo(@NotNull Collection<? super T> c)
    {
        int transferred = blockingQueue.drainTo(c);
        if (transferred != 0)
        {
            adder.add(-transferred);
            write(transferred);
        }
        return transferred;
    }

    @Override
    public int drainTo(@NotNull Collection<? super T> c, int maxElements)
    {
        int transferred = blockingQueue.drainTo(c, maxElements);
        if (transferred != 0)
        {
            adder.add(-transferred);
            write(transferred);
        }
        return transferred;
    }
}
