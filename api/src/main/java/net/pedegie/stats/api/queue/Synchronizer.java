package net.pedegie.stats.api.queue;

import lombok.experimental.Delegate;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

enum Synchronizer
{
    CONCURRENT, NON_SYNCHRONIZED;

    Counter newCounter()
    {
        if (this == CONCURRENT)
        {
            return new ConcurrentCounter();
        }

        return new NonSynchronizedCounter();
    }

    Lock newLock()
    {
        if (this == CONCURRENT)
        {
            return new ReentrantLock();
        }

        return new NonSynchronizedLock();
    }

    private static class ConcurrentCounter implements Counter
    {
        @Delegate
        private final AtomicInteger counter = new AtomicInteger();
    }

    private static class NonSynchronizedCounter implements Counter
    {
        int counter;

        @Override
        public int incrementAndGet()
        {
            return ++counter;
        }

        @Override
        public int decrementAndGet()
        {
            return --counter;
        }

        @Override
        public int addAndGet(int size)
        {
            return counter += size;
        }

        @Override
        public void set(int currentSize)
        {
            counter = currentSize;
        }

        @Override
        public int getAndAdd(int probeSize)
        {
            var tmp = counter;
            counter += probeSize;
            return tmp;
        }

        @Override
        public int get()
        {
            return counter;
        }
    }

    private static class NonSynchronizedLock implements Lock
    {
        @Override
        public void lock()
        {

        }

        @Override
        public void lockInterruptibly()
        {

        }

        @Override
        public boolean tryLock()
        {
            return true;
        }

        @Override
        public boolean tryLock(long time, @NotNull TimeUnit unit)
        {
            return true;
        }

        @Override
        public void unlock()
        {

        }

        @NotNull
        @Override
        public Condition newCondition()
        {
            throw new UnsupportedOperationException("Not implemented");
        }
    }

}
