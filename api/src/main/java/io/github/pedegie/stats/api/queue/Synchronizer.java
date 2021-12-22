package io.github.pedegie.stats.api.queue;

import lombok.experimental.Delegate;
import lombok.experimental.NonFinal;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.LongAdder;

enum Synchronizer
{
    CONCURRENT, NON_SYNCHRONIZED;

    Adder newAdder()
    {
        if (this == CONCURRENT)
        {
            return new ConcurrentAdder();
        }

        return new NonSynchronizedAdder();
    }

    StateUpdater newStateUpdater()
    {
        if (this == CONCURRENT)
        {
            return new ConcurrentStateUpdater();
        }

        return new NonConcurrentStateUpdater();
    }

    private static class ConcurrentAdder implements Adder
    {
        @Delegate
        private final LongAdder counter = new LongAdder();
    }

    private static class NonSynchronizedAdder implements Adder
    {
        long counter = 0;

        @Override
        public void increment()
        {
            counter++;
        }

        @Override
        public void decrement()
        {
            counter--;
        }

        @Override
        public void add(long size)
        {
            counter += size;
        }

        @Override
        public int intValue()
        {
            return (int) counter;
        }
    }

    private static class NonConcurrentStateUpdater implements StateUpdater
    {
        @NonFinal
        private int state = ConcurrentStateUpdater.FREE;

        @Override
        public boolean intoBusy()
        {
            if (state == ConcurrentStateUpdater.FREE)
            {
                state = ConcurrentStateUpdater.BUSY;
                return true;
            }
            return false;
        }

        @Override
        public void intoFree()
        {
            state = ConcurrentStateUpdater.FREE;
        }

        @Override
        public boolean intoClosing()
        {
            if (state != ConcurrentStateUpdater.CLOSED)
            {
                state = ConcurrentStateUpdater.CLOSING;
                return true;
            }
            return false;
        }

        @Override
        public void intoClosed()
        {
            state = ConcurrentStateUpdater.CLOSED;
        }
    }

    private static class ConcurrentStateUpdater implements StateUpdater
    {
        static final int FREE = 0, BUSY = 1, CLOSING = 2, CLOSED = 3;
        private static final AtomicIntegerFieldUpdater<ConcurrentStateUpdater> STATE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(ConcurrentStateUpdater.class, "state");
        @NonFinal
        private volatile int state = FREE;

        @Override
        public boolean intoBusy()
        {
            return STATE_UPDATER.compareAndSet(this, FREE, BUSY);
        }

        @Override
        public void intoFree()
        {
            STATE_UPDATER.set(this, FREE);
        }

        @Override
        public boolean intoClosing()
        {
            return STATE_UPDATER.getAndSet(this, CLOSING) != CLOSED;
        }

        @Override
        public void intoClosed()
        {
            STATE_UPDATER.set(this, CLOSED);
        }
    }

}

