package net.pedegie.stats.api.queue;

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
}
