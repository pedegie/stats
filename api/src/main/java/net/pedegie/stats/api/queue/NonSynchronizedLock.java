package net.pedegie.stats.api.queue;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

class NonSynchronizedLock implements Lock
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
