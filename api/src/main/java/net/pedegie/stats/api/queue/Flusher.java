package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Slf4j
class Flusher implements Runnable
{
    private static final int BUSY_WAIT_MILLIS_BOUND = 50;
    private static final int FLUSH_MAX_TRIES = 3;

    int busyWaitMillisBound;
    int flushMaxTries;

    @NonFinal
    volatile Thread flusherThread;

    AtomicBoolean isRunning = new AtomicBoolean();
    Lock lock = new ReentrantLock();
    Condition notEmpty = lock.newCondition();

    PriorityQueue<TimestampedFlushable> flushables = new PriorityQueue<>(Comparator.comparing(s -> s.flushTimestamp));

    public Flusher()
    {
        this(BUSY_WAIT_MILLIS_BOUND, FLUSH_MAX_TRIES);
    }

    public Flusher(int busyWaitMillisBound, int flushMaxTries)
    {
        if (busyWaitMillisBound < 0 || flushMaxTries < 1)
            throw new IllegalArgumentException("Incorrect flusher values. " +
                    "busyWaitMillisBound [ " + busyWaitMillisBound + " ] cannot be less than 0, " +
                    "flushMaxTries [ " + flushMaxTries + " ] cannot be less than 1");

        this.busyWaitMillisBound = busyWaitMillisBound;
        this.flushMaxTries = flushMaxTries;
    }

    public void addFlushable(BatchFlushable flushable)
    {
        lock.lock();
        try
        {
            flushables.add(new TimestampedFlushable(flushable));
            if (flushables.size() == 1)
                notEmpty.signal();
        } finally
        {
            lock.unlock();
        }
    }

    public boolean start()
    {
        if (isRunning.getAndSet(true))
            return false;

        assert flusherThread == null || !flusherThread.isAlive();

        flusherThread = new Thread(this, "stats-flusher");
        flusherThread.start();
        return true;
    }

    public void stop()
    {
        isRunning.set(false);
        flusherThread.interrupt();
    }

    @Override
    public void run()
    {
        while (isRunning.get())
        {
            var flushable = flushables.poll();
            if (flushable == null)
            {
                boolean interrupted = waitForFlushable();
                if (interrupted)
                {
                    stop();
                    break;
                }
                flushable = flushables.poll();
            }

            if (flushable.batchFlushable.isClosed())
                continue;

            var currentTime = System.currentTimeMillis();
            var nextFlushTimestamp = flushable.calculateNextFlushTimestamp();
            var waitMillis = nextFlushTimestamp - currentTime;

            if (waitMillis > busyWaitMillisBound)
            {
                boolean interrupted = sleep(waitMillis);
                if (interrupted)
                {
                    stop();
                    break;
                }

            } else if (waitMillis > 0)
            {
                BusyWaiter.busyWait(waitMillis);
            }

            boolean flushed = flush(flushable, nextFlushTimestamp);
            if (flushed)
                flushable.flushTimestamp = flushable.calculateNextFlushTimestamp();
            else
                flushable.flushTimestamp = nextFlushTimestamp + flushable.batchFlushable.flushIntervalMillis();

            flushables.add(flushable);
        }

        flushables.clear();
    }

    private boolean flush(TimestampedFlushable flushable, long nextFlushTimestamp)
    {
        for (int i = 0; i < flushMaxTries; i++)
        {
            if (flushedInMeanwhile(flushable, nextFlushTimestamp))
                return true;

            if (flushable.batchFlushable.batchFlush())
                return true;

            BusyWaiter.busyWait(1);
        }
        return false;
    }

    private boolean flushedInMeanwhile(TimestampedFlushable flushable, long nextFlushTimestamp)
    {
        return flushable.calculateNextFlushTimestamp() != nextFlushTimestamp;
    }

    private boolean waitForFlushable()
    {
        lock.lock();
        try
        {
            while (flushables.isEmpty())
                notEmpty.await();
        } catch (InterruptedException e)
        {
            return true;
        } finally
        {
            lock.unlock();
        }
        return false;
    }

    private boolean sleep(long millis)
    {
        try
        {
            Thread.sleep(millis);
        } catch (InterruptedException e)
        {
            return true;
        }
        return false;
    }

    private static class TimestampedFlushable
    {
        private final BatchFlushable batchFlushable;
        private long flushTimestamp;

        private TimestampedFlushable(BatchFlushable batchFlushable)
        {
            this.batchFlushable = batchFlushable;
            if (batchFlushable.lastBatchFlushTimestamp() == 0)
            {
                this.flushTimestamp = System.currentTimeMillis() + batchFlushable.flushIntervalMillis();
            } else
            {
                this.flushTimestamp = calculateNextFlushTimestamp();
            }
        }

        private long calculateNextFlushTimestamp()
        {
            long lastBatchFlushTimestamp = batchFlushable.lastBatchFlushTimestamp();
            if (lastBatchFlushTimestamp == 0)
            {
                return flushTimestamp;
            }

            return lastBatchFlushTimestamp + batchFlushable.flushIntervalMillis();
        }
    }
}
