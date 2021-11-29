package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
class Flusher implements Runnable
{
    private static final int ACCEPT_FLUSHABLE_MOD_COUNT = 32;
    private static final int FLUSH_MAX_TRIES = 3;
    int flushMaxTries;

    @NonFinal
    volatile Thread flusherThread;

    AtomicBoolean isRunning = new AtomicBoolean();
    PriorityQueue<TimestampedFlushable> flushables = new PriorityQueue<>(Comparator.comparing(s -> s.flushTimestamp));
    AtomicReference<TimestampedFlushable> newFlushable = new AtomicReference<>();
    AtomicBoolean pausing = new AtomicBoolean();
    @NonFinal
    boolean newHandler;

    public Flusher()
    {
        this(FLUSH_MAX_TRIES);
    }

    public Flusher(int flushMaxTries)
    {
        if (flushMaxTries < 1)
            throw new IllegalArgumentException("Incorrect flusher values. " +
                    "flushMaxTries [ " + flushMaxTries + " ] cannot be less than 1");

        this.flushMaxTries = flushMaxTries;
    }

    public void addFlushable(BatchFlushable flushable)
    {
        TimestampedFlushable timestampedFlushable = new TimestampedFlushable(flushable);
        if (flusherThread == null || flusherThread == Thread.currentThread())
        {
            flushables.add(timestampedFlushable);
            return;
        }

        do
        {
            newHandler = true;
            unpause();
        } while (isRunning.get() && !newFlushable.compareAndSet(null, timestampedFlushable));

    }

    public boolean start()
    {
        if (isRunning.getAndSet(true))
            return false;

        assert flusherThread == null || !flusherThread.isAlive();

        flusherThread = new Thread(this, "stats-flusher");
        flusherThread.setDaemon(true);
        flusherThread.start();
        return true;
    }

    public void stop()
    {
        isRunning.set(false);
        if (flusherThread != null)
        {
            LockSupport.unpark(flusherThread);
            BusyWaiter.busyWait(() -> !flusherThread.isAlive(), 5000, "waiting for flusher termination");
        }
    }

    @Override
    public void run()
    {
        int acceptFlushableModCount = ACCEPT_FLUSHABLE_MOD_COUNT;

        while (isRunning.get())
        {
            TimestampedFlushable flushable = flushables.poll();

            if (flushable == null && !acceptNewFlushable())
                pause();

            if (flushable == null) // spurious wakeup or closing flusher, continue to make decision
                continue;

            if (flushable.batchFlushable.isClosed())
                continue;

            var currentTime = System.currentTimeMillis();
            var nextFlushTimestamp = flushable.calculateNextFlushTimestamp();
            var waitMillis = nextFlushTimestamp - currentTime;

            if (waitMillis > 1 || --acceptFlushableModCount <= 0)
            {
                acceptFlushableModCount = ACCEPT_FLUSHABLE_MOD_COUNT;
                if (newFlushable(flushable))
                    continue;
            }

            pause(waitMillis);

            if (newHandler)
            {
                newHandler = false;
                if (newFlushable(flushable))
                    continue;
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

    private boolean newFlushable(TimestampedFlushable flushable)
    {
        if (acceptNewFlushable())
        {
            flushables.add(flushable);
            return true;
        }
        return false;
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

    private void pause()
    {
        pausing.set(true);
        LockSupport.park();
        pausing.set(false);
    }

    private void pause(long millis)
    {
        if (millis < 1)
            return;

        pausing.set(true);
        if (!Thread.currentThread().isInterrupted())
            LockSupport.parkNanos(millis * 1_000_000);
        pausing.set(false);
    }

    private void unpause()
    {
        Thread thread = this.flusherThread;
        if (thread != null && pausing.get())
            LockSupport.unpark(thread);
    }

    private boolean acceptNewFlushable()
    {
        TimestampedFlushable flushable = newFlushable.getAndSet(null);
        if (flushable == null)
            return false;

        flushables.add(flushable);
        return true;
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
