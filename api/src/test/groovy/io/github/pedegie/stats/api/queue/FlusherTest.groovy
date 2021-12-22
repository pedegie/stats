package io.github.pedegie.stats.api.queue


import spock.lang.Specification

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class FlusherTest extends Specification
{
    def "should correctly close flusher, when stop command is invoked during waiting on new flushable"()
    {
        given:
            Flusher flusher = new Flusher()
            flusher.start()
            sleep(2)
        when:
            flusher.stop()
        then:
            BusyWaiter.busyWaitMillis({ flusher.flusherThread.state == Thread.State.TERMINATED }, 3000, "waiting for flusher thread termination")
        cleanup:
            flusher.stop()
    }

    def "running flusher more than once, should has no effect"()
    {
        given:
            Flusher flusher = new Flusher()
        when:
            boolean started = flusher.start()
            sleep(2)
        then:
            started
        when:
            started &= flusher.start()
            sleep(2)
            started &= flusher.start()
        then:
            !started
        cleanup:
            flusher.stop()
    }

    def "stopping flusher more than once, should has no effect"()
    {
        given:
            Flusher flusher = new Flusher()
            flusher.start()
            sleep(2)
        when:
            flusher.stop()
            sleep(2)
            flusher.stop()
        then:
            noExceptionThrown()
        cleanup:
            flusher.stop()
    }

    def "should be able to start, then stop, then start again flusher"()
    {
        given:
            Flusher flusher = new Flusher()
        when:
            flusher.start()
            sleep(2)
            flusher.stop()
            sleep(2)
            flusher.start()
        then:
            1 == 1
        cleanup:
            flusher.stop()
    }

    def "should remove flushables when they become closed"()
    {
        given:
            TestFlushable flushable1 = new TestFlushable(100)
            TestFlushable flushable2 = new TestFlushable(100)
            Flusher flusher = new Flusher()
            flusher.addFlushable(flushable1)
            flusher.addFlushable(flushable2)
            int flushables = flusher.flushables.size()
            flusher.start()
        when:
            flushable1.close()
            flushable2.close()
        then:
            flushables == 2 // it may be 2 or only 1 if one is currently processing
            BusyWaiter.busyWaitMillis({ flusher.flushables.size() == 0 }, 300, "waiting for removing flushables")
        cleanup:
            flusher.stop()
    }

    def "should flush flushables within given interval"()
    {
        given:
            Flusher flusher = new Flusher(1)
            TestFlushable flushable1 = new TestFlushable(100)
            TestFlushable flushable2 = new TestFlushable(300)
            TestFlushable flushable3 = new TestFlushable(1000)
            flusher.addFlushable(flushable1)
            flusher.addFlushable(flushable2)
            flusher.addFlushable(flushable3)
            flusher.start()
        when:
            boolean finishedInTme = BusyWaiter.busyWaitMillis({ flushable1.flushedTimes == 10 && flushable2.flushedTimes == 3 && flushable3.flushedTimes == 1 }, 1150, "waiting for flushables")
        then:
            flushable1.flushedTimes < 13
            flushable2.flushedTimes == 3
            flushable3.flushedTimes == 1
            finishedInTme
        cleanup:
            flusher.stop()
    }

    def "adding flusher with less interval, than currently processing, should takes precedence"()
    {
        given:
            Flusher flusher = new Flusher(1)
            flusher.start()
            TestFlushable flushable1 = new TestFlushable(1000)
            TestFlushable flushable2 = new TestFlushable(300)
        when:
            flusher.addFlushable(flushable1)
            sleep(5)
            flusher.addFlushable(flushable2)
            boolean finishedInTme = BusyWaiter.busyWaitMillis({ flushable1.flushedTimes == 1 && flushable2.flushedTimes == 3 }, 1150, "waiting for flushables2")
        then:
            flushable1.flushedTimes == 1
            flushable2.flushedTimes == 3
            finishedInTme
        cleanup:
            flusher.stop()
    }

    def "should try to flush n times and then postpone flush to next interval"()
    {
        given:
            Flusher flusher = new Flusher(3)
            TestFlushable flushable = new TestFlushable(100, 4)
            flusher.start()
            long start = System.nanoTime()
            flusher.addFlushable(flushable)
        when:
            boolean flushed = BusyWaiter.busyWaitMillis({ flushable.lastBatchFlushTimestamp != 0 }, 215, "postpone flush")
        then:
            flushed
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) > 190
        cleanup:
            flusher.stop()
    }

    def "should continue flushing if error happens during flushing flushable"()
    {
        given:
            Flusher flusher = new Flusher()
            flusher.start()
            TestFlushable flushable = Spy(TestFlushable, constructorArgs: [100L])
            AtomicInteger invokedFlushTimes = new AtomicInteger()
            flushable.batchFlush() >> { invokedFlushTimes.incrementAndGet(); throw new TestExpectedException() }
        when:
            flusher.addFlushable(flushable)
        then:
            BusyWaiter.busyWaitMillis({ invokedFlushTimes.get() > 1 }, 5000, "waiting for flushes")
        cleanup:
            flusher.stop()
    }

    def "should throw an exception if passed parameters are less than min values"()
    {
        given:
            int maxTries = flushMaxTries
        when:
            new Flusher(maxTries)
        then:
            thrown(IllegalArgumentException)
        where:
            flushMaxTries << [0, -1]
    }

    private static class TestFlushable implements BatchFlushable
    {
        private final long interval
        private final int flushAt

        private volatile boolean closed
        int flushedTimes
        long lastBatchFlushTimestamp

        TestFlushable(long interval)
        {
            this(interval, 1)
        }

        TestFlushable(long interval, int flushAt)
        {
            this.interval = interval
            this.flushAt = flushAt
        }

        @Override
        boolean batchFlush()
        {
            if ((++flushedTimes % flushAt) == 0)
            {
                lastBatchFlushTimestamp = System.currentTimeMillis()
                return true
            } else
            {
                return false
            }
        }

        @Override
        long flushIntervalMillis()
        {
            return interval
        }

        @Override
        long lastBatchFlushTimestamp()
        {
            return lastBatchFlushTimestamp
        }

        @Override
        boolean isClosed()
        {
            return closed
        }

        void close()
        {
            closed = true
        }
    }
}
