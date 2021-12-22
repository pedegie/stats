package io.github.pedegie.stats.api.queue


import net.openhft.chronicle.core.OS
import spock.lang.Specification

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit

class StatsBlockingQueueTest extends Specification
{
    private StatsBlockingQueue<Integer> statsQueue

    def setup()
    {
        StatsQueue.stopFlusher()
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
        statsQueue = createQueue(new ArrayBlockingQueue<Integer>(2))
    }

    def cleanup()
    {
        statsQueue.close()
    }

    def cleanupSpec()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def "should correctly put elements"()
    {
        setup:
            statsQueue.put(5)
            statsQueue.put(5)
        expect:
            statsQueue.size() == 2
    }

    def "should correctly offer elements"()
    {
        setup:
            boolean firstOffer = statsQueue.offer(1, 1, TimeUnit.MILLISECONDS)
            boolean secondOffer = statsQueue.offer(1, 1, TimeUnit.MILLISECONDS)
            boolean thirdOffer = statsQueue.offer(1, 1, TimeUnit.MILLISECONDS)
        expect:
            firstOffer && secondOffer && !thirdOffer
    }

    def "should correctly take elements"()
    {
        setup:
            statsQueue.put(1)
        expect:
            statsQueue.take() != null
    }

    def "should correctly poll elements"()
    {
        setup:
            statsQueue.put(1)
        expect:
            statsQueue.poll(1, TimeUnit.MILLISECONDS) != null
            statsQueue.poll(1, TimeUnit.MILLISECONDS) == null
    }

    def "should correctly return remainingCapacity"()
    {
        setup:
            statsQueue.add(1)
        expect:
            statsQueue.remainingCapacity() == 1
    }

    def "should correctly drainTo elements"()
    {
        given:
            List<Integer> drainTo = new ArrayList<>()
            statsQueue.add(1)
            statsQueue.add(1)
        when:
            statsQueue.drainTo(drainTo)
        then:
            drainTo.size() == 2
    }

    def "should correctly drainTo with MAX elements"()
    {
        given:
            List<Integer> drainTo = new ArrayList<>()
            statsQueue.add(1)
            statsQueue.add(1)
        when:
            statsQueue.drainTo(drainTo, 1)
        then:
            drainTo.size() == 1
    }

    private static StatsBlockingQueue<Integer> createQueue(BlockingQueue<Integer> queue)
    {
        QueueConfiguration queueConfiguration = QueueConfiguration
                .builder()
                .path(TestQueueUtil.PATH)
                .mmapSize(OS.pageSize())
                .build()

        return StatsQueue.blockingQueue(queue, queueConfiguration)
    }
}
