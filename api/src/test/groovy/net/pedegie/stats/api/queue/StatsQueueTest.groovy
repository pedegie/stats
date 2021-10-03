package net.pedegie.stats.api.queue

import net.openhft.chronicle.core.OS
import spock.lang.Specification

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentLinkedQueue

class StatsQueueTest extends Specification
{
    private StatsQueue<Integer> statsQueue

    def setup()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
        statsQueue = createQueue(new ConcurrentLinkedQueue<Integer>())
    }

    def cleanup()
    {
        StatsQueue.shutdown()
    }

    def cleanupSpec()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def "should correctly add element"()
    {
        given:
            int element1 = 25
            int element2 = 10
        when:
            statsQueue.add(element1)
            statsQueue.add(element2)
        then:
            statsQueue.contains(element1)
            statsQueue.contains(element2)
            statsQueue.size() == 2
    }

    def "should throw an error on add if queue is full"()
    {
        given:
            FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
            statsQueue = createQueue(new ArrayBlockingQueue<Integer>(1))
        when:
            statsQueue.add(1)
            statsQueue.add(2)
        then:
            thrown(IllegalStateException)
    }

    def "should correctly addAll elements"()
    {
        given:
            int element1 = 25
            int element2 = 10
        when:
            statsQueue.addAll([element1, element2])
        then:
            statsQueue.contains(element1)
            statsQueue.contains(element2)
            statsQueue.size() == 2
    }

    def "should correctly return size"()
    {
        given:
            Runnable putElements = createPutElementsAction(2000)
            List<Thread> threads = new ArrayList<>()
            (1..5).each { threads.add(new Thread(putElements)) }
        when:
            threads.forEach { it.start() }
            threads.forEach { it.join() }
        then:
            statsQueue.size() == 10000
    }

    def "should correctly return isEmpty"()
    {
        expect:
            statsQueue.isEmpty()
        when:
            statsQueue.add(5)
        then:
            !statsQueue.isEmpty()
        when:
            statsQueue.clear()
        then:
            statsQueue.isEmpty()
    }

    def "should correctly remove elements"()
    {
        given:
            statsQueue.addAll([1, 2, 5])
        when:
            statsQueue.remove(1)
            statsQueue.remove(5)
        then:
            statsQueue.contains(2)
            statsQueue.size() == 1
        when:
            statsQueue.clear()
            statsQueue.remove()
        then:
            thrown(NoSuchElementException)
    }

    def "should correctly poll elements"()
    {
        given:
            statsQueue.addAll([1, 2, 5])
        when:
            statsQueue.poll()
            statsQueue.poll()
        then:
            statsQueue.contains(5)
            statsQueue.size() == 1
        when:
            statsQueue.clear()
            Integer element = statsQueue.poll()
        then:
            element == null
    }

    def "should correctly offer elements"()
    {
        given:
            FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
            statsQueue = createQueue(new ArrayBlockingQueue<Integer>(1))
        when:
            boolean firstElementPut = statsQueue.offer(1)
            boolean secondElementPut = statsQueue.offer(2)
        then:
            firstElementPut
            !secondElementPut
    }

    def "should correctly clear queue"()
    {
        given:
            statsQueue.addAll([1, 2, 5])
        when:
            statsQueue.clear()
        then:
            statsQueue.isEmpty()
            statsQueue.size() == 0
    }

    def "should correctly retainAll"()
    {
        given:
            List<Integer> collection = [1, 2, 5]
            statsQueue.addAll([1, 2, 6, 4, 3])
        when:
            statsQueue.retainAll(collection)
        then:
            statsQueue.containsAll([1, 2])
            statsQueue.size() == 2
    }

    def "should correctly removeAll"()
    {
        given:
            List<Integer> collection = [1, 2, 5]
            statsQueue.addAll([1, 2, 6, 4, 3])
        when:
            statsQueue.removeAll(collection)
        then:
            statsQueue.containsAll([6, 4, 3])
            statsQueue.size() == 3
    }

    private Runnable createPutElementsAction(int elements)
    {
        return new Runnable() {
            @Override
            void run()
            {
                for (int i = 0; i < elements; i++)
                {
                    if (i % 100 == 0)
                    {
                        sleep(4)
                    }
                    statsQueue.add(i)
                }
            }
        }
    }

    private static StatsQueue<Integer> createQueue(Queue<Integer> queue)
    {
        QueueConfiguration queueConfiguration = QueueConfiguration
                .builder()
                .path(TestQueueUtil.PATH)
                .mmapSize(OS.pageSize())
                .build()

        return StatsQueue.<Integer> builder()
                .queue(queue)
                .queueConfiguration(queueConfiguration)
                .build()
    }
}
