package net.pedegie.stats.api.queue

import net.pedegie.stats.api.queue.fileaccess.FileUtils
import spock.lang.Specification

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentLinkedQueue

class MPMCQueueStatsTest extends Specification
{
    private MPMCQueueStats<Integer> mpmcQueueStats

    def setup()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
        mpmcQueueStats = createQueue(new ConcurrentLinkedQueue<Integer>())
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
            mpmcQueueStats.add(element1)
            mpmcQueueStats.add(element2)
        then:
            mpmcQueueStats.contains(element1)
            mpmcQueueStats.contains(element2)
            mpmcQueueStats.size() == 2
    }

    def "should throw an error on add if queue is full"()
    {
        given:
            mpmcQueueStats = createQueue(new ArrayBlockingQueue<Integer>(1))
        when:
            mpmcQueueStats.add(1)
            mpmcQueueStats.add(2)
        then:
            thrown(IllegalStateException)
    }

    def "should correctly addAll elements"()
    {
        given:
            int element1 = 25
            int element2 = 10
        when:
            mpmcQueueStats.addAll([element1, element2])
        then:
            mpmcQueueStats.contains(element1)
            mpmcQueueStats.contains(element2)
            mpmcQueueStats.size() == 2
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
            mpmcQueueStats.size() == 10000
    }

    def "should correctly return isEmpty"()
    {
        expect:
            mpmcQueueStats.isEmpty()
        when:
            mpmcQueueStats.add(5)
        then:
            !mpmcQueueStats.isEmpty()
        when:
            mpmcQueueStats.clear()
        then:
            mpmcQueueStats.isEmpty()
    }

    def "should correctly remove elements"()
    {
        given:
            mpmcQueueStats.addAll([1, 2, 5])
        when:
            mpmcQueueStats.remove(1)
            mpmcQueueStats.remove(5)
        then:
            mpmcQueueStats.contains(2)
            mpmcQueueStats.size() == 1
        when:
            mpmcQueueStats.clear()
            mpmcQueueStats.remove()
        then:
            thrown(NoSuchElementException)
    }

    def "should correctly poll elements"()
    {
        given:
            mpmcQueueStats.addAll([1, 2, 5])
        when:
            mpmcQueueStats.poll()
            mpmcQueueStats.poll()
        then:
            mpmcQueueStats.contains(5)
            mpmcQueueStats.size() == 1
        when:
            mpmcQueueStats.clear()
            Integer element = mpmcQueueStats.poll()
        then:
            element == null
    }

    def "should correctly offer elements"()
    {
        given:
            mpmcQueueStats = createQueue(new ArrayBlockingQueue<Integer>(1))
        when:
            boolean firstElementPut = mpmcQueueStats.offer(1)
            boolean secondElementPut = mpmcQueueStats.offer(2)
        then:
            firstElementPut
            !secondElementPut
    }

    def "should correctly clear queue"()
    {
        given:
            mpmcQueueStats.addAll([1, 2, 5])
        when:
            mpmcQueueStats.clear()
        then:
            mpmcQueueStats.isEmpty()
            mpmcQueueStats.size() == 0
    }

    def "should correctly retainAll"()
    {
        given:
            List<Integer> collection = [1, 2, 5]
            mpmcQueueStats.addAll([1, 2, 6, 4, 3])
        when:
            mpmcQueueStats.retainAll(collection)
        then:
            mpmcQueueStats.containsAll([1, 2])
            mpmcQueueStats.size() == 2
    }

    def "should correctly removeAll"()
    {
        given:
            List<Integer> collection = [1, 2, 5]
            mpmcQueueStats.addAll([1, 2, 6, 4, 3])
        when:
            mpmcQueueStats.removeAll(collection)
        then:
            mpmcQueueStats.containsAll([6, 4, 3])
            mpmcQueueStats.size() == 3
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
                    mpmcQueueStats.add(i)
                }
            }
        }
    }

    private static MPMCQueueStats<Integer> createQueue(Queue<Integer> queue)
    {
        LogFileConfiguration logFileConfiguration = LogFileConfiguration
                .builder()
                .path(TestQueueUtil.PATH)
                .build()

        return MPMCQueueStats.<Integer> builder()
                .queue(queue)
                .logFileConfiguration(logFileConfiguration)
                .build()
    }
}
