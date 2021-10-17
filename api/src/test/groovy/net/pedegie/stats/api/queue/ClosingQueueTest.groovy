package net.pedegie.stats.api.queue

import net.openhft.chronicle.core.OS
import net.pedegie.stats.api.tailer.ProbeTailer
import spock.lang.Specification

class ClosingQueueTest extends Specification
{
    def setup()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def cleanup()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def "close should be idempotent"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when:
            queue.close()
            queue.close()
            queue.close()
        then:
            noExceptionThrown()
    }

    def "after closing file, it should accept only messages sent to original queue, discarding writes to file"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .flushThreshold(FlushThreshold.flushOnEachWrite())
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when:
            queue.add(5)
            queue.close()
            queue.add(5)
            queue.add(5)
            queue.add(5)
        then: "decorated queue contains all elements, but StatsQueue only 1 (+1 during close flush)"
            queue.size() == 4
            ProbeTailer tailer = ProbeTailer.from(queueConfiguration.withTailer(new TestTailer()))
            tailer.probes() == 2
            tailer.close()
    }

    def "should flush state on close queue"()
    {
        given: "queue with delay 5 seconds between writes"
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .flushThreshold(FlushThreshold.of(5000, 2))
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when: "we put 4 elements, one by one"
            queue.add(5)
            queue.add(5)
            queue.add(5)
            queue.add(5)
        and:
            queue.close()
        then: "there are 2 elements, first added on first add because of millis condition met and second element added during close"
            TestTailer testTailer = new TestTailer()
            ProbeTailer tailer = ProbeTailer.from(queueConfiguration.withTailer(testTailer))
            tailer.probes() == 2
            tailer.readProbes()
            testTailer.probes.get(1).count == 4
            tailer.close()
    }
}
