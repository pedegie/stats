package net.pedegie.stats.api.queue

import net.openhft.chronicle.core.OS
import net.pedegie.stats.api.tailer.ProbeTailer
import net.pedegie.stats.api.tailer.TailerFactory
import spock.lang.Specification

class WriteFilterTest extends Specification
{
    def setup()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def cleanupSpec()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def "acceptWhenSizeHigherThan filter should accept writes only when queue size higher than 3"()
    {
        given: "write filter which writes only when queue size is larger than 3"
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .batchSize(1)
                    .disableCompression(true)
                    .writeFilter(WriteFilter.acceptWhenSizeHigherThan(3))
                    .writeThreshold(WriteThreshold.flushOnEachWrite())
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when: "we add 4 elements"
            queue.add(1)
            queue.add(1)
            queue.add(1)
            queue.add(1)
            queue.close()
        then: "there should be only last element in file (+1 during close flush)"
            ProbeTailer tailer = TailerFactory.tailerFor(TestQueueUtil.PATH)
            tailer.probes() == 2
            tailer.close()
    }

    def "default write filter should be taken into account if none configured - which accepts all"()
    {
        given: "write filter which accept all (default)"
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .disableCompression(true)
                    .batchSize(1)
                    .writeThreshold(WriteThreshold.flushOnEachWrite())
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when: "we add 4 elements"
            queue.add(1)
            queue.add(1)
            queue.add(1)
            queue.add(1)
            queue.close()
        then: "there should 4 elements (+1 during close flush)"
            ProbeTailer tailer = TailerFactory.tailerFor(TestQueueUtil.PATH)
            tailer.probes() == 5
            tailer.close()
    }
}
