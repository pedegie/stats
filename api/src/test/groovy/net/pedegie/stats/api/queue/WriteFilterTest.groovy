package net.pedegie.stats.api.queue


import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentLinkedQueue

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
                    .disableCompression(true)
                    .build()

            WriteFilter writeFilter = WriteFilter.acceptWhenSizeHigherThan(3)

            StatsQueue<Integer> queue = StatsQueue.<Integer> builder()
                    .queue(new ConcurrentLinkedQueue<Integer>())
                    .queueConfiguration(queueConfiguration)
                    .writeFilter(writeFilter)
                    .build()
        when: "we add 4 elements"
            queue.add(1)
            queue.add(1)
            queue.add(1)
            queue.add(1)
            queue.close()
        then: "there should be only last element in file"
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            Files.readAllBytes(logFile).length == DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM
    }

    def "default write filter should be taken into account if none configured - which accepts all"()
    {
        given: "write filter which writes only when queue size is larger than 3"
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when: "we add 4 elements"
            queue.add(1)
            queue.add(1)
            queue.close()
        then: "there should be only last element in file"
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            Files.readAllBytes(logFile).length == DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM * 2
    }
}
