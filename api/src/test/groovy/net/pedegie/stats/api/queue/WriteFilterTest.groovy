package net.pedegie.stats.api.queue

import net.openhft.chronicle.core.OS
import net.pedegie.stats.api.queue.probe.DefaultProbeWriter
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path

class WriteFilterTest extends Specification
{
    def setup()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def cleanup()
    {
        StatsQueue.shutdown()
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
                    .disableCompression(true)
                    .writeFilter(WriteFilter.acceptWhenSizeHigherThan(3))
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when: "we add 4 elements"
            queue.add(1)
            queue.add(1)
            queue.add(1)
            queue.add(1)
            queue.closeBlocking()
        then: "there should be only last element in file"
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            Files.readAllBytes(logFile).length == DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM
    }

    def "default write filter should be taken into account if none configured - which accepts all"()
    {
        given: "write filter which accept all (default)"
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .disableCompression(true)
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when: "we add 4 elements"
            queue.add(1)
            queue.add(1)
            queue.add(1)
            queue.add(1)
            queue.closeBlocking()
        then: "there should be only last element in file"
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            Files.readAllBytes(logFile).length == DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM * 4
    }
}
