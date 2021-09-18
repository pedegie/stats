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
            LogFileConfiguration logFileConfiguration = LogFileConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .build()

            WriteFilter writeFilter = WriteFilter.acceptWhenSizeHigherThan(3)

            MPMCQueueStats<Integer> queue = MPMCQueueStats.<Integer> builder()
                    .queue(new ConcurrentLinkedQueue<Integer>())
                    .logFileConfiguration(logFileConfiguration)
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
            LogFileConfiguration logFileConfiguration = LogFileConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .build()
            MPMCQueueStats<Integer> queue = TestQueueUtil.createQueue(logFileConfiguration)
        when: "we add 4 elements"
            queue.add(1)
            queue.add(1)
            queue.close()
        then: "there should be only last element in file"
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            Files.readAllBytes(logFile).length == DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM * 2
    }
}
