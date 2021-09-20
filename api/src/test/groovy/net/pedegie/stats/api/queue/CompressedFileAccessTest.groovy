package net.pedegie.stats.api.queue

import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

class CompressedFileAccessTest extends Specification
{
    def setup()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def cleanupSpec()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def "should enable compression when cycle duration is less than Integer.MAX_VALUE millis"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .fileCycleDuration(Duration.of(1, ChronoUnit.HOURS))
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when:
            queue.add(5)
            queue.add(5)
            queue.add(5)
            queue.close()
        then:
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            Files.readAllBytes(logFile).length == 8 + CompressedProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM * 3
    }

    def "should disable compression when cycle duration is more than Integer.MAX_VALUE millis"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .fileCycleDuration(Duration.of(30, ChronoUnit.DAYS))
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when:
            queue.add(5)
            queue.add(5)
            queue.add(5)
            queue.close()
        then:
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            Files.readAllBytes(logFile).length == DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM * 3
    }

    def "should disable compression when disable compression flag is set"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .fileCycleDuration(Duration.of(1, ChronoUnit.HOURS))
                    .disableCompression(true)
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when:
            queue.add(5)
            queue.add(5)
            queue.add(5)
            queue.close()
        then:
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            Files.readAllBytes(logFile).length == DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM * 3
    }

    def "should disable compression if recycle window matches already existing file which is not compressed file"()
    {
        given: "create queue with disabled compression"
            ZonedDateTime now = ZonedDateTime.of(LocalDateTime.parse("2020-01-03T15:00:00"), ZoneId.of("UTC"))
            QueueConfiguration queueConfiguration = QueueConfiguration
                    .builder()
                    .path(TestQueueUtil.PATH)
                    .fileCycleDuration(Duration.of(1, ChronoUnit.HOURS))
                    .fileCycleClock(Clock.fixed(now.toInstant(), ZoneId.of("UTC")))
                    .disableCompression(true)
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when: "we put 3 elements"
            queue.add(5)
            queue.add(5)
            queue.add(5)
            queue.close()
        then: "there are stored 3 non-compressed elements"
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            Files.readAllBytes(logFile).length == DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM * 3
        when: "we create compressed queue which cycles matches to already existing one, created in previous step"
            queueConfiguration = QueueConfiguration
                    .builder()
                    .path(TestQueueUtil.PATH)
                    .fileCycleDuration(Duration.of(1, ChronoUnit.HOURS))
                    .fileCycleClock(Clock.fixed(now.toInstant(), ZoneId.of("UTC")))
                    .build()
            queue = TestQueueUtil.createQueue(queueConfiguration)
            queue.add(3)
            queue.add(3)
            queue.add(3)
            queue.close()
        then: "compression were disabled anyway"
            Path logFile2 = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            Files.readAllBytes(logFile2).length == DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM * 6
    }

    def "should be able to append to already existing compressed file"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .fileCycleDuration(Duration.of(1, ChronoUnit.HOURS))
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when:
            queue.add(5)
            queue.add(5)
            queue.add(5)
            queue.close()
        then:
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            Files.readAllBytes(logFile).length == 8 + CompressedProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM * 3
        when:
            queue = TestQueueUtil.createQueue(queueConfiguration)
            queue.add(5)
            queue.close()
        then:
            Path logFile2 = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            Files.readAllBytes(logFile2).length == 8 + CompressedProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM * 4
    }
}
