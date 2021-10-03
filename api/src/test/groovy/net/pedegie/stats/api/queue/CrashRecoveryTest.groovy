package net.pedegie.stats.api.queue

import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

class CrashRecoveryTest extends Specification
{
    private static ZonedDateTime time = ZonedDateTime.of(LocalDateTime.parse("2020-01-03T00:00:00"), ZoneId.of("UTC"))

    def setup()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def cleanupSpec()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def "should recover from crash when it happens between non-atomic probe write and timestamp - it should truncate to last correct probe and start from there"()
    {
        given: "Crashing probe writer"
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .probeWriter(crashingProbeWriter)
                    .fileCycleClock(Clock.fixed(time.toInstant(), ZoneId.of("UTC")))
                    .fileCycleDuration(Duration.of(1, ChronoUnit.HOURS))
                    .disableCompression(true)
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when: "we put elements"
            (0..(crashOnElement)).forEach { queue.add(5) }
            queue.closeBlocking()
        then: "there are probes in file but last one is missing timestamp"
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            byte[] bytes = Files.readAllBytes(logFile)
            bytes.length == halfProbeSize
        when: "we create queue one more time, with normal (non-crashing) probe writer"
            queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .probeWriter(probeWriter)
                    .fileCycleDuration(Duration.of(1, ChronoUnit.HOURS))
                    .fileCycleClock(Clock.fixed(time.toInstant(), ZoneId.of("UTC")))
                    .disableCompression(true)
                    .build()
            queue = TestQueueUtil.createQueue(queueConfiguration)
        and: "we put there one element"
            queue.add(5)
            queue.closeBlocking()
        then: "it should remove previous half-written probe and then append new element to queue"
            Path logFile2 = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            ByteBuffer bytes2 = ByteBuffer.wrap(Files.readAllBytes(logFile2))
            bytes2.limit() == expectedBytes
            bytes2.getLong(bytes2.limit() - crashingProbeWriter.probeSize()) != 0
        where:
            halfProbeSize << [4, 12,
                              28, 28]
            expectedBytes << [12, 16,
                              36, 32]
            crashOnElement << [1, 1,
                               3, 3]
            crashingProbeWriter << [new CrashingProbes.DefaultCrashingProbeWriter(1), new CrashingProbes.CompressedCrashingProbeWriter(time, 1),
                                    new CrashingProbes.DefaultCrashingProbeWriter(3), new CrashingProbes.CompressedCrashingProbeWriter(time, 3)]
            probeWriter << [ProbeWriter.defaultProbeWriter(), ProbeWriter.compressedProbeWriter(time),
                            ProbeWriter.defaultProbeWriter(), ProbeWriter.compressedProbeWriter(time)]
    }

    def "should recover from crash when it happens before any writes - it should truncate to beginning of buffer"()
    {
        given: "Crashing probe writer"
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .probeWriter(crashingProbeWriter)
                    .fileCycleClock(Clock.fixed(time.toInstant(), ZoneId.of("UTC")))
                    .fileCycleDuration(Duration.of(1, ChronoUnit.HOURS))
                    .disableCompression(true)
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when: "we try to put first element"
            queue.add(5)
        then: "file is empty (contains only metadata) due crash on first probe write"
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            byte[] bytes = Files.readAllBytes(logFile)
            bytes.length == metadataByes
        when: "we create queue one more time, with normal (non-crashing) probe writer"
            queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .probeWriter(probeWriter)
                    .fileCycleDuration(Duration.of(1, ChronoUnit.HOURS))
                    .fileCycleClock(Clock.fixed(time.toInstant(), ZoneId.of("UTC")))
                    .disableCompression(true)
                    .build()
            queue = TestQueueUtil.createQueue(queueConfiguration)
        and: "we put there one element"
            queue.add(5)
            queue.closeBlocking()
        then: "it should contain one element"
            Path logFile2 = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            ByteBuffer bytes2 = ByteBuffer.wrap(Files.readAllBytes(logFile2))
            bytes2.limit() == expectedBytes
            bytes2.getLong(bytes2.limit() - crashingProbeWriter.probeSize()) != 0
        where:
            metadataByes << [0, 8]
            expectedBytes << [12, 16]
            crashingProbeWriter << [new CrashingProbes.DefaultCrashingProbeWriter(-1), new CrashingProbes.CompressedCrashingProbeWriter(time, -1)]
            probeWriter << [ProbeWriter.defaultProbeWriter(), ProbeWriter.compressedProbeWriter(time)]
    }
}
