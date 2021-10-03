package net.pedegie.stats.api.queue

import net.openhft.chronicle.core.OS
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.time.temporal.ChronoUnit

class QueueCreationTest extends Specification
{
    def setup()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def cleanupSpec()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def "should create nested directories if path not exists"()
    {
        given:
            Path nestedFilePath = Paths.get(System.getProperty("java.io.tmpdir"), "dir1", "dir2", "stats_queue.log")
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .mmapSize(OS.pageSize())
                    .path(nestedFilePath)
                    .build()
            FileUtils.cleanDirectory(nestedFilePath.getParent())
        when:
            StatsQueue queue = TestQueueUtil.createQueue(queueConfiguration)
        then:
            Files.newDirectoryStream(nestedFilePath.getParent())
                    .find { it.toAbsolutePath().toString().contains("stats_queue") } != null
        cleanup:
            queue.closeBlocking()
            FileUtils.cleanDirectory(nestedFilePath.getParent())
    }

    def "should throw an exception if mmap size is less than page size"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize() - 1)
                    .build()
        when:
            TestQueueUtil.createQueue(queueConfiguration)
        then:
            thrown(IllegalArgumentException)
    }

    def "should throw an exception if file cycle duration is less than 1 minute"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .fileCycleDuration(Duration.of(59, ChronoUnit.SECONDS))
                    .build()
        when:
            TestQueueUtil.createQueue(queueConfiguration)
        then:
            thrown(IllegalArgumentException)
    }

    def "should correctly find first free index in byte buffer"()
    {
        expect:
            FileUtils.findFirstFreeIndex(buffer) == expectedFirstFreeIndex
        where:
            buffer                                                        | expectedFirstFreeIndex
            wrap([0, 0, 0, 0, 0, 0, 0, 0, 0, 0] as byte[])                | 0
            wrap([1, 0, 0, 0, 0, 0, 0, 0, 0, 0] as byte[])                | 4
            wrap([0, 0, 0, 1, 0, 0, 0, 0, 0, 0] as byte[])                | 4
            wrap([0, 0, 0, 1, 1, 0, 0, 0, 0, 0] as byte[])                | 8
            wrap([0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0] as byte[])          | 8
            wrap([0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0] as byte[])          | 12 // full
            wrap([0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1] as byte[])          | 12 // full
    }

    def "should correctly find first free index in specific corner case when window of 0's is the widest possible between two int probes (62 bits)"()
    {
        given: "specific value which divided by 2 during binary search gives index equal of 192 which starts 62 bits 0's window"
            // we are working on: 1000000000000000000000010110111101101000101100111010000000000000 00000000000000000000000000000001 10000000000000000000000000000000 00000000000000000000000000000001 10000000000000000000000000000000 00000000000000000000000000000000 000000000000000000000000
            long header = Long.parseLong("-100000000000000000000010110111101101000101100111010000000000000", 2)
            int firstProbe = Integer.parseInt("00000000000000000000000000000001", 2)
            int secondProbe = Integer.parseInt("-1000000000000000000000000000000", 2)
            int thirdProbe = Integer.parseInt("00000000000000000000000000000001", 2)
            int fourthProbe = Integer.parseInt("-1000000000000000000000000000000", 2)
            int padding = Integer.parseInt("00000000000000000000000000000000", 2)
            short padding2 = Short.parseShort("0000000000000000", 2)
            byte padding3 = Byte.parseByte("00000000", 2)

        when:
            ByteBuffer buffer = ByteBuffer.allocate(31)
            buffer.putLong(header)
            buffer.putInt(firstProbe)
            buffer.putInt(secondProbe)
            buffer.putInt(thirdProbe)
            buffer.putInt(fourthProbe)
            buffer.putInt(padding)
            buffer.putShort(padding2)
            buffer.put(padding3)
        then:
            FileUtils.findFirstFreeIndex(buffer) == 24

    }

    def "should differentiate probe which's value is 0 from free/sparse space during appending to already existing file"()
    {
        given: "queue"
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .disableCompression(compressionDisabled)
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when: "we put first element and then remove from queue"
            queue.add(5)
            queue.remove()
            queue.closeBlocking()
        then: "there should be 2 elements"
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            ByteBuffer logFileBuffer = ByteBuffer.wrap(Files.readAllBytes(logFile))
            logFileBuffer.limit() == expectedBytesAfterWritingTwoElements
        and: "second probe value instead of 0 has first bit set indicating its zero"
            logFileBuffer.getInt(secondProbeValueIndex) == Integer.MIN_VALUE
        when: "we create queue again and put there one element"
            queue = TestQueueUtil.createQueue(queueConfiguration)
            queue.add(5)
            queue.closeBlocking()
        then: "it should have 3 elements"
            Path logFile2 = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            ByteBuffer logFileBuffer2 = ByteBuffer.wrap(Files.readAllBytes(logFile2))
            logFileBuffer2.limit() == expectedBytesAfterWritingThreelements
        where:
            compressionDisabled << [true, false]
            expectedBytesAfterWritingTwoElements << [24, 24]
            expectedBytesAfterWritingThreelements << [36, 32]
            secondProbeValueIndex << [12, 16]

    }

    private static ByteBuffer wrap(byte[] bytes)
    {
        return ByteBuffer.wrap(bytes)
    }
}
