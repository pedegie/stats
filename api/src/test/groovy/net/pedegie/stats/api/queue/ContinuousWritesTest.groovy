package net.pedegie.stats.api.queue

import net.openhft.chronicle.core.OS
import spock.lang.Ignore
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path

class ContinuousWritesTest extends Specification
{
    def setup()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def cleanupSpec()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def "should continuously allocate mmap files"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .mmapSize(OS.pageSize())
                    .path(TestQueueUtil.PATH)
                    .disableCompression(disableCompression)
                    .build()

            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            int[] elementsToFillWholeBuffer = allocate(queueConfiguration, probeSize)

        when: "we put one element more than mmap size"
            elementsToFillWholeBuffer.each { queue.add(it) }
            queue.add(5)
        and: "we put one more element due to dropped previous one on resize"
            queue.add(5)
            queue.close()
        then:
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            ByteBuffer probes = ByteBuffer.wrap(Files.readAllBytes(logFile))
            probes.getInt(probes.limit() - probeSize) == elementsToFillWholeBuffer.length + 2
        where:
            disableCompression << [true, false]
            probeSize << [DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM, CompressedProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM]
    }

    def "should be able to append to log file when whole mmaped memory is dirty"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .disableCompression(disableCompression)
                    .build()

            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when: "we put elements equal to mmaped size"
            def range = 1..((OS.pageSize() / probeSize) - (headerSize / probeSize))
            range.forEach {
                queue.add(it.intValue())
            }
            queue.close()
        then:
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            ByteBuffer probes = ByteBuffer.wrap(Files.readAllBytes(logFile))
            probes.limit() == probeSize * range.size() + headerSize
        when: "we put two more probes, because of one is dropped during resize"
            queue = TestQueueUtil.createQueue(queueConfiguration)
            queue.add(5)
            queue.add(5)
            queue.close()
            probes = ByteBuffer.wrap(Files.readAllBytes(logFile))
        then: "there is additional probe"
            probes.limit() == probeSize * (range.size() + 1) + headerSize
        where:
            disableCompression << [true, false]
            probeSize << [DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM, CompressedProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM]
            headerSize << [0, CompressedProbeWriter.HEADER_SIZE]
    }

    // it takes too long, run with integration tests nightly CI build
    @Ignore
    def "should be able to mmap more memory than 2GB"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .mmapSize(Integer.MAX_VALUE)
                    .disableCompression(disableCompression)
                    .path(TestQueueUtil.PATH)
                    .build()

            StatsQueue queue = TestQueueUtil.createQueue(queueConfiguration)

            int[] elementsToFillWholeBuffer = allocate(queueConfiguration, probeSize)
        when:
            for (int i = 0; i < elementsToFillWholeBuffer.length; i++)
            {
                queue.add(elementsToFillWholeBuffer[i])
            }
            queue.add(1)
            queue.add(1)
            queue.close()
        then:
            noExceptionThrown()
        where:
            disableCompression << [true, false]
            probeSize << [DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM, CompressedProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM]
    }

    private static int[] allocate(QueueConfiguration queueConfiguration, int probeSize)
    {
        int elements = (int) (queueConfiguration.mmapSize / probeSize)
        return new int[elements]
    }
}
