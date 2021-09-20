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
                    .disableCompression(true)
                    .build()

            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            int[] elementsToFillWholeBuffer = allocate(queueConfiguration)

        when: "we put one element more than mmap size"
            elementsToFillWholeBuffer.each { queue.add(it) }
            queue.add(5)
        and: "we put one more element due to dropped previous one on resize"
            queue.add(5)
            queue.close()
        then:
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            ByteBuffer probes = ByteBuffer.wrap(Files.readAllBytes(logFile))
            probes.getInt(probes.limit() - DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM) == elementsToFillWholeBuffer.length + 2
    }

    def "should be able to append to log file when whole mmaped memory is dirty"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .disableCompression(true)
                    .build()

            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)

            def range = 1..(OS.pageSize() / DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM)
            range.forEach {
                queue.add(it.intValue())
            }
            queue.close()
        when:
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            ByteBuffer probes = ByteBuffer.wrap(Files.readAllBytes(logFile))
        then: "there are 2 probes"
            probes.limit() == DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM * range.size()
        when: "we put two more probes, because of one is dropped during resize"
            queue = TestQueueUtil.createQueue(queueConfiguration)
            queue.add(5)
            queue.add(5)
            queue.close()
            probes = ByteBuffer.wrap(Files.readAllBytes(logFile))
        then: "there are 3 probes"
            probes.limit() == DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM * (range.size() + 1)
    }

    // it takes too long, run with integration tests nightly CI build
    @Ignore
    def "should be able to mmap more memory than 2GB"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .mmapSize(Integer.MAX_VALUE)
                    .disableCompression(true)
                    .path(TestQueueUtil.PATH)
                    .build()

            StatsQueue queue = TestQueueUtil.createQueue(queueConfiguration)

            int[] elementsToFillWholeBuffer = allocate(queueConfiguration)
            for (int i = 0; i < elementsToFillWholeBuffer.length; i++)
            {
                queue.add(elementsToFillWholeBuffer[i])
            }

        when:
            queue.add(1)
            queue.add(1)
            queue.close()
        then:
            noExceptionThrown()
    }

    private static int[] allocate(QueueConfiguration queueConfiguration)
    {
        int elements = (int) (queueConfiguration.mmapSize / DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM)
        return new int[elements]
    }
}
