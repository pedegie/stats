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
            LogFileConfiguration logFileConfiguration = LogFileConfiguration.builder()
                    .mmapSize(OS.pageSize())
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .build()

            MPMCQueueStats<Integer> queue = TestQueueUtil.createQueue(logFileConfiguration)
            int[] elementsToFillWholeBuffer = allocate(logFileConfiguration)

        when:
            elementsToFillWholeBuffer.each { queue.add(it) }
            queue.add(1)
            queue.close()
        then:
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            ByteBuffer probes = ByteBuffer.wrap(Files.readAllBytes(logFile))
            probes.getInt(probes.limit() - DefaultFileAccess.PROBE_AND_TIMESTAMP_BYTES_SUM) == elementsToFillWholeBuffer.length + 1
    }

    def "should be able to append to log file when whole mmaped memory is dirty"()
    {
        given:
            LogFileConfiguration logFileConfiguration = LogFileConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .disableCompression(true)
                    .build()

            MPMCQueueStats<Integer> queue = TestQueueUtil.createQueue(logFileConfiguration)

            def range = 1..(FileUtils.PAGE_SIZE / DefaultFileAccess.PROBE_AND_TIMESTAMP_BYTES_SUM)
            range.forEach {
                queue.add(it.intValue())
            }
            queue.close()
        when:
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            ByteBuffer probes = ByteBuffer.wrap(Files.readAllBytes(logFile))
        then: "there are 2 probes"
            probes.limit() == DefaultFileAccess.PROBE_AND_TIMESTAMP_BYTES_SUM * range.size()
        when:
            queue = TestQueueUtil.createQueue(logFileConfiguration)
            queue.add(5)
            queue.close()
            probes = ByteBuffer.wrap(Files.readAllBytes(logFile))
        then: "there are 3 probes"
            probes.limit() == DefaultFileAccess.PROBE_AND_TIMESTAMP_BYTES_SUM * (range.size() + 1)
    }

    // it takes too long, run with integration tests nightly CI build
    @Ignore
    def "should be able to mmap more memory than 2GB"()
    {
        given:
            LogFileConfiguration logFileConfiguration = LogFileConfiguration.builder()
                    .mmapSize(Integer.MAX_VALUE)
                    .disableCompression(true)
                    .path(TestQueueUtil.PATH)
                    .build()

            MPMCQueueStats queue = TestQueueUtil.createQueue(logFileConfiguration)

            int[] elementsToFillWholeBuffer = allocate(logFileConfiguration)
            for (int i = 0; i < elementsToFillWholeBuffer.length; i++)
            {
                queue.add(elementsToFillWholeBuffer[i])
            }

        when:
            queue.add(1)
            queue.close()
        then:
            noExceptionThrown()
    }

    private static int[] allocate(LogFileConfiguration logFileConfiguration)
    {
        int elements = (int) (logFileConfiguration.mmapSize / DefaultFileAccess.PROBE_AND_TIMESTAMP_BYTES_SUM)
        return new int[elements]
    }
}
