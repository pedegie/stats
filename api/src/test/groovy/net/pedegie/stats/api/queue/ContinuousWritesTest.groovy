package net.pedegie.stats.api.queue

import net.pedegie.stats.api.queue.fileaccess.FileAccess
import spock.lang.Ignore
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.ConcurrentLinkedQueue

class ContinuousWritesTest extends Specification
{
    private static final Path testQueuePath = Paths.get(System.getProperty("java.io.tmpdir").toString(), "stats_queue").toAbsolutePath()

    def setup()
    {
        Files.deleteIfExists(testQueuePath)
    }

    def cleanupSpec()
    {
        Files.deleteIfExists(testQueuePath)
    }

    def "should continuously allocate mmap files"()
    {
        given:
            LogFileConfiguration logFileConfiguration = LogFileConfiguration.builder()
                    .override(true)
                    .mmapSize(50)
                    .path(testQueuePath)
                    .build()

            MPMCQueueStats queue = MPMCQueueStats.<Integer> builder()
                    .queue(new ConcurrentLinkedQueue<Integer>())
                    .logFileConfiguration(logFileConfiguration)
                    .build()

            int[] elementsToFillWholeBuffer = allocate(logFileConfiguration)

        when:
            elementsToFillWholeBuffer.each { queue.add(it) }
            queue.add(1)
            queue.close()
        then:
            ByteBuffer probes = ByteBuffer.wrap(Files.readAllBytes(testQueuePath))
            probes.getInt(probes.limit() - FileAccess.PROBE_AND_TIMESTAMP_BYTES_SUM) == elementsToFillWholeBuffer.length + 1
    }

    // it takes too long, run with integration tests nightly CI build
    @Ignore
    def "should be able to mmap more memory than 2GB"()
    {
        given:
            LogFileConfiguration logFileConfiguration = LogFileConfiguration.builder()
                    .override(true)
                    .mmapSize(Integer.MAX_VALUE)
                    .path(testQueuePath)
                    .build()

            MPMCQueueStats queue = MPMCQueueStats.<Integer> builder()
                    .queue(new ConcurrentLinkedQueue<Integer>())
                    .logFileConfiguration(logFileConfiguration)
                    .build()

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
        int elements = (int) (logFileConfiguration.mmapSize / FileAccess.PROBE_AND_TIMESTAMP_BYTES_SUM)
        return new int[elements]
    }
}
