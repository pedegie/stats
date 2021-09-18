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
            LogFileConfiguration logFileConfiguration = LogFileConfiguration.builder()
                    .path(nestedFilePath)
                    .build()
            FileUtils.cleanDirectory(nestedFilePath.getParent())
        when:
            MPMCQueueStats queue = TestQueueUtil.createQueue(logFileConfiguration)
        then:
            Files.newDirectoryStream(nestedFilePath.getParent())
                    .find { it.toAbsolutePath().toString().contains("stats_queue") } != null
        cleanup:
            queue.close()
            FileUtils.cleanDirectory(nestedFilePath.getParent())
    }

    def "should throw an exception if mmap size is less than page size"()
    {
        given:
            LogFileConfiguration logFileConfiguration = LogFileConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize() - 1)
                    .build()
        when:
            TestQueueUtil.createQueue(logFileConfiguration)
        then:
            thrown(IllegalArgumentException)
    }

    def "should throw an exception if file cycle duration is less than 1 minute"()
    {
        given:
            LogFileConfiguration logFileConfiguration = LogFileConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .fileCycleDuration(Duration.of(59, ChronoUnit.SECONDS))
                    .build()
        when:
            TestQueueUtil.createQueue(logFileConfiguration)
        then:
            thrown(IllegalArgumentException)
    }

    def "should correctly find first free index in byte buffer"()
    {
        expect:
            FileUtils.findFirstFreeIndex(buffer, 4) == expectedFirstFreeIndex
        where:
            buffer                                               | expectedFirstFreeIndex
            wrap([0, 0, 0, 0, 0, 0, 0, 0, 0, 0] as byte[])       | 0
            wrap([1, 0, 0, 0, 0, 0, 0, 0, 0, 0] as byte[])       | 4
            wrap([0, 0, 0, 1, 0, 0, 0, 0, 0, 0] as byte[])       | 4
            wrap([0, 0, 0, 1, 1, 0, 0, 0, 0, 0] as byte[])       | 8
            wrap([0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0] as byte[]) | 8
            wrap([0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0] as byte[]) | 12 // full
            wrap([0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1] as byte[]) | 12 // full
    }

    private static ByteBuffer wrap(byte[] bytes)
    {
        return ByteBuffer.wrap(bytes)
    }
}
