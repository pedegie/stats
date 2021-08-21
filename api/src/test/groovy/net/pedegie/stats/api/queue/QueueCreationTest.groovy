package net.pedegie.stats.api.queue

import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.ConcurrentLinkedQueue

class QueueCreationTest extends Specification
{
    private static final Path QUEUE_PATH = Paths.get(System.getProperty("java.io.tmpdir"), "stats_queue.log")

    def "should throw an exception on misconfiguration log file creation"()
    {
        when:
            LogFileConfiguration logFileConfiguration = LogFileConfiguration.builder()
                    .path(QUEUE_PATH)
                    .append(true)
                    .override(true)
                    .newFileWithDate(false)
                    .build()
            createQueue(logFileConfiguration)

        then:
            thrown(IllegalArgumentException)
        when:
            logFileConfiguration = LogFileConfiguration.builder()
                    .path(QUEUE_PATH)
                    .append(true)
                    .override(false)
                    .newFileWithDate(true)
                    .build()
            createQueue(logFileConfiguration)
        then:
            thrown(IllegalArgumentException)
        when:
            logFileConfiguration = LogFileConfiguration.builder()
                    .path(QUEUE_PATH)
                    .append(false)
                    .override(true)
                    .newFileWithDate(true)
                    .build()
            createQueue(logFileConfiguration)
        then:
            thrown(IllegalArgumentException)
        when:
            logFileConfiguration = LogFileConfiguration.builder()
                    .path(QUEUE_PATH)
                    .append(true)
                    .override(true)
                    .newFileWithDate(false)
                    .build()
            createQueue(logFileConfiguration)
        then:
            thrown(IllegalArgumentException)
        when:
            logFileConfiguration = LogFileConfiguration.builder()
                    .path(QUEUE_PATH)
                    .append(false)
                    .override(true)
                    .newFileWithDate(true)
                    .build()
            createQueue(logFileConfiguration)
        then:
            thrown(IllegalArgumentException)
        when:
            logFileConfiguration = LogFileConfiguration.builder()
                    .path(QUEUE_PATH)
                    .append(true)
                    .override(false)
                    .newFileWithDate(true)
                    .build()
            createQueue(logFileConfiguration)
        then:
            thrown(IllegalArgumentException)
        when:
            logFileConfiguration = LogFileConfiguration.builder()
                    .path(null)
                    .append(true)
                    .override(false)
                    .newFileWithDate(false)
                    .build()
            createQueue(logFileConfiguration)
        then:
            thrown(NullPointerException)
        when:
            createQueue(null)
        then:
            thrown(NullPointerException)
    }

    def "should create nested directories if path not exists"()
    {
        given:
            Path nestedFilePath = Paths.get(System.getProperty("java.io.tmpdir"), "dir1", "dir2", "stats_queue.log")
            LogFileConfiguration logFileConfiguration = LogFileConfiguration.builder()
                    .override(true)
                    .path(nestedFilePath)
                    .build()
        when:
            MPMCQueueStats queue = MPMCQueueStats.<Integer> builder()
                    .queue(new ConcurrentLinkedQueue<Integer>())
                    .logFileConfiguration(logFileConfiguration)
                    .build()
        then:
            Files.exists(nestedFilePath)
        cleanup:
            queue.close()
            Files.deleteIfExists(nestedFilePath)
    }

    private static void createQueue(LogFileConfiguration logFileConfiguration)
    {
        MPMCQueueStats.<Integer> builder()
                .queue(new ConcurrentLinkedQueue<>())
                .logFileConfiguration(logFileConfiguration)
                .build()
    }
}
