package net.pedegie.stats.api.queue

import spock.lang.Specification

import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.ConcurrentLinkedQueue

class QueueCreationTest extends Specification
{
    private static final Path QUEUE_PATH = Paths.get("tmp", "stats_queue.log")

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

    private static void createQueue(LogFileConfiguration logFileConfiguration)
    {
        MPMCQueueStats.<Integer> builder()
                .queue(new ConcurrentLinkedQueue<>())
                .logFileConfiguration(logFileConfiguration)
                .build()
    }
}
