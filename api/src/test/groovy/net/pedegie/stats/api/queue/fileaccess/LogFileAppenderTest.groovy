package net.pedegie.stats.api.queue.fileaccess

import net.pedegie.stats.api.queue.LogFileConfiguration
import net.pedegie.stats.api.queue.MPMCQueueStats
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDateTime
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.stream.Collectors

class LogFileAppenderTest extends Specification
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

    def "should override existing file if 'override' parameter is set"()
    {
        given:
            LogFileConfiguration logFileConfiguration = LogFileConfiguration.builder()
                    .path(testQueuePath)
                    .override(true)
                    .build()

            MPMCQueueStats<Integer> queue = MPMCQueueStats.<Integer> builder()
                    .logFileConfiguration(logFileConfiguration)
                    .queue(new ConcurrentLinkedQueue<Integer>())
                    .build()

            queue.add(5)
            queue.close()
        when:
            ByteBuffer probes = ByteBuffer.wrap(Files.readAllBytes(testQueuePath))
        then:
            probes.getInt() == 1
        when:
            queue = MPMCQueueStats.<Integer> builder()
                    .logFileConfiguration(logFileConfiguration)
                    .queue(new ConcurrentLinkedQueue<Integer>())
                    .build()
            queue.close()
            probes = ByteBuffer.wrap(Files.readAllBytes(testQueuePath))
        then:
            probes.limit() == 0
    }

    def "should create new file with current date if 'newFileWithDate' parameter is set"()
    {
        given:
            Path logFile = Paths.get(System.getProperty("java.io.tmpdir").toString(), "queue", "test_queue")
            cleanDirectory(logFile.getParent())
            LogFileConfiguration logFileConfiguration = LogFileConfiguration.builder()
                    .path(logFile)
                    .newFileWithDate(true)
                    .build()

            MPMCQueueStats queue = MPMCQueueStats.<Integer> builder()
                    .logFileConfiguration(logFileConfiguration)
                    .queue(new ConcurrentLinkedQueue<Integer>())
                    .build()
            queue.close()
        when:
            List<Path> logFiles = Files.walk(logFile.getParent())
                    .filter { it.toString().startsWith(logFile.toString()) }
                    .collect(Collectors.toList())
        then:
            logFiles.size() == 1
            Path savedLogFile = logFiles[0]

        when:
            queue = MPMCQueueStats.<Integer> builder()
                    .logFileConfiguration(logFileConfiguration)
                    .queue(new ConcurrentLinkedQueue<Integer>())
                    .build()
            queue.close()
            logFiles = Files.walk(logFile.getParent())
                    .filter { it.toString().startsWith(logFile.toString()) }
                    .sorted()
                    .collect()
        then:
            logFiles.size() == 2
            logFiles[0] == savedLogFile
            LocalDateTime firstLogFileDate = PathDateFormatter.takeDate(logFiles[0])
            LocalDateTime secondLogFileDate = PathDateFormatter.takeDate(logFiles[1])
            firstLogFileDate.isBefore(secondLogFileDate)
        cleanup:
            cleanDirectory(logFile.getParent())
    }

    def "should append logs to file if 'append' parameter is set"()
    {
        given:
            LogFileConfiguration logFileConfiguration = LogFileConfiguration.builder()
                    .path(testQueuePath)
                    .append(true)
                    .build()

            MPMCQueueStats<Integer> queue = MPMCQueueStats.<Integer> builder()
                    .logFileConfiguration(logFileConfiguration)
                    .queue(new ConcurrentLinkedQueue<Integer>())
                    .build()

            queue.add(5)
            queue.close()
        when:
            ByteBuffer probes = ByteBuffer.wrap(Files.readAllBytes(testQueuePath))
        then: "there is one probe"
            probes.getInt(0) == 1
        when:
            queue = MPMCQueueStats.<Integer> builder()
                    .logFileConfiguration(logFileConfiguration)
                    .queue(new ConcurrentLinkedQueue<Integer>())
                    .build()
            queue.add(5)
            queue.close()
            probes = ByteBuffer.wrap(Files.readAllBytes(testQueuePath))
        then: "there are two probes"
            probes.getInt(0) == 1
            probes.getInt(12) == 1
    }

    private static void cleanDirectory(Path path)
    {
        if (Files.exists(path) && Files.isDirectory(path))
        {
            Files.walk(path)
                    .filter { Files.isRegularFile(it) }
                    .forEach { Files.delete(it) }
        }
    }
}
