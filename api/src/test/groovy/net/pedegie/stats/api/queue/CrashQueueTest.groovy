package net.pedegie.stats.api.queue

import net.openhft.chronicle.core.OS
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path
import java.time.ZonedDateTime

class CrashQueueTest extends Specification
{
    def setup()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def cleanupSpec()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def "should disable writing to file when queue crash"()
    {
        given: "Crashing probe write"
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .probeWriter(probeWriter)
                    .disableCompression(disableCompression)
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when: "we add first element"
            queue.add(5)
        then: "it should close queue due crash"
            waitUntilQueueIsClosedOrThrow(queue, 5)
        when: "we put one more element"
            queue.add(5)
        then: "there is single broken element in file but two elements in decorated queue"
            queue.size() == 2
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            Files.readAllBytes(logFile).length == halfProbeSize
        where:
            disableCompression << [true, false]
            halfProbeSize << [4, 12]
            probeWriter << [new ProbeWriters.DefaultCrashingProbeWriter(1), new ProbeWriters.CompressedCrashingProbeWriter(ZonedDateTime.now(), 1)]
    }

    static boolean waitUntilQueueIsClosedOrThrow(StatsQueue<Integer> queue, int seconds)
    {
        int iterations = 0
        int sleepMillis = 100

        do
        {
            boolean closed = queue.isClosed()
            if(closed)
                return true
            sleep(sleepMillis)
            iterations += sleepMillis
        } while (iterations < (seconds * 1000))

        return false
    }
}
