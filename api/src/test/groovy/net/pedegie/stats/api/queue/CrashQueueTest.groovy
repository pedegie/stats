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
        when: "we add 2 elements"
            queue.add(5)
            queue.add(5)
            queue.closeBlocking()
        then: "there is single broken element in file but two elements in decorated queue"
            queue.size() == 2
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            Files.readAllBytes(logFile).length == halfProbeSize
        where:
            disableCompression << [true, false]
            halfProbeSize << [4, 12]
            probeWriter << [new CrashingProbes.DefaultCrashingProbeWriter(1), new CrashingProbes.CompressedCrashingProbeWriter(ZonedDateTime.now(), 1)]
    }
}
