package net.pedegie.stats.api.queue

import net.openhft.chronicle.core.OS
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path

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
            LogFileConfiguration logFileConfiguration = LogFileConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .probeWriter(new CrashingProbes.DefaultCrashingProbeWriter(1))
                    .disableCompression(true)
                    .build()
            MPMCQueueStats<Integer> queue = TestQueueUtil.createQueue(logFileConfiguration)
        when: "we add 2 elements"
            queue.add(5)
            queue.add(5)
        then: "there is single broken element in file but two elements in decorated queue"
            queue.size() == 2
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            Files.readAllBytes(logFile).length == DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM
    }
}
