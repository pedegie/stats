package net.pedegie.stats.integrationtests

import net.openhft.chronicle.core.OS
import net.pedegie.stats.api.queue.Batching
import net.pedegie.stats.api.queue.FileUtils
import net.pedegie.stats.api.queue.QueueConfiguration
import net.pedegie.stats.api.queue.StatsQueue
import net.pedegie.stats.api.queue.WriteThreshold
import net.pedegie.stats.api.queue.probe.Probe
import net.pedegie.stats.api.tailer.ProbeTailer
import net.pedegie.stats.api.tailer.Tailer
import net.pedegie.stats.api.tailer.TailerConfiguration
import spock.lang.Specification

import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.ConcurrentLinkedQueue

class ReadWriteProbes extends Specification
{
    private static final Path PATH = Paths.get(System.getProperty("java.io.tmpdir").toString(), "stats_queue", "stats_queue.log")

    def setup()
    {
        StatsQueue.stopFlusher()
        FileUtils.cleanDirectory(PATH.getParent())
    }

    def cleanupSpec()
    {
        FileUtils.cleanDirectory(PATH.getParent())
    }

    def "probe tailer should be able to subscribe on probes to non-existing file and then wait for probes"()
    {
        given:
            HoldProbesTailer tailer = new HoldProbesTailer()
            TailerConfiguration tailerConfiguration = TailerConfiguration.builder()
                    .tailer(tailer)
                    .mmapSize(OS.pageSize())
                    .path(PATH)
                    .build()
            ProbeTailer probeTailer = ProbeTailer.from(tailerConfiguration)
        and:
            ConcurrentLinkedQueue<Integer> queue = new ConcurrentLinkedQueue<>()

            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(PATH)
                    .batching(new Batching(1))
                    .writeThreshold(WriteThreshold.of(0, 1))
                    .mmapSize(OS.pageSize())
                    .build()

            StatsQueue<Integer> statsQueue = StatsQueue.queue(queue, queueConfiguration)
        when:
            probeTailer.read()
        then:
            tailer.probesRead == 0
        when:
            statsQueue.add(5)
            statsQueue.add(5)
            statsQueue.add(5)
        then:
            probeTailer.read()
            tailer.probesRead == 3
        cleanup:
            statsQueue.close()
            probeTailer.close()
    }

    def "probe tailer should be able to read probes from currently supplying probe file"()
    {
        given:
            ConcurrentLinkedQueue<Integer> queue = new ConcurrentLinkedQueue<>()

            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(PATH)
                    .batching(new Batching(1))
                    .writeThreshold(WriteThreshold.of(0, 1))
                    .mmapSize(OS.pageSize())
                    .build()

            StatsQueue<Integer> statsQueue = StatsQueue.queue(queue, queueConfiguration)

            statsQueue.add(5)
            statsQueue.add(5)
            statsQueue.add(5)
        when:
            HoldProbesTailer tailer = new HoldProbesTailer()
            TailerConfiguration tailerConfiguration = TailerConfiguration.builder()
                    .tailer(tailer)
                    .mmapSize(OS.pageSize())
                    .path(PATH)
                    .build()
            ProbeTailer probeTailer = ProbeTailer.from(tailerConfiguration)
            probeTailer.read()
        then:
            tailer.probesRead == 3
        cleanup:
            statsQueue.close()
            probeTailer.close()
    }

    private static class HoldProbesTailer implements Tailer
    {
        volatile int probesRead

        @Override
        void onProbe(Probe probe)
        {
            probesRead++
        }
    }
}
