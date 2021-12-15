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
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

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

    def "should correctly read, all written probes to all queues from multiple threads"()
    {
        given: "2 queues"
            ExecutorService pool = Executors.newFixedThreadPool(8)

            Path path1 = Paths.get(System.getProperty("java.io.tmpdir").toString(), "stats_queue", "stats_queue_1.log").toAbsolutePath()
            Path path2 = Paths.get(System.getProperty("java.io.tmpdir").toString(), "stats_queue", "stats_queue_2.log").toAbsolutePath()

            ConcurrentLinkedQueue<Integer> queue1 = new ConcurrentLinkedQueue<>()
            ConcurrentLinkedQueue<Integer> queue2 = new ConcurrentLinkedQueue<>()

            QueueConfiguration queueConfiguration1 = QueueConfiguration.builder()
                    .path(path1)
                    .batching(new Batching(1))
                    .mmapSize(Integer.MAX_VALUE)
                    .build()
            QueueConfiguration queueConfiguration2 = queueConfiguration1.withPath(path2)

            StatsQueue<Integer> statsQueue1 = StatsQueue.queue(queue1, queueConfiguration1)
            StatsQueue<Integer> statsQueue2 = StatsQueue.queue(queue2, queueConfiguration2)

        when: "concurrently write to 2 queues, 20k elements to each"
            AtomicInteger elementsProcessed = new AtomicInteger(0)
            Runnable addElementsToQueue1 = {
                (1..10000).forEach({
                    statsQueue1.add(it)
                    elementsProcessed.incrementAndGet()
                    if (it % 300 == 0)
                        sleep(10)
                })
            }
            Runnable addElementsToQueue2 = {
                (1..10000).forEach({
                    statsQueue2.add(it)
                    elementsProcessed.incrementAndGet()
                    if (it % 500 == 0)
                        sleep(10)
                })
            }

            pool.submit(addElementsToQueue1)
            pool.submit(addElementsToQueue1)
            pool.submit(addElementsToQueue2)
            pool.submit(addElementsToQueue2)
            pool.shutdown()
            pool.awaitTermination(5, TimeUnit.SECONDS)
            statsQueue1.close()
            statsQueue2.close()

        and: "read from 2 files"
            HoldProbesTailer tailer1 = new HoldProbesTailer()
            HoldProbesTailer tailer2 = new HoldProbesTailer()
            TailerConfiguration tailerConfiguration1 = TailerConfiguration.builder()
                    .tailer(tailer1)
                    .path(path1)
                    .build()
            TailerConfiguration tailerConfiguration2 = TailerConfiguration.builder()
                    .tailer(tailer2)
                    .path(path2)
                    .build()
            ProbeTailer probeTailer1 = ProbeTailer.from(tailerConfiguration1)
            ProbeTailer probeTailer2 = ProbeTailer.from(tailerConfiguration2)

            probeTailer1.read()
            probeTailer2.read()

            probeTailer1.close()
            probeTailer2.close()
        then:
            tailer1.probes.get(tailer1.probes.size() - 1).count == 20000
            tailer2.probes.get(tailer2.probes.size() - 1).count == 20000

    }

    private static class HoldProbesTailer implements Tailer
    {
        List<Probe> probes = new ArrayList<>(30_000)

        @Override
        void onProbe(Probe probe)
        {
            probes.add(probe.copyForStore())
        }
    }
}
