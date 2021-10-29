package net.pedegie.stats.integrationtests

import net.openhft.chronicle.core.OS
import net.pedegie.stats.api.queue.BusyWaiter
import net.pedegie.stats.api.queue.FileUtils
import net.pedegie.stats.api.queue.FlushThreshold
import net.pedegie.stats.api.queue.QueueConfiguration
import net.pedegie.stats.api.queue.StatsQueue
import net.pedegie.stats.api.queue.probe.Probe
import net.pedegie.stats.api.tailer.ProbeTailer
import net.pedegie.stats.api.tailer.ProbeTailerScheduler
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

    public static Path PATH_1 = Paths.get(System.getProperty("java.io.tmpdir").toString(), "stats_queue", "stats_queue_1.log").toAbsolutePath()
    public static Path PATH_2 = Paths.get(System.getProperty("java.io.tmpdir").toString(), "stats_queue", "stats_queue_2.log").toAbsolutePath()

    def setup()
    {
        System.setProperty("disable.thread.safety", "true");
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
                    .batchSize(1)
                    .flushThreshold(FlushThreshold.of(0, 1))
                    .mmapSize(OS.pageSize())
                    .build()

            StatsQueue<Integer> statsQueue = StatsQueue.builder()
                    .queue(queue)
                    .queueConfiguration(queueConfiguration)
                    .build()
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
                    .batchSize(1)
                    .flushThreshold(FlushThreshold.of(0, 1))
                    .mmapSize(OS.pageSize())
                    .build()

            StatsQueue<Integer> statsQueue = StatsQueue.builder()
                    .queue(queue)
                    .queueConfiguration(queueConfiguration)
                    .build()

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
            ConcurrentLinkedQueue<Integer> queue1 = new ConcurrentLinkedQueue<>()
            ConcurrentLinkedQueue<Integer> queue2 = new ConcurrentLinkedQueue<>()

            QueueConfiguration queueConfiguration1 = QueueConfiguration.builder()
                    .path(PATH_1)
                    .countDropped(true)
                    .batchSize(1)
                    .mmapSize(Integer.MAX_VALUE)
                    .build()
            QueueConfiguration queueConfiguration2 = queueConfiguration1.withPath(PATH_2)

            StatsQueue<Integer> statsQueue1 = StatsQueue.builder()
                    .queue(queue1)
                    .queueConfiguration(queueConfiguration1)
                    .build()
            StatsQueue<Integer> statsQueue2 = StatsQueue.builder()
                    .queue(queue2)
                    .queueConfiguration(queueConfiguration2)
                    .build()
        and: "2 tailers"
            HoldProbesTailer tailer1 = new HoldProbesTailer()
            HoldProbesTailer tailer2 = new HoldProbesTailer()
            TailerConfiguration tailerConfiguration1 = TailerConfiguration.builder()
                    .tailer(tailer1)
                    .path(PATH_1)
                    .build()
            TailerConfiguration tailerConfiguration2 = TailerConfiguration.builder()
                    .tailer(tailer2)
                    .path(PATH_2)
                    .build()
            ProbeTailer probeTailer1 = ProbeTailer.from(tailerConfiguration1)
            ProbeTailer probeTailer2 = ProbeTailer.from(tailerConfiguration2)

        and: "probe tailer scheduler"
            ProbeTailerScheduler scheduler = ProbeTailerScheduler.create(2)
        when: "concurrently write and read from queue"
            AtomicInteger elementsProcessed = new AtomicInteger(0)
            int requiredAmountOfReadElements = 80_000
            Runnable addElementsToQueue1 = {
                (1..10000).forEach({
                    statsQueue1.add(it)
                    elementsProcessed.incrementAndGet()
                    if (it % 500 == 0)
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

            Runnable consumeElementsFromQueue1 = {
                while (true)
                {
                    Integer elem = statsQueue1.poll()
                    if (elem != null)
                        elementsProcessed.incrementAndGet()
                    else if (elem == null && elementsProcessed.get() == requiredAmountOfReadElements)
                        break
                }
            }

            Runnable consumeElementsFromQueue2 = {
                while (true)
                {
                    Integer elem = statsQueue2.poll()
                    if (elem != null)
                        elementsProcessed.incrementAndGet()
                    else if (elem == null && elementsProcessed.get() == requiredAmountOfReadElements)
                        break
                }
            }

            ExecutorService pool = Executors.newFixedThreadPool(8)
            scheduler.addTailer(probeTailer2)
            pool.submit(addElementsToQueue1)
            pool.submit(addElementsToQueue1)
            pool.submit(consumeElementsFromQueue2)
            pool.submit(consumeElementsFromQueue2)
            pool.submit(consumeElementsFromQueue1)
            pool.submit(consumeElementsFromQueue1)
            pool.submit(addElementsToQueue2)
            pool.submit(addElementsToQueue2)
            scheduler.addTailer(probeTailer1)

            boolean allRead = BusyWaiter.busyWait({
                long read = tailer1.probesRead + tailer2.probesRead + statsQueue1.dropped + statsQueue2.dropped
                read == requiredAmountOfReadElements
            }, 5, "waiting for tailers read")

            scheduler.close()
            pool.shutdown()
            statsQueue1.close()
            statsQueue2.close()
            boolean terminated = pool.awaitTermination(30, TimeUnit.SECONDS)
        then: "probe tailers have read all probes"
            terminated
            allRead

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
