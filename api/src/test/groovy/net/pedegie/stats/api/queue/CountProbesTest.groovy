package net.pedegie.stats.api.queue

import net.openhft.chronicle.core.OS
import net.pedegie.stats.api.tailer.ProbeTailer
import net.pedegie.stats.api.tailer.TailerConfiguration
import spock.lang.Specification

class CountProbesTest extends Specification
{
    def setup()
    {
        StatsQueue.stopFlusher()
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def cleanupSpec()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def "should return 0 probes before any write"()
    {
        setup:
            TestTailer testTailer = new TestTailer()
            TailerConfiguration tailerConfiguration = TailerConfiguration.builder()
                    .tailer(testTailer)
                    .path(TestQueueUtil.PATH)
                    .build()
            ProbeTailer probeTailer = ProbeTailer.from(tailerConfiguration)
        expect:
            probeTailer.probes() == 0
            probeTailer.probes() == 0
    }

    def "should return correct probes count after writes"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .batching(new Batching(3))
                    .writeThreshold(WriteThreshold.flushOnEachWrite())
                    .build()

            TestTailer testTailer = new TestTailer()
            TailerConfiguration tailerConfiguration = TailerConfiguration.builder()
                    .tailer(testTailer)
                    .path(TestQueueUtil.PATH)
                    .build()

            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            ProbeTailer probeTailer = ProbeTailer.from(tailerConfiguration)
        when:
            queue.add(1)
            queue.add(1)
            queue.add(1)
        then:
            probeTailer.probes() == 3
            probeTailer.probes() == 3
        when:
            queue.add(1)
            queue.add(1)
            queue.add(1)
            queue.add(1)
            queue.add(1)
            queue.add(1)
            queue.add(1)
            queue.add(1)
            queue.add(1)
        then:
            probeTailer.probes() == 12
            probeTailer.probes() == 12
        cleanup:
            queue.close()
    }

    def "should return correct probes count after reads"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .batching(new Batching(3))
                    .writeThreshold(WriteThreshold.flushOnEachWrite())
                    .build()

            TestTailer testTailer = new TestTailer()
            TailerConfiguration tailerConfiguration = TailerConfiguration.builder()
                    .tailer(testTailer)
                    .path(TestQueueUtil.PATH)
                    .build()

            ProbeTailer probeTailer = ProbeTailer.from(tailerConfiguration)
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when:
            queue.add(5)
            queue.add(5)
            queue.add(5)
        then:
            probeTailer.probes() == 3
            probeTailer.probes() == 3
        when:
            probeTailer.read(1)
            probeTailer.read(1)
            probeTailer.read(1)
        then:
            probeTailer.probes() == 0
            probeTailer.probes() == 0
        when:
            queue.add(5)
            queue.add(5)
            queue.add(5)
            queue.add(5)
            queue.add(5)
            queue.add(5)
            queue.add(5)
            queue.add(5)
            queue.add(5)
        then:
            probeTailer.probes() == 9
            probeTailer.probes() == 9
        when:
            probeTailer.read(1)
        then:
            probeTailer.probes() == 8
            probeTailer.probes() == 8
        cleanup:
            queue.close()
    }

    def "should correctly count probes if there is only single batch"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .batching(new Batching(2))
                    .writeThreshold(WriteThreshold.flushOnEachWrite())
                    .build()

            TestTailer testTailer = new TestTailer()
            TailerConfiguration tailerConfiguration = TailerConfiguration.builder()
                    .tailer(testTailer)
                    .path(TestQueueUtil.PATH)
                    .build()

            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            ProbeTailer probeTailer = ProbeTailer.from(tailerConfiguration)
        when: "add single batch"
            queue.add(1)
            queue.add(1)
        then:
            probeTailer.probes() == 2
        cleanup:
            queue.close()
    }

    def "should correctly count probes if last batch contains only part of probes"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .batching(new Batching(5))
                    .writeThreshold(WriteThreshold.flushOnEachWrite())
                    .build()

            TestTailer testTailer = new TestTailer()
            TailerConfiguration tailerConfiguration = TailerConfiguration.builder()
                    .tailer(testTailer)
                    .path(TestQueueUtil.PATH)
                    .build()

            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            ProbeTailer probeTailer = ProbeTailer.from(tailerConfiguration)
        when: "add single batch"
            queue.add(1)
            queue.close()
        then: "it contains 2 probes, because of close() always put additional probe to file"
            probeTailer.probes() == 2
    }
}
