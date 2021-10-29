package net.pedegie.stats.api.queue

import net.openhft.chronicle.core.OS
import net.pedegie.stats.api.tailer.ProbeTailer
import net.pedegie.stats.api.tailer.TailerConfiguration
import spock.lang.Specification

class CountProbesTest extends Specification
{
    def setup()
    {
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
                    .batchSize(1)
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
                    .batchSize(1)
                    .flushThreshold(FlushThreshold.flushOnEachWrite())
                    .build()

            TestTailer testTailer = new TestTailer()
            TailerConfiguration tailerConfiguration = TailerConfiguration.builder()
                    .tailer(testTailer)
                    .path(TestQueueUtil.PATH)
                    .batchSize(1)
                    .build()

            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            ProbeTailer probeTailer = ProbeTailer.from(tailerConfiguration)
        when:
            queue.add(1)
        then:
            probeTailer.probes() == 1
            probeTailer.probes() == 1
        when:
            queue.add(1)
            queue.add(1)
        then:
            probeTailer.probes() == 3
            probeTailer.probes() == 3
        cleanup:
            queue.close()
    }

    def "should return correct probes count after reads"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .batchSize(1)
                    .flushThreshold(FlushThreshold.flushOnEachWrite())
                    .build()

            TestTailer testTailer = new TestTailer()
            TailerConfiguration tailerConfiguration = TailerConfiguration.builder()
                    .tailer(testTailer)
                    .path(TestQueueUtil.PATH)
                    .batchSize(1)
                    .build()

            ProbeTailer probeTailer = ProbeTailer.from(tailerConfiguration)
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when:
            queue.add(5)
        then:
            probeTailer.probes() == 1
            probeTailer.probes() == 1
        when:
            probeTailer.read(1)
        then:
            probeTailer.probes() == 0
            probeTailer.probes() == 0
        when:
            queue.add(5)
            queue.add(5)
            queue.add(5)
        then:
            probeTailer.probes() == 3
            probeTailer.probes() == 3
        when:
            probeTailer.read(2)
        then:
            probeTailer.probes() == 1
            probeTailer.probes() == 1
        cleanup:
            queue.close()
    }
}
