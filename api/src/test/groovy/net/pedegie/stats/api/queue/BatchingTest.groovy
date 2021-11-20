package net.pedegie.stats.api.queue

import net.openhft.chronicle.core.OS
import net.pedegie.stats.api.tailer.ProbeTailer
import net.pedegie.stats.api.tailer.TailerConfiguration
import spock.lang.Specification

import static net.pedegie.stats.api.tailer.ProbeTailerTest.writeElementsTo

class BatchingTest extends Specification
{
    def setup()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def cleanupSpec()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def "should independently batch writes and reads"()
    {
        given:
            int writeBatchSize = 4
            int readBatchSize = 2

            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .batchSize(writeBatchSize)
                    .writeThreshold(WriteThreshold.flushOnEachWrite())
                    .build()

            TestTailer testTailer = new TestTailer()
            TailerConfiguration tailerConfiguration = TailerConfiguration.builder()
                    .tailer(testTailer)
                    .path(TestQueueUtil.PATH)
                    .batchSize(readBatchSize)
                    .build()

            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            ProbeTailer probeTailer = ProbeTailer.from(tailerConfiguration)
        when:
            long probes = probeTailer.probes()
        then:
            probes == 0
        when:
            queue.add(5)
        then:
            probeTailer.probes() == 0
        when:
            queue.add(5)
            queue.add(5)
        then:
            probeTailer.probes() == 0
        when:
            queue.add(5)
        then:
            probeTailer.probes() == 4
        when:
            queue.add(5)
            queue.add(5)
            queue.add(5)
        then:
            probeTailer.probes() == 4
        when:
            queue.add(5)
        then:
            probeTailer.probes() == 8
        when:
            probeTailer.read(3)
        then:
            probeTailer.probes() == 5
        when:
            probeTailer.read(4)
        then:
            probeTailer.probes() == 1
        cleanup:
            queue.close()
            probeTailer.close()
    }

    def "should throw an exception if batch size is less than 1 for queue writer"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .batchSize(batchSize)
                    .writeThreshold(WriteThreshold.flushOnEachWrite())
                    .build()
        when:
            TestQueueUtil.createQueue(queueConfiguration)
        then:
            thrown(IllegalArgumentException)
        where:
            batchSize << [-1, 0]
    }

    def "should throw an exception if batch size is less than 1 for queue tailer"()
    {
        given:
            TestTailer testTailer = new TestTailer()
            TailerConfiguration tailerConfiguration = TailerConfiguration.builder()
                    .tailer(testTailer)
                    .path(TestQueueUtil.PATH)
                    .batchSize(batchSize)
                    .build()
        when:
            ProbeTailer.from(tailerConfiguration)
        then:
            thrown(IllegalArgumentException)
        where:
            batchSize << [-1, 0]
    }

    def "should flush batched probes during closing queue"()
    {
        given:
            int writeBatchSize = 4
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .batchSize(writeBatchSize)
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
            queue.add(5)
        then:
            probeTailer.probes() == 0
        when:
            queue.close()
        then:
            probeTailer.probes() == 2
    }

    def "should flush batched probes during closing tailer"()
    {
        given:
            writeElementsTo(4, TestQueueUtil.PATH)
            TestTailer testTailer = new TestTailer()
            TailerConfiguration tailerConfiguration = TailerConfiguration.builder()
                    .tailer(testTailer)
                    .batchSize(4)
                    .path(TestQueueUtil.PATH)
                    .build()
            ProbeTailer probeTailer = ProbeTailer.from(tailerConfiguration)
        when:
            probeTailer.read(1)
        then:
            probeTailer.probes() == 3
        when:
            probeTailer.close()
            probeTailer = ProbeTailer.from(tailerConfiguration)
        then:
            probeTailer.probes() == 3
        when:
            probeTailer.read(3)
            probeTailer.close()
            probeTailer = ProbeTailer.from(tailerConfiguration)
        then:
            probeTailer.probes() == 0
    }

    def "tailer should not see probes batched and not written yet by queue"()
    {
        given:
            int writeBatchSize = 2
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .batchSize(writeBatchSize)
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
            queue.add(5)
            queue.add(5)
            queue.add(5)
        then:
            probeTailer.probes() == 2
        when:
            probeTailer.read()
        then:
            probeTailer.probes() == 0
            testTailer.probes.size() == 2
        when:
            queue.add(5)
        then:
            probeTailer.probes() == 2
        when:
            probeTailer.read()
        then:
            testTailer.probes.size() == 4
            probeTailer.probes() == 0
        cleanup:
            queue.close()
    }
}
