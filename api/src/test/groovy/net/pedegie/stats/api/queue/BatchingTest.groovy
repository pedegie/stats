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
        StatsQueue.stopFlusher()
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
                    .batching(new Batching(writeBatchSize))
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
                    .batching(new Batching(batchSize))
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
                    .batching(new Batching(writeBatchSize))
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
                    .batching(new Batching(writeBatchSize))
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

    def "batched probes should be flushed on flush"()
    {
        given:
            int writeBatchSize = 4
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .batching(new Batching(writeBatchSize))
                    .writeThreshold(WriteThreshold.flushOnEachWrite())
                    .build()

            TestTailer testTailer = new TestTailer()
            TailerConfiguration tailerConfiguration = TailerConfiguration.builder()
                    .tailer(testTailer)
                    .path(TestQueueUtil.PATH)
                    .build()

            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            ProbeTailer probeTailer = ProbeTailer.from(tailerConfiguration)
        when: "half of batch added"
            queue.add(5)
            queue.add(5)
        then: "probe tailer doesn't see anything yet"
            probeTailer.probes() == 0
        when: "flush"
            queue.batchFlush()
            probeTailer.read()
        then: "tailer see half of batch probes already"
            probeTailer.probes() == 0
            testTailer.probes.size() == 2
        when: "add 3 more probes, still one is missing to full batch, ignoring previous 2 probes because they were read already"
            queue.add(5)
            queue.add(5)
            queue.add(5)
        then: "tailer doesn't see any probes yet"
            probeTailer.probes() == 0
        when: "add last part of batch"
            queue.add(5)
        then: "tailer see probes"
            probeTailer.probes() == 4
        when:
            probeTailer.read()
        then:
            probeTailer.probes() == 0
            testTailer.probes.size() == 6
        cleanup:
            queue.close()
    }

    def "batch flush should be idempotent if there isn't anything to flush"()
    {
        given:
            int writeBatchSize = 5
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .batching(new Batching(writeBatchSize))
                    .writeThreshold(WriteThreshold.flushOnEachWrite())
                    .build()

            TestTailer testTailer = new TestTailer()
            TailerConfiguration tailerConfiguration = TailerConfiguration.builder()
                    .tailer(testTailer)
                    .path(TestQueueUtil.PATH)
                    .build()

            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            ProbeTailer probeTailer = ProbeTailer.from(tailerConfiguration)
        when: "add one probe and flush"
            queue.add(5)
            queue.batchFlush()
        then: "it contains 1 probe"
            probeTailer.probes() == 1
        when: "add one more probe and flush"
            queue.add(5)
            queue.batchFlush()
        then: "it contains 2 probes"
            probeTailer.probes() == 2
        when: "batch flush without adding additional probes"
            queue.batchFlush()
        then: "no effect"
            probeTailer.probes() == 2
        cleanup:
            queue.close()
    }

    def "batch flush should increase timestamp of next flush if there isn't anything to flush"()
    {
        given:
            int writeBatchSize = 5
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .batching(new Batching(writeBatchSize, 500))
                    .writeThreshold(WriteThreshold.flushOnEachWrite())
                    .build()

            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            long flushTimestamp = queue.lastBatchFlushTimestamp()
        when:
            sleep(3)
        then:
            queue.lastBatchFlushTimestamp() == flushTimestamp
        when:
            queue.batchFlush()
        then:
            queue.lastBatchFlushTimestamp() > flushTimestamp
        cleanup:
            queue.close()

    }

    def "batched probes should be flushed on flush threshold"()
    {
        given:
            int writeBatchSize = 4
            long flushMillisThreshold = 10

            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .batching(new Batching(writeBatchSize, flushMillisThreshold))
                    .writeThreshold(WriteThreshold.flushOnEachWrite())
                    .build()

            TestTailer testTailer = new TestTailer()
            TailerConfiguration tailerConfiguration = TailerConfiguration.builder()
                    .tailer(testTailer)
                    .path(TestQueueUtil.PATH)
                    .build()

            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            ProbeTailer probeTailer = ProbeTailer.from(tailerConfiguration)

        when: "half of batch added"
            queue.add(5)
            queue.add(5)
            boolean flushed = BusyWaiter.busyWaitMillis({ probeTailer.probes() == 2 }, 20, "waiting for flush")
        then:
            flushed
        cleanup:
            queue.close()
    }
}
