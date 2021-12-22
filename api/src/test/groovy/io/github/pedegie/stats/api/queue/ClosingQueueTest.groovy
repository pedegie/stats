package io.github.pedegie.stats.api.queue


import io.github.pedegie.stats.api.tailer.ProbeTailer
import io.github.pedegie.stats.api.tailer.TailerFactory
import net.openhft.chronicle.core.OS
import spock.lang.Specification

class ClosingQueueTest extends Specification
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

    def "close should be idempotent"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when:
            queue.close()
            queue.close()
            queue.close()
        then:
            noExceptionThrown()
    }

    def "after closing file, it should accept only messages sent to original queue, discarding writes to file"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .batching(new Batching(1))
                    .writeThreshold(WriteThreshold.flushOnEachWrite())
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when:
            queue.add(5)
            queue.close()
            queue.add(5)
            queue.add(5)
            queue.add(5)
        then: "decorated queue contains all elements, but StatsQueue only 1 (+1 during close flush)"
            queue.size() == 4
            ProbeTailer tailer = TailerFactory.tailerFor(TestQueueUtil.PATH)
            tailer.probes() == 2
            tailer.close()
    }

    def "should flush state on close queue"()
    {
        given: "queue with delay 5 seconds between writes"
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .batching(new Batching(1))
                    .mmapSize(OS.pageSize())
                    .writeThreshold(WriteThreshold.of(5000, 2))
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when: "we put 4 elements, one by one"
            queue.add(5)
            queue.add(5)
            queue.add(5)
            queue.add(5)
        and:
            queue.close()
        then: "there are 2 elements, first added on first add because of millis condition met and second element added during close"
            TestTailer testTailer = new TestTailer()
            ProbeTailer tailer = TailerFactory.tailerFor(TestQueueUtil.PATH, testTailer)
            tailer.probes() == 2
            tailer.read()
            testTailer.probes.get(1).count == 4
            tailer.close()
    }

    def "should set queue into CLOSE_ONLY state, if error happens during close - which means it accepts only close requests, ignoring all the rest"()
    {

        given:
            InternalFileAccessMock accessMock = new InternalFileAccessMock()
            accessMock.onClose = [{ throw new TestExpectedException() }, {}].iterator()
            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)

            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .mmapSize(OS.pageSize())
                    .errorHandler(errorHandler)
                    .internalFileAccess(accessMock)
                    .build()
        when:
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            queue.close()
        then:
            1 * errorHandler.onError(_)
        when:
            queue.add(5)
            queue.add(5)
            queue.close()
        then:
            ProbeTailer tailer = TailerFactory.tailerFor(TestQueueUtil.PATH)
            tailer.probes() == 1
            tailer.close()
    }
}
