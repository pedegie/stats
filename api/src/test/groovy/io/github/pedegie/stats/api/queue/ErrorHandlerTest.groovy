package io.github.pedegie.stats.api.queue

import io.github.pedegie.stats.api.queue.probe.ProbeAccess
import io.github.pedegie.stats.api.queue.probe.ProbeHolder
import io.github.pedegie.stats.api.tailer.ProbeTailer
import io.github.pedegie.stats.api.tailer.TailerFactory
import net.openhft.chronicle.bytes.BytesIn
import net.openhft.chronicle.bytes.BytesOut
import net.openhft.chronicle.core.OS
import spock.lang.Specification

class ErrorHandlerTest extends Specification
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

    def "should invoke error handler if error occurs during writing probe"()
    {
        given:
            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .writeThreshold(WriteThreshold.flushOnEachWrite())
                    .disableCompression(true)
                    .probeAccess(new ProbeAccess() {
                        int written = 0

                        @Override
                        void writeProbe(BytesOut<?> batchBytes, int count, long timestamp)
                        {
                            if (++written == 1)
                                throw new TestExpectedException()
                        }

                        @Override
                        void readProbeInto(BytesIn<?> batchBytes, ProbeHolder probe)
                        {

                        }
                    })
                    .mmapSize(OS.pageSize())
                    .errorHandler(errorHandler)
                    .build()
        when:
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            queue.add(5)
            queue.close()
        then:
            1 * errorHandler.onError(_)
    }

    def "should invoke error handler if error occurs during closing file"()
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
        cleanup:
            queue.close()
    }

    def "should close queue if error happens during writing probe and onError returns TRUE"()
    {
        given:
            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            errorHandler.onError(_) >> true

            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .batching(new Batching(1))
                    .disableCompression(true)
                    .mmapSize(OS.pageSize())
                    .writeThreshold(WriteThreshold.flushOnEachWrite())
                    .probeAccess(new ProbeAccess() {
                        int written = 0

                        @Override
                        void writeProbe(BytesOut<?> batchBytes, int count, long timestamp)
                        {
                            if (++written == 1)
                                throw new TestExpectedException()

                            batchBytes.writeInt(count)
                            batchBytes.writeLong(timestamp)
                        }

                        @Override
                        void readProbeInto(BytesIn<?> batchBytes, ProbeHolder probe)
                        {

                        }
                    })
                    .errorHandler(errorHandler)
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when:
            queue.add(5)
            queue.add(5)
            queue.add(5)
            queue.close()
        then: "it should contain only first probe"
            ProbeTailer tailer = TailerFactory.tailerFor(TestQueueUtil.PATH)
            tailer.probes() == 1
            tailer.close()
    }

    def "should NOT close queue if error happens during writing probe and onError returns FALSE"()
    {
        given:
            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            errorHandler.onError(_) >> false

            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .batching(new Batching(1))
                    .writeThreshold(WriteThreshold.flushOnEachWrite())
                    .mmapSize(OS.pageSize())
                    .probeAccess(new ProbeAccess() {
                        int written = 0

                        @Override
                        void writeProbe(BytesOut<?> batchBytes, int count, long timestamp)
                        {
                            if (++written == 1)
                                throw new TestExpectedException()

                            batchBytes.writeInt(count)
                            batchBytes.writeLong(timestamp)
                        }

                        @Override
                        void readProbeInto(BytesIn<?> batchBytes, ProbeHolder probe)
                        {

                        }
                    })
                    .errorHandler(errorHandler)
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when:
            queue.add(5)
            queue.add(5)
            queue.add(5)
            queue.close()
        then: "it should contain 2 probes (+1 during close flush)"
            ProbeTailer tailer = TailerFactory.tailerFor(TestQueueUtil.PATH)
            tailer.probes() == 3
            tailer.close()
    }
}
