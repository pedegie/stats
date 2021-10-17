package net.pedegie.stats.api.queue

import net.openhft.chronicle.bytes.BytesOut
import net.openhft.chronicle.core.OS
import net.pedegie.stats.api.queue.probe.ProbeAccess
import net.pedegie.stats.api.tailer.ProbeTailer
import spock.lang.Specification


class ErrorHandlerTest extends Specification
{
    def setup()
    {
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
                    .delayBetweenWritesMillis(0)
                    .disableCompression(true)
                    .probeAccess(new ProbeAccess() {
                        int written = 0

                        @Override
                        void writeProbe(BytesOut<?> mmapedFile, int count, long timestamp)
                        {
                            if (++written == 1)
                                throw new TestExpectedException()
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
                    .disableCompression(true)
                    .mmapSize(OS.pageSize())
                    .delayBetweenWritesMillis(0)
                    .probeAccess(new ProbeAccess() {
                        int written = 0

                        @Override
                        void writeProbe(BytesOut<?> mmapedFile, int count, long timestamp)
                        {
                            if (++written == 1)
                                throw new TestExpectedException()
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
            ProbeTailer tailer = ProbeTailer.from(queueConfiguration.withTailer(new TestTailer()))
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
                    .delayBetweenWritesMillis(0)
                    .mmapSize(OS.pageSize())
                    .probeAccess(new ProbeAccess() {
                        int written = 0

                        @Override
                        void writeProbe(BytesOut<?> mmapedFile, int count, long timestamp)
                        {
                            if (++written == 1)
                                throw new TestExpectedException()

                            mmapedFile.writeInt(count)
                            mmapedFile.writeLong(timestamp)
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
        then: "it should contain 3 probes (+1 during close flush)"
            ProbeTailer tailer = ProbeTailer.from(queueConfiguration.withTailer(new TestTailer()))
            tailer.probes() == 4
            tailer.close()
    }
}
