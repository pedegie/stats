package net.pedegie.stats.api.queue

import net.openhft.chronicle.core.OS
import net.pedegie.stats.api.queue.probe.DefaultProbeWriter
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

class ErrorHandlerTest extends Specification
{
    def setup()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def cleanup()
    {
        StatsQueue.shutdown()
    }

    def cleanupSpec()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def "should invoke error handler if error occurs during creating file"()
    {
        given:
            InternalFileAccessMock accessMock = new InternalFileAccessMock()
            accessMock.onAccessContext = [{ throw new TestExpectedException("") }].iterator()

            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .mmapSize(OS.pageSize())
                    .internalFileAccess(accessMock)
                    .errorHandler(errorHandler)
                    .build()
        when:
            TestQueueUtil.createQueue(queueConfiguration)
        then:
            thrown(IllegalStateException)
            1 * errorHandler.errorOnCreatingFile(_)
    }

    def "should invoke error handler if error occurs during writing probe"()
    {
        given:
            InternalFileAccessMock accessMock = new InternalFileAccessMock()
            accessMock.onWriteProbe = [{ throw new TestExpectedException("") }].iterator()

            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .mmapSize(OS.pageSize())
                    .internalFileAccess(accessMock)
                    .errorHandler(errorHandler)
                    .build()
        when:
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            queue.add(5)
            queue.closeBlocking()
        then:
            1 * errorHandler.errorOnProbeWrite(_)
    }

    def "should invoke error handler if error occurs during recycle"()
    {
        given:
            InternalFileAccessMock accessMock = new InternalFileAccessMock()
            accessMock.onRecycle = { throw new TestExpectedException("") }.iterator()
            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            ZonedDateTime time = ZonedDateTime.of(LocalDateTime.parse("2020-01-03T00:00:00"), ZoneId.of("UTC"))
            RecycleTest.SpyClock spyClock = new RecycleTest.SpyClock(Clock.fixed(time.toInstant(), ZoneId.of("UTC")))

            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .mmapSize(OS.pageSize())
                    .fileCycleClock(spyClock)
                    .fileCycleDuration(Duration.of(1, ChronoUnit.MINUTES))
                    .errorHandler(errorHandler)
                    .internalFileAccess(accessMock)
                    .build()
        when:
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            queue.add(5)
            spyClock.setClock(Clock.fixed(time.plus(1, ChronoUnit.MINUTES).toInstant(), ZoneId.of("UTC")))
            queue.add(5)
            queue.closeBlocking()
        then:
            1 * errorHandler.errorOnRecycle(_)
    }

    def "should invoke error handler if error occurs during resize"()
    {
        given:
            InternalFileAccessMock accessMock = new InternalFileAccessMock()
            accessMock.onResize = { throw new TestExpectedException("") }.iterator()
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
            int[] elementsToFillWholeBuffer = new int[(queueConfiguration.mmapSize / 12)]
            elementsToFillWholeBuffer.each { queue.add(it) }
            queue.add(1)
            queue.closeBlocking()
        then:
            1 * errorHandler.errorOnResize(_)
    }

    def "should invoke error handler if error occurs during closing file"()
    {
        given:
            InternalFileAccessMock accessMock = new InternalFileAccessMock()
            accessMock.onClose = [{ throw new TestExpectedException("") }].iterator()
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
            queue.closeBlocking()
        then:
            1 * errorHandler.errorOnClosingFile(_)
    }

    def "should close queue if error happens during writing probe and errorOnProbeWrite returns TRUE"()
    {
        given:
            InternalFileAccessMock accessMock = new InternalFileAccessMock()
            accessMock.onWriteProbe = [{}, { throw new TestExpectedException("") }, {}].iterator()
            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            errorHandler.errorOnProbeWrite(_) >> true

            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .mmapSize(OS.pageSize())
                    .internalFileAccess(accessMock)
                    .errorHandler(errorHandler)
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when:
            queue.add(5)
            queue.add(5)
            queue.add(5)
        then: "it should contain only first probe"
            waitUntilQueueIsTerminatedOrThrow(queue, 5)
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            byte[] bytes = Files.readAllBytes(logFile)
            bytes.length == DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM
    }

    def "should NOT close queue if error happens during writing probe and errorOnProbeWrite returns FALSE"()
    {
        given:
            InternalFileAccessMock accessMock = new InternalFileAccessMock()
            accessMock.onWriteProbe = [{}, { throw new TestExpectedException("") }, {}].iterator()
            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            errorHandler.errorOnProbeWrite(_) >> false

            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .mmapSize(OS.pageSize())
                    .internalFileAccess(accessMock)
                    .errorHandler(errorHandler)
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when:
            queue.add(5)
            queue.add(5)
            queue.add(5)
        then: "it should contain two probes"
            queue.closeBlocking()
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            byte[] bytes = Files.readAllBytes(logFile)
            bytes.length == DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM * 2
    }

    def "should close queue if error happens during resize and errorOnResize returns TRUE"()
    {
        given:
            InternalFileAccessMock accessMock = new InternalFileAccessMock()
            accessMock.onResize = [{ throw new TestExpectedException("") }].iterator()
            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            errorHandler.errorOnResize(_) >> true

            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .mmapSize(OS.pageSize())
                    .internalFileAccess(accessMock)
                    .errorHandler(errorHandler)
                    .build()
        when: "put one element over mmap size"
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            int[] elementsToFillWholeBuffer = new int[(queueConfiguration.mmapSize / DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM)]
            elementsToFillWholeBuffer.each { queue.add(it) }
            queue.add(1)
        and: "one more element, after queue crashed during resize on previous element"
            queue.add(5)
        then: "queue is closed, and not contains last element"
            waitUntilQueueIsTerminatedOrThrow(queue, 5)
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            byte[] bytes = Files.readAllBytes(logFile)
            bytes.length == DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM * elementsToFillWholeBuffer.length

    }

    def "should NOT close queue if error happens during resize and errorOnResize returns FALSE"()
    {
        given:
            ResizeTest.WaitingForResizeInternalAccessMock accessMock = new ResizeTest.WaitingForResizeInternalAccessMock()
            accessMock.onResize = [{ throw new TestExpectedException("") }, {}].iterator()

            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            errorHandler.errorOnResize(_) >> false

            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .mmapSize(OS.pageSize())
                    .internalFileAccess(accessMock)
                    .errorHandler(errorHandler)
                    .build()
        when: "put one element over mmap size"
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            int[] elementsToFillWholeBuffer = new int[(queueConfiguration.mmapSize / DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM)]
            elementsToFillWholeBuffer.each { queue.add(it) }
            queue.add(1)
            BusyWaiter.busyWait({ accessMock.writesEnabled() }, "resize termination (test)")
        and: "one more element, after queue crashed during resize on previous element - which will cause resizing again"
            accessMock.reset()
            queue.add(5)
            BusyWaiter.busyWait({ accessMock.writesEnabled() }, "resize termination (test)")
        and: "one more, after resizing"
            queue.add(5)
            queue.closeBlocking()
        then: "queue was open, and contains last element, added after resizing"
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            byte[] bytes = Files.readAllBytes(logFile)
            bytes.length == DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM * (elementsToFillWholeBuffer.length + 1)
    }

    def "should close queue if error happens during recycle and errorOnRecycle returns TRUE"()
    {
        given:
            InternalFileAccessMock accessMock = new InternalFileAccessMock()
            accessMock.onRecycle = [{ throw new TestExpectedException("") }].iterator()
            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            errorHandler.errorOnRecycle(_) >> true

            ZonedDateTime time = ZonedDateTime.of(LocalDateTime.parse("2020-01-03T00:00:00"), ZoneId.of("UTC"))
            RecycleTest.SpyClock spyClock = new RecycleTest.SpyClock(Clock.fixed(time.toInstant(), ZoneId.of("UTC")))

            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .mmapSize(OS.pageSize())
                    .fileCycleClock(spyClock)
                    .fileCycleDuration(Duration.of(1, ChronoUnit.MINUTES))
                    .internalFileAccess(accessMock)
                    .errorHandler(errorHandler)
                    .build()
        when: "put one element"
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            queue.add(5)
        and: "set clock, one cycle ahead"
            spyClock.setClock(Clock.fixed(time.plus(1, ChronoUnit.MINUTES).toInstant(), ZoneId.of("UTC")))
        and: "put next element which cause recycle and crash"
            queue.add(5)
        and: "we put two more elements"
            queue.add(5)
            queue.add(5)
        then: "it should contain only one element, written before crash"
            waitUntilQueueIsTerminatedOrThrow(queue, 5)
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            byte[] bytes = Files.readAllBytes(logFile)
            bytes.length == DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM
    }

    def "should NOT close queue if error happens during recycle and errorOnRecycle returns FALSE"()
    {
        given:
            RecycleTest.WaitingForRecycleInternalAccessMock accessMock = new RecycleTest.WaitingForRecycleInternalAccessMock()
            accessMock.onRecycle = [{ throw new TestExpectedException("") }].iterator()
            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            errorHandler.errorOnRecycle(_) >> false

            ZonedDateTime time = ZonedDateTime.of(LocalDateTime.parse("2020-01-03T00:00:00"), ZoneId.of("UTC"))
            RecycleTest.SpyClock spyClock = new RecycleTest.SpyClock(Clock.fixed(time.toInstant(), ZoneId.of("UTC")))

            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .mmapSize(OS.pageSize())
                    .fileCycleClock(spyClock)
                    .fileCycleDuration(Duration.of(1, ChronoUnit.MINUTES))
                    .internalFileAccess(accessMock)
                    .errorHandler(errorHandler)
                    .build()
        when: "put one element"
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            queue.add(5)
        and: "set clock, one cycle ahead"
            spyClock.setClock(Clock.fixed(time.plus(1, ChronoUnit.MINUTES).toInstant(), ZoneId.of("UTC")))
        and: "put next element which cause recycle and crash"
            queue.add(5)
            BusyWaiter.busyWait({ accessMock.writesEnabled() }, "recycle termination (test)")
            accessMock.reset()
        and: "we put one more element which causes recycle again"
            queue.add(5)
            BusyWaiter.busyWait({ accessMock.writesEnabled() }, "recycle termination (test)")
        and: "and then one more element"
            queue.add(5)
        then: "it should contain two elements in two files, one written before crash and 2nd one after recycle"
            queue.closeBlocking()
            List<Path> logFiles = TestQueueUtil.findMany(TestQueueUtil.PATH).sort { it.toString() }
            logFiles.size() == 2

            Files.readAllBytes(logFiles.get(0)).length == DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM
            Files.readAllBytes(logFiles.get(1)).length == DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM
    }

    static boolean waitUntilQueueIsTerminatedOrThrow(StatsQueue<Integer> queue, int seconds)
    {
        int iterations = 0
        int sleepMillis = 100

        do
        {
            boolean closed = queue.isTerminated()
            if (closed)
                return true
            sleep(sleepMillis)
            iterations += sleepMillis
        } while (iterations < (seconds * 1000))

        return false
    }
}
