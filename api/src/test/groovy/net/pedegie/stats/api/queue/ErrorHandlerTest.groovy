package net.pedegie.stats.api.queue

import net.openhft.chronicle.core.OS
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

    def "should close queue on error handler - error during probe write"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .mmapSize(OS.pageSize())
                    .errorHandler(FileAccessErrorHandler.logAndClose())
                    .probeWriter(new ProbeWriters.DefaultCrashingProbeWriter(1))
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when:
            queue.add(5)
            queue.add(5)
        then: "it should contain only half of first probe"
            CrashQueueTest.waitUntilQueueIsClosedOrThrow(queue, 5)
            queue.isClosed()
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            byte[] bytes = Files.readAllBytes(logFile)
            bytes.length == 4
    }

    def "should close queue on error handler - error during close"()
    {
        given:
            InternalFileAccessMock accessMock = new InternalFileAccessMock()
            accessMock.onClose = { throw new TestExpectedException("") }
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .internalFileAccess(accessMock)
                    .mmapSize(OS.pageSize())
                    .errorHandler(FileAccessErrorHandler.logAndClose())
                    .probeWriter(ProbeWriter.defaultProbeWriter())
                    .build()
        when:
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            queue.closeBlocking()
        then:
            queue.isClosed()
    }

    def "should close queue on error handler - error during recycle"()
    {
        given:
            ZonedDateTime time = ZonedDateTime.of(LocalDateTime.parse("2020-01-03T00:00:00"), ZoneId.of("UTC"))
            RecyclerTest.SpyClock spyClock = new RecyclerTest.SpyClock(Clock.fixed(time.toInstant(), ZoneId.of("UTC")))
            InternalFileAccessMock accessMock = new InternalFileAccessMock()
            accessMock.onRecycle = { throw new TestExpectedException("") }

            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .internalFileAccess(accessMock)
                    .fileCycleClock(spyClock)
                    .fileCycleDuration(Duration.of(1, ChronoUnit.MINUTES))
                    .mmapSize(OS.pageSize())
                    .errorHandler(FileAccessErrorHandler.logAndClose())
                    .probeWriter(ProbeWriter.defaultProbeWriter())
                    .build()
        when:
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            queue.add(5)
            spyClock.setClock(Clock.fixed(time.plus(1, ChronoUnit.MINUTES).toInstant(), ZoneId.of("UTC")))
            queue.add(5)
        then:
            CrashQueueTest.waitUntilQueueIsClosedOrThrow(queue, 5)
            queue.isClosed()
    }

    def "should close queue on error handler - error during resize"()
    {
        given:
            InternalFileAccessMock accessMock = new InternalFileAccessMock()
            accessMock.onResize = { throw new TestExpectedException("") }

            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .internalFileAccess(accessMock)
                    .mmapSize(OS.pageSize())
                    .errorHandler(FileAccessErrorHandler.logAndClose())
                    .probeWriter(ProbeWriter.defaultProbeWriter())
                    .build()
        when:
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            int[] elementsToFillWholeBuffer = new int[(queueConfiguration.mmapSize / 12)]
            elementsToFillWholeBuffer.each { queue.add(it) }
            queue.add(1)
        then:
            CrashQueueTest.waitUntilQueueIsClosedOrThrow(queue, 5)
            queue.isClosed()
    }

    def "should continue working on error handler"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .mmapSize(OS.pageSize())
                    .errorHandler(FileAccessErrorHandler.logAndIgnore())
                    .probeWriter(new ProbeWriters.DefaultCrashingProbeWriter(1))
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when:
            queue.add(5)
            queue.add(5)
        then: "it should contain one and half probes"
            queue.closeBlocking()
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            byte[] bytes = Files.readAllBytes(logFile)
            bytes.length == 12 + 4
    }

    def "should invoke error handler if error occurs during initialization"()
    {
        given:
            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .mmapSize(OS.pageSize())
                    .errorHandler(errorHandler)
                    .probeWriter({ throw new TestExpectedException("") })
                    .build()
        when:
            TestQueueUtil.createQueue(queueConfiguration)
        then:
            thrown(IllegalStateException)
            1 * errorHandler.handle(_)
    }

    def "should invoke error handler if error occurs during closing file"()
    {
        given:
            InternalFileAccessMock accessMock = new InternalFileAccessMock()
            accessMock.onClose = { throw new TestExpectedException("") }
            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            errorHandler.handle(_) >> false

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
            1 * errorHandler.handle(_)
    }

    def "if error occurs on close it should ignore result of FileAccessErrorHandler, left file leaked but terminate queue"()
    {
        given:
            InternalFileAccessMock accessMock = new InternalFileAccessMock()
            accessMock.onClose = { throw new TestExpectedException("") }
            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            errorHandler.handle(_) >> true

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
            queue.isClosed()
    }

    def "should invoke error handler if error occurs during recycle"()
    {
        given:
            InternalFileAccessMock accessMock = new InternalFileAccessMock()
            accessMock.onRecycle = { throw new TestExpectedException("") }
            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            errorHandler.handle(_) >> false
            ZonedDateTime time = ZonedDateTime.of(LocalDateTime.parse("2020-01-03T00:00:00"), ZoneId.of("UTC"))
            RecyclerTest.SpyClock spyClock = new RecyclerTest.SpyClock(Clock.fixed(time.toInstant(), ZoneId.of("UTC")))

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
            1 * errorHandler.handle(_)
    }

    def "should invoke error handler if error occurs during resize"()
    {
        given:
            InternalFileAccessMock accessMock = new InternalFileAccessMock()
            accessMock.onResize = { throw new TestExpectedException("") }
            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            errorHandler.handle(_) >> false

            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .mmapSize(OS.pageSize())
                    .errorHandler(errorHandler)
                    .probeWriter(ProbeWriter.defaultProbeWriter())
                    .internalFileAccess(accessMock)
                    .build()
        when:
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            int[] elementsToFillWholeBuffer = new int[(queueConfiguration.mmapSize / 12)]
            elementsToFillWholeBuffer.each { queue.add(it) }
            queue.add(1)
            queue.closeBlocking()
        then:
            1 * errorHandler.handle(_)
    }

}
