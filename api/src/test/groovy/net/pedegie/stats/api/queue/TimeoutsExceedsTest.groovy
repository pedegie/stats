package net.pedegie.stats.api.queue

import net.openhft.chronicle.core.OS
import spock.lang.Ignore
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

@Ignore
// too long, run only on MRs
class TimeoutsExceedsTest extends Specification
{

    private static final int SLEEP_MILLIS = 5000
    private static final int TIMEOUT_THRESHOLD_MILLIS = 3000

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

    def "should correctly handle situation when timeout occurs during registering file"()
    {
        given: "queue configuration which sleeps 3 seconds during creating file"
            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            InternalFileAccessMock fileAccessMock = new InternalFileAccessMock()
            fileAccessMock.onAccessContext = [{ sleep(SLEEP_MILLIS) }, {}].iterator()
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .internalFileAccess(fileAccessMock)
                    .errorHandler(errorHandler)
                    .mmapSize(OS.pageSize())
                    .build()
        and: "set timeout"
            Properties.add("fileaccess.timeoutthresholdmillis", TIMEOUT_THRESHOLD_MILLIS)
        when:
            long start = System.nanoTime()
            TestQueueUtil.createQueue(queueConfiguration)
        then:
            long elapsed = System.nanoTime() - start
            thrown(IllegalStateException)
            elapsed < 3.2e9
            println(elapsed)
            1 * errorHandler.errorOnCreatingFile(_)
        when: "try creating queue one more time, without sleeping"
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            queue.add(5)
            queue.closeBlocking()
        then: "its properly initialized queue"
            queue.size() == 1
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            ByteBuffer.wrap(Files.readAllBytes(logFile)).limit() == DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM
    }

    def "should correctly handle situation when timeout occurs during resizing file"()
    {
        given:
            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            errorHandler.errorOnResize(_) >> false
            InternalFileAccessMock fileAccessMock = new InternalFileAccessMock()
            fileAccessMock.onResize = [{ sleep(SLEEP_MILLIS) }, {}].iterator()
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .internalFileAccess(fileAccessMock)
                    .errorHandler(errorHandler)
                    .mmapSize(OS.pageSize())
                    .build()
        and: "set timeout"
            Properties.add("fileaccess.timeoutthresholdmillis", TIMEOUT_THRESHOLD_MILLIS)
        when:
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            int[] elementsToFillWholeBuffer = new int[(queueConfiguration.mmapSize / 12)]
            elementsToFillWholeBuffer.each { queue.add(it) }
            queue.add(1)
            queue.closeBlocking()
        then:
            1 * errorHandler.errorOnResize(_)
    }

    def "should correctly handle situation when timeout occurs during recycle file"()
    {
        given:
            RecycleTest.WaitingForRecycleInternalAccessMock fileAccessMock = new RecycleTest.WaitingForRecycleInternalAccessMock()
            fileAccessMock.onRecycle = [{ sleep(SLEEP_MILLIS) }, {}].iterator()

            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            errorHandler.errorOnRecycle(_) >> false

            ZonedDateTime time = ZonedDateTime.of(LocalDateTime.parse("2020-01-03T00:00:00"), ZoneId.of("UTC"))
            RecycleTest.SpyClock spyClock = new RecycleTest.SpyClock(Clock.fixed(time.toInstant(), ZoneId.of("UTC")))

            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .internalFileAccess(fileAccessMock)
                    .errorHandler(errorHandler)
                    .fileCycleClock(spyClock)
                    .fileCycleDuration(Duration.of(1, ChronoUnit.MINUTES))
                    .mmapSize(OS.pageSize())
                    .build()
        and: "timeout of 2 seconds"
            Properties.add("fileaccess.timeoutthresholdmillis", TIMEOUT_THRESHOLD_MILLIS)
        when:
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            queue.add(5)
            spyClock.setClock(Clock.fixed(time.plus(1, ChronoUnit.MINUTES).toInstant(), ZoneId.of("UTC")))
            queue.add(5)
            BusyWaiter.busyWait({ fileAccessMock.recycled() }, "recycle termination (test)")
            queue.closeBlocking()
        then:
            1 * errorHandler.errorOnRecycle(_)
    }


    def "should correctly handle situation when timeout occurs during closing file"()
    {
        given: "queue configuration which sleeps 3 seconds during closing file"
            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            InternalFileAccessMock fileAccessMock = new InternalFileAccessMock()
            fileAccessMock.onClose = [{ sleep(SLEEP_MILLIS) }, {}].iterator()
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .errorHandler(errorHandler)
                    .internalFileAccess(fileAccessMock)
                    .mmapSize(OS.pageSize())
                    .build()
        and: "set timeout"
            Properties.add("fileaccess.timeoutthresholdmillis", TIMEOUT_THRESHOLD_MILLIS)
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when:
            long start = System.nanoTime()
            queue.closeBlocking()
            long elapsed = System.nanoTime() - start
        then:
            elapsed < SLEEP_MILLIS * 1e9
            1 * errorHandler.errorOnClosingFile(_)
        when: "trying to close queue again without sleeping"
            queue.closeBlocking()
        then: "it should normally close so we can create new instance"
            StatsQueue<Integer> queue2 = TestQueueUtil.createQueue(queueConfiguration)
            queue2.add(5)
            queue2.closeBlocking()
        then: "its properly initialized queue"
            queue2.size() == 1
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            ByteBuffer.wrap(Files.readAllBytes(logFile)).limit() == DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM
    }


    def "should correctly handle situation when timeout occurs during closing file because of waiting until resize/recycle finishes"()
    {
        given:
            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            errorHandler.errorOnResize(_) >> false
            InternalFileAccessMock fileAccessMock = new InternalFileAccessMock()
            fileAccessMock.onResize = [{ sleep(SLEEP_MILLIS) }, {}].iterator()
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .internalFileAccess(fileAccessMock)
                    .errorHandler(errorHandler)
                    .mmapSize(OS.pageSize())
                    .build()
        and: "set timeout"
            Properties.add("fileaccess.timeoutthresholdmillis", TIMEOUT_THRESHOLD_MILLIS)
        when:
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            int[] elementsToFillWholeBuffer = new int[(queueConfiguration.mmapSize / 12)]
            elementsToFillWholeBuffer.each { queue.add(it) }
            queue.add(1)
            queue.closeBlocking()
        then:
            1 * errorHandler.errorOnClosingFile(_)
    }
}
