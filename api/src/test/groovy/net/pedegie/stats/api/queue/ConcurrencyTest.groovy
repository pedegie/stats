package net.pedegie.stats.api.queue

import net.openhft.chronicle.core.OS
import spock.lang.Ignore
import spock.lang.Specification

import java.time.Clock
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit

@Ignore
// too long, run only on MRs
class ConcurrencyTest extends Specification
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

    def "close message should wait until resize finishes and then close access"()
    {
        given:
            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            errorHandler.errorOnResize(_) >> false
            InternalFileAccessMock fileAccessMock = new InternalFileAccessMock()
            fileAccessMock.onResize = [{ sleep(3000) }, {}].iterator()
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .internalFileAccess(fileAccessMock)
                    .errorHandler(errorHandler)
                    .mmapSize(OS.pageSize())
                    .build()
        when:
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            int[] elementsToFillWholeBuffer = new int[(queueConfiguration.mmapSize / 12)]
            elementsToFillWholeBuffer.each { queue.add(it) }
            queue.add(1)
            queue.closeBlocking()
        then:
            queue.isTerminated()
    }

    def "close message should wait until recycle finishes and then close access"()
    {
        given:
            RecycleTest.WaitingForRecycleInternalAccessMock fileAccessMock = new RecycleTest.WaitingForRecycleInternalAccessMock()
            fileAccessMock.onRecycle = [{ sleep(3000) }, {}].iterator()

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
        when:
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            queue.add(5)
            spyClock.setClock(Clock.fixed(time.plus(1, ChronoUnit.MINUTES).toInstant(), ZoneId.of("UTC")))
            queue.add(5)
            queue.closeBlocking()
        then:
            queue.isTerminated()
    }

    def "should reject requests for registering the same file more than once"()
    {
        given: "queue configuration which sleeps 3 seconds during creating file"
            FileAccessErrorHandler errorHandler = Mock(FileAccessErrorHandler)
            InternalFileAccessMock fileAccessMock = new InternalFileAccessMock()
            fileAccessMock.onAccessContext = [{ sleep(3000) }, {}].iterator()
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .internalFileAccess(fileAccessMock)
                    .errorHandler(errorHandler)
                    .mmapSize(OS.pageSize())
                    .build()
        when: "try creating queue one more time, without sleeping"
            CompletableFuture<StatsQueue<Integer>> queue1 = CompletableFuture.supplyAsync({ TestQueueUtil.createQueue(queueConfiguration) })
            CompletableFuture<StatsQueue<Integer>> queue2 = CompletableFuture.supplyAsync({ TestQueueUtil.createQueue(queueConfiguration) })
            CompletableFuture.allOf([queue1, queue2].toArray() as CompletableFuture[]).get(15, TimeUnit.SECONDS)
        then: "its properly initialized queue"
            Throwable exception = thrown(ExecutionException)
            exception.cause.class == IllegalArgumentException.class
    }
}
