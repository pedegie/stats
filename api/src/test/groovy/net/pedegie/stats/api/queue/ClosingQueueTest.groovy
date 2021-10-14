package net.pedegie.stats.api.queue

import net.openhft.chronicle.core.OS
import spock.lang.Requires
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.TimeUnit

@Requires({ env.LONG_RUNNING_TESTS == 'true' })
class ClosingQueueTest extends Specification
{
    def setup()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def cleanup()
    {
        StatsQueue.shutdown()
        Properties.clear()
    }

    def cleanupSpec()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def "should close queue asynchronously"()
    {
        given: "queue with delayed probe writer"
            int delaySeconds = 1
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .mmapSize(OS.pageSize())
                    .probeWriter(new ProbeWriters.DelayedProbeWriter(delaySeconds))
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when: "we add two elements and close"
            long start = System.nanoTime()
            queue.add(1)
            queue.add(1)
            queue.close()
            long elapsed = System.nanoTime() - start
        then: "it should take less than 2 seconds to order a close message"
            TimeUnit.SECONDS.convert(elapsed, TimeUnit.NANOSECONDS) < 2
        and: "queue is not closed yet"
            !queue.isTerminated()
        and: "its closed after 3 seconds"
            sleep(3000)
            queue.isTerminated()
    }

    def "should close queue synchronously on closeBlocking"()
    {
        given: "queue with delayed probe writer"
            int delaySeconds = 1
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .mmapSize(OS.pageSize())
                    .probeWriter(new ProbeWriters.DelayedProbeWriter(delaySeconds))
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when: "we add two elements and close"
            long start = System.nanoTime()
            queue.add(1)
            queue.add(1)
            queue.closeBlocking()
            long elapsed = System.nanoTime() - start
        then: "it should take more than 2 seconds to close queue"
            TimeUnit.SECONDS.convert(elapsed, TimeUnit.NANOSECONDS) >= 2
            queue.isTerminated()
    }

    def "should terminate all queues on shutdown but process all remaining probes"()
    {
        given: "queue with delayed probe writer"
            Path path1 = Paths.get(TestQueueUtil.PATH.toString() + "1")
            Path path2 = Paths.get(TestQueueUtil.PATH.toString() + "2")
            QueueConfiguration queueConfiguration1 = createConfiguration(path1)
            QueueConfiguration queueConfiguration2 = createConfiguration(path2)
            StatsQueue<Integer> queue1 = TestQueueUtil.createQueue(queueConfiguration1)
            StatsQueue<Integer> queue2 = TestQueueUtil.createQueue(queueConfiguration2)
        when: "we add two elements and close"
            queue1.add(1)
            queue1.add(1)
            queue2.add(1)
            queue2.add(1)
            StatsQueue.shutdown()
        then: "both queues are closed"
            queue1.isTerminated()
            queue2.isTerminated()
        and: "there are all probes in both files"
            ByteBuffer.wrap(Files.readAllBytes(TestQueueUtil.findExactlyOneOrThrow(path1))).limit() == 2 * 12
            ByteBuffer.wrap(Files.readAllBytes(TestQueueUtil.findExactlyOneOrThrow(path2))).limit() == 2 * 12
    }

    def "should terminate all queues on shutdownForce without processing remaining probes"()
    {
        given: "queue with delayed probe writer"
            Path path1 = Paths.get(TestQueueUtil.PATH.toString() + "1")
            Path path2 = Paths.get(TestQueueUtil.PATH.toString() + "2")
            QueueConfiguration queueConfiguration1 = createConfiguration(path1)
            QueueConfiguration queueConfiguration2 = createConfiguration(path2)
            StatsQueue<Integer> queue1 = TestQueueUtil.createQueue(queueConfiguration1)
            StatsQueue<Integer> queue2 = TestQueueUtil.createQueue(queueConfiguration2)
        when: "we add two elements and close"
            queue1.add(1)
            queue1.add(1)
            queue2.add(1)
            queue2.add(1)
            StatsQueue.shutdownForce()
        then: "both queues are closed"
            queue1.isTerminated()
            queue2.isTerminated()
        and: "its missing some probes in files"
            ByteBuffer.wrap(Files.readAllBytes(TestQueueUtil.findExactlyOneOrThrow(path1))).limit() != 2 * 12
            ByteBuffer.wrap(Files.readAllBytes(TestQueueUtil.findExactlyOneOrThrow(path2))).limit() != 2 * 12
    }

    def "should close all queues irrespective of their state on timeout threshold during shutdown"()
    {
        given: "3 queues with different close duration sleep"
            Properties.add("fileaccess.timeoutthresholdmillis", 1000)
            InternalFileAccessMock accessMock = new InternalFileAccessMock()
            accessMock.onClose = { FileAccessContext accessContext -> sleep(1000 * closeSleep) }.iterator()
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .internalFileAccess(accessMock)
                    .mmapSize(OS.pageSize())
                    .build()
        when:
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            long start = System.nanoTime()
            StatsQueue.shutdown()
            long elapsed = System.nanoTime() - start
        then:
            queue.isTerminated()
            elapsed < 2.3e9
        where:
            closeSleep << [0, 1, 2, 3, 4]

    }

    def "should set file in CLOSE_ONLY state, when queue fails to close itself"()
    {
        given:
            InternalFileAccessMock accessMock = new InternalFileAccessMock()
            accessMock.onClose = [{ throw new TestExpectedException("") }, {}].iterator()

            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .disableCompression(true)
                    .mmapSize(OS.pageSize())
                    .internalFileAccess(accessMock)
                    .build()
        when:
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
            queue.add(5)
            queue.closeBlocking()
        then: "it should crash and set CLOSE_ONLY state, what means we can't save probes anymore, but we can send CLOSE_MESSAGE again"
            !queue.isTerminated()
            queue.add(5)
            queue.add(5)
            queue.closeBlocking()
            ErrorHandlerTest.waitUntilQueueIsTerminatedOrThrow(queue, 5)
        and: "it contains only first one element, added before first close"
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            byte[] bytes = Files.readAllBytes(logFile)
            bytes.length == DefaultProbeWriter.PROBE_AND_TIMESTAMP_BYTES_SUM
    }


    private static QueueConfiguration createConfiguration(Path path)
    {
        return QueueConfiguration.builder()
                .path(path)
                .disableCompression(true)
                .mmapSize(OS.pageSize())
                .probeWriter(new ProbeWriters.DelayedProbeWriter(1))
                .build()
    }
}
