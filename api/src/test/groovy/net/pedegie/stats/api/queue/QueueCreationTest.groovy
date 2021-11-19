package net.pedegie.stats.api.queue

import net.openhft.chronicle.core.OS
import spock.lang.Specification

import java.nio.ByteBuffer

class QueueCreationTest extends Specification
{
    def setup()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def cleanupSpec()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def "should throw an exception if mmap size is less than page size"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize() - 1)
                    .build()
        when:
            TestQueueUtil.createQueue(queueConfiguration)
        then:
            thrown(IllegalArgumentException)
    }

    def "should throw an exception when trying to create 2nd queue with the same path"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when: "we create 2nd queue with the same path"
            TestQueueUtil.createQueue(queueConfiguration)
        then:
            thrown(IllegalArgumentException)
        when: "we close queue"
            queue.close()
            queue = TestQueueUtil.createQueue(queueConfiguration)
        then: "we can create again"
            noExceptionThrown()
        cleanup:
            queue.close()
    }

    def "should throw an exception when batch size is less than 1"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .batchSize(0)
                    .build()
        when:
            TestQueueUtil.createQueue(queueConfiguration)
        then:
            thrown(IllegalArgumentException)
    }

    private static ByteBuffer wrap(byte[] bytes)
    {
        return ByteBuffer.wrap(bytes)
    }
}
