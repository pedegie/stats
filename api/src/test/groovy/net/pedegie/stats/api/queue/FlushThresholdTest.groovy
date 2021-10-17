package net.pedegie.stats.api.queue

import net.openhft.chronicle.core.OS
import net.pedegie.stats.api.tailer.ProbeTailer
import spock.lang.Specification

import java.nio.file.Path

class FlushThresholdTest extends Specification
{
    def setup()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def cleanupSpec()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def "should accept only writes within specified time range"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .flushThreshold(FlushThreshold.of(10, 2))
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when: "we put there 3 elements one by one instantly"
            queue.add(5)
            queue.add(5)
            queue.add(5)
        and: "put next element after delay"
            sleep(10)
            queue.add(5)
            queue.close()
        then: "there are only 2 probes (+1 during close flush)"
            ProbeTailer tailer = ProbeTailer.from(queueConfiguration.withTailer(new TestTailer()))
            tailer.probes() == 3
            tailer.close()
    }

    def "should accept only writes which exceeds specified min size difference"()
    {
        given:
            QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                    .path(TestQueueUtil.PATH)
                    .mmapSize(OS.pageSize())
                    .flushThreshold(FlushThreshold.minSizeDifference(2))
                    .build()
            StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)
        when: "we put there 3 elements, one by one"
            queue.add(1)
            queue.add(2)
            queue.remove(2)
            queue.close()
        then: "it contains only two elements, one added on first add second during flush on close"
            ProbeTailer tailer = ProbeTailer.from(queueConfiguration.withTailer(new TestTailer()))
            tailer.probes() == 2
            tailer.close()
        when: "put there 4 elements, two at each add"
            Path newPath = Path.of(TestQueueUtil.PATH.toString() + "_1")
            queue = TestQueueUtil.createQueue(queueConfiguration.withPath(newPath))
            queue.addAll([1, 2])
            queue.removeAll([1, 2])
            queue.close()
        then: "there are 2 probes added during add/remove (+1 during close flush)"
            ProbeTailer tailer2 = ProbeTailer.from(queueConfiguration.withPath(newPath).withTailer(new TestTailer()))
            tailer2.probes() == 3
            tailer2.close()
    }
}
