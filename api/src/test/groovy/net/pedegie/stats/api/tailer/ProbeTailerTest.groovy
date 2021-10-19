package net.pedegie.stats.api.tailer

import net.openhft.chronicle.core.OS
import net.pedegie.stats.api.queue.FileUtils
import net.pedegie.stats.api.queue.QueueConfiguration
import net.pedegie.stats.api.queue.StatsQueue
import net.pedegie.stats.api.queue.TestQueueUtil
import net.pedegie.stats.api.queue.TestTailer
import spock.lang.Specification

import java.nio.file.Path

class ProbeTailerTest extends Specification
{
    def setup()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def cleanupSpec()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def "should read n probes"()
    {
        given:
            writeElementsTo(20, TestQueueUtil.PATH)
            TestTailer tailer = new TestTailer()
            TailerConfiguration configuration = TailerConfiguration.builder()
                    .tailer(tailer)
                    .path(TestQueueUtil.PATH)
                    .build()
            ProbeTailer probeTailer = ProbeTailer.from(configuration)
        when:
            probeTailer.read(10)
        then:
            tailer.probes.size() == 10
        cleanup:
            probeTailer.close()
    }

    def "should read all probes from beginning"()
    {
        given:
            writeElementsTo(20, TestQueueUtil.PATH)
            TestTailer tailer = new TestTailer()
            TailerConfiguration configuration = TailerConfiguration.builder()
                    .tailer(tailer)
                    .path(TestQueueUtil.PATH)
                    .build()
            ProbeTailer probeTailer = ProbeTailer.from(configuration)
        when: "read first 10 probes"
            probeTailer.read(10)
            probeTailer.close()
        and:
            probeTailer = ProbeTailer.from(configuration)
        then: "it has 10 elements more"
            probeTailer.probes() == 10
        when: "read from start"
            probeTailer.readFromStart()
        then:
            tailer.probes.size() == 30
        cleanup:
            probeTailer.close()
    }

    def "should continue reading probes from last read probe"()
    {
        given:
            writeElementsTo(10, TestQueueUtil.PATH)
            TestTailer tailer = new TestTailer()
            TailerConfiguration configuration = TailerConfiguration.builder()
                    .tailer(tailer)
                    .path(TestQueueUtil.PATH)
                    .build()
            ProbeTailer probeTailer = ProbeTailer.from(configuration)
        when:
            probeTailer.read(3)
            probeTailer.close()
            probeTailer = ProbeTailer.from(configuration)
            probeTailer.read()
        then:
            tailer.probes.size() == 10
        cleanup:
            probeTailer.close()
    }

    def "reading an amount should return true if tailer has more elements"()
    {
        given:
            writeElementsTo(10, TestQueueUtil.PATH)
            TestTailer tailer = new TestTailer()
            TailerConfiguration configuration = TailerConfiguration.builder()
                    .tailer(tailer)
                    .path(TestQueueUtil.PATH)
                    .build()
            ProbeTailer probeTailer = ProbeTailer.from(configuration)
        when:
            boolean readAll = probeTailer.read(9)
        then:
            readAll
        when:
            readAll = probeTailer.read(1)
        then:
            readAll
        when:
            readAll = probeTailer.read(1)
        then:
            !readAll
        cleanup:
            probeTailer.close()
    }

    private static void writeElementsTo(int elements, Path path)
    {
        QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                .path(path)
                .mmapSize(OS.pageSize())
                .build()
        StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)

        (2..elements).forEach({ queue.add(it) })
        queue.close()
    }
}
