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
        when:
            probeTailer.close()
            probeTailer = ProbeTailer.from(configuration)
            probeTailer.read(15)
        then:
            tailer.probes.size() == 20
        when:
            writeElementsTo(5, TestQueueUtil.PATH)
            probeTailer.read(5)
        then:
            tailer.probes.size() == 25
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
        when:
            writeElementsTo(5, TestQueueUtil.PATH)
            probeTailer.readFromStart()
        then:
            tailer.probes.size() == 55
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
        then:
            tailer.probes.size() == 3
            probeTailer.probes() == 7
        when:
            probeTailer.close()
            probeTailer = ProbeTailer.from(configuration)
            probeTailer.read()
        then:
            tailer.probes.size() == 10
        when:
            writeElementsTo(5, TestQueueUtil.PATH)
            probeTailer.read()
        then:
            tailer.probes.size() == 15
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
    }

    def "should correctly read probes in order - read[n] -> read[] -> readFromStart[]"()
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
            probeTailer.read(5)
        then:
            probeTailer.probes() == 5
            tailer.probes.size() == 5
        when:
            probeTailer.read()
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 10
        when:
            writeElementsTo(10, TestQueueUtil.PATH)
            probeTailer.read()
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 20
        when:
            probeTailer.readFromStart()
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 40
        when:
            writeElementsTo(10, TestQueueUtil.PATH)
            probeTailer.readFromStart()
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 70
    }

    def "should correctly read probes in order - read[n] -> readFromStart[] -> read[]"()
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
            probeTailer.read(5)
        then:
            probeTailer.probes() == 5
            tailer.probes.size() == 5
        when:
            probeTailer.readFromStart()
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 15
        when:
            writeElementsTo(10, TestQueueUtil.PATH)
            probeTailer.readFromStart()
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 35
        when:
            probeTailer.read()
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 35
        when:
            writeElementsTo(10, TestQueueUtil.PATH)
            probeTailer.read()
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 45
    }

    def "should correctly read probes in order - read[] -> read[n] -> readFromStart[]"()
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
            probeTailer.read()
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 10
        when:
            probeTailer.read(5)
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 10
        when:
            writeElementsTo(10, TestQueueUtil.PATH)
            probeTailer.read(5)
        then:
            probeTailer.probes() == 5
            tailer.probes.size() == 15
        when:
            probeTailer.readFromStart()
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 35
        when:
            writeElementsTo(10, TestQueueUtil.PATH)
            probeTailer.readFromStart()
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 65
    }

    def "should correctly read probes in order - read[] -> readFromStart[] -> read[n]"()
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
            probeTailer.read()
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 10
        when:
            probeTailer.readFromStart()
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 20
        when:
            writeElementsTo(10, TestQueueUtil.PATH)
            probeTailer.readFromStart()
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 40
        when:
            probeTailer.read(5)
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 40
        when:
            writeElementsTo(10, TestQueueUtil.PATH)
            probeTailer.read(5)
        then:
            probeTailer.probes() == 5
            tailer.probes.size() == 45
    }

    def "should correctly read probes in order - readFromStart[] -> read[] -> read[n]"()
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
            probeTailer.readFromStart()
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 10
        when:
            probeTailer.read()
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 10
        when:
            writeElementsTo(10, TestQueueUtil.PATH)
            probeTailer.read()
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 20
        when:
            probeTailer.read(5)
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 20
        when:
            writeElementsTo(10, TestQueueUtil.PATH)
            probeTailer.read(5)
        then:
            probeTailer.probes() == 5
            tailer.probes.size() == 25
    }

    def "should correctly read probes in order - readFromStart[] -> read[n] -> read[]"()
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
            probeTailer.readFromStart()
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 10
        when:
            probeTailer.read(5)
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 10
        when:
            writeElementsTo(10, TestQueueUtil.PATH)
            probeTailer.read(5)
        then:
            probeTailer.probes() == 5
            tailer.probes.size() == 15
        when:
            probeTailer.read()
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 20
        when:
            writeElementsTo(10, TestQueueUtil.PATH)
            probeTailer.read()
        then:
            probeTailer.probes() == 0
            tailer.probes.size() == 30
    }


    static void writeElementsTo(int elements, Path path)
    {
        QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                .path(path)
                .batchSize(1)
                .mmapSize(OS.pageSize())
                .build()
        StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)

        (2..elements).forEach({ queue.add(it) })
        queue.close()
    }
}
