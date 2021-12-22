package io.github.pedegie.stats.api.tailer

import io.github.pedegie.stats.api.queue.FileUtils
import io.github.pedegie.stats.api.queue.StatsQueue
import io.github.pedegie.stats.api.queue.TestQueueUtil
import io.github.pedegie.stats.api.queue.TestTailer
import io.github.pedegie.stats.api.queue.probe.Probe
import io.github.pedegie.stats.api.queue.probe.ProbeAccess
import net.openhft.chronicle.bytes.BytesIn
import net.openhft.chronicle.bytes.BytesOut
import net.openhft.chronicle.core.OS
import io.github.pedegie.stats.api.queue.Batching
import io.github.pedegie.stats.api.queue.QueueConfiguration
import io.github.pedegie.stats.api.queue.TestExpectedException
import io.github.pedegie.stats.api.queue.probe.ProbeHolder
import spock.lang.Specification

import java.nio.file.Path

class ProbeTailerTest extends Specification
{
    def setup()
    {
        StatsQueue.stopFlusher()
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
        when: "read first 12 probes"
            probeTailer.read(12)
            probeTailer.close()
        and:
            probeTailer = ProbeTailer.from(configuration)
        then: "it has 8 elements more"
            probeTailer.probes() == 8
            tailer.probes.size() == 12
        when: "read from start"
            probeTailer.readFromStart()
        then:
            tailer.probes.size() == 32
        when:
            writeElementsTo(5, TestQueueUtil.PATH)
            probeTailer.readFromStart()
        then:
            tailer.probes.size() == 57
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

    def "should be able to continue reading probes if error throws within ProbeAccess during read[]"()
    {
        given:
            writeElementsTo(10, TestQueueUtil.PATH)
            TestTailer tailer = new TestTailer()
            TailerConfiguration configuration = TailerConfiguration.builder()
                    .tailer(tailer)
                    .probeAccess(new ThrowOnReadProbeAccess(1))
                    .path(TestQueueUtil.PATH)
                    .build()
            ProbeTailer probeTailer = ProbeTailer.from(configuration)
        when:
            probeTailer.read()
        then:
            tailer.probes.size() == 10
        cleanup:
            probeTailer.close()
    }

    def "should be able to continue reading probes if error throws within ProbeAccess during read[n]"()
    {
        given:
            writeElementsTo(10, TestQueueUtil.PATH)
            TestTailer tailer = new TestTailer()
            TailerConfiguration configuration = TailerConfiguration.builder()
                    .tailer(tailer)
                    .probeAccess(new ThrowOnReadProbeAccess(1))
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

    def "should be able to continue reading probes if error throws within ProbeAccess during readFromStart[]"()
    {
        given:
            writeElementsTo(10, TestQueueUtil.PATH)
            TestTailer tailer = new TestTailer()
            TailerConfiguration configuration = TailerConfiguration.builder()
                    .tailer(tailer)
                    .probeAccess(new ThrowOnReadProbeAccess(1))
                    .path(TestQueueUtil.PATH)
                    .build()
            ProbeTailer probeTailer = ProbeTailer.from(configuration)
        when:
            probeTailer.readFromStart()
        then:
            tailer.probes.size() == 10
        cleanup:
            probeTailer.close()
    }

    def "should be able to continue reading probes if error throws within Tailer during read[]"()
    {
        given:
            writeElementsTo(10, TestQueueUtil.PATH)
            ThrowOnReadTailer tailer = new ThrowOnReadTailer(1)
            TailerConfiguration configuration = TailerConfiguration.builder()
                    .tailer(tailer)
                    .path(TestQueueUtil.PATH)
                    .build()
            ProbeTailer probeTailer = ProbeTailer.from(configuration)
        when:
            probeTailer.read()
        then:
            tailer.probes.size() == 10
        cleanup:
            probeTailer.close()
    }

    def "should be able to continue reading probes if error throws within Tailer during read[n]"()
    {
        given:
            writeElementsTo(10, TestQueueUtil.PATH)
            ThrowOnReadTailer tailer = new ThrowOnReadTailer(1)
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

    def "should be able to continue reading probes if error throws within Tailer during readFromStart[]"()
    {
        given:
            writeElementsTo(10, TestQueueUtil.PATH)
            ThrowOnReadTailer tailer = new ThrowOnReadTailer(1)
            TailerConfiguration configuration = TailerConfiguration.builder()
                    .tailer(tailer)
                    .path(TestQueueUtil.PATH)
                    .build()
            ProbeTailer probeTailer = ProbeTailer.from(configuration)
        when:
            probeTailer.readFromStart()
        then:
            tailer.probes.size() == 10
        cleanup:
            probeTailer.close()
    }

    def "closing probe tailer should be idempotent"()
    {
        given:
            writeElementsTo(10, TestQueueUtil.PATH)
            TestTailer tailer = new TestTailer()
            TailerConfiguration configuration = TailerConfiguration.builder()
                    .tailer(tailer)
                    .path(TestQueueUtil.PATH)
                    .build()
            ProbeTailer probeTailer = ProbeTailer.from(configuration)
            probeTailer.read(2)
        when:
            probeTailer.close()
            probeTailer.close()
        then:
            noExceptionThrown()
    }

    static void writeElementsTo(int elements, Path path)
    {
        writeElementsTo(elements, path, 3)
    }

    static void writeElementsTo(int elements, Path path, int batchSize)
    {
        QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                .path(path)
                .batching(new Batching(batchSize))
                .mmapSize(OS.pageSize())
                .build()
        StatsQueue<Integer> queue = TestQueueUtil.createQueue(queueConfiguration)

        (2..elements).forEach({ queue.add(it) })
        queue.close()
    }

    private static class ThrowOnReadProbeAccess implements ProbeAccess
    {
        private final int throwOn
        private int readProbes = 0

        ThrowOnReadProbeAccess(int throwOn)
        {
            this.throwOn = throwOn
        }

        @Override
        void writeProbe(BytesOut<?> batchBytes, int count, long timestamp)
        {

        }

        @Override
        void readProbeInto(BytesIn<?> batchBytes, ProbeHolder probe)
        {
            if (++readProbes == throwOn)
                throw new TestExpectedException()

            probe.setTimestamp(batchBytes.readLong())
            probe.setCount(batchBytes.readInt())
        }
    }

    private static class ThrowOnReadTailer extends TestTailer
    {
        private final int throwOn
        private int readProbes = 0

        ThrowOnReadTailer(int throwOn)
        {
            this.throwOn = throwOn
        }

        @Override
        void onProbe(Probe probe)
        {
            if (++readProbes == throwOn)
                throw new TestExpectedException()

            super.onProbe(probe)
        }
    }
}
