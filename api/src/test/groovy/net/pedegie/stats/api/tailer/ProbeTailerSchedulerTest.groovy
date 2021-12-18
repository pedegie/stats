package net.pedegie.stats.api.tailer

import net.pedegie.stats.api.queue.FileUtils
import net.pedegie.stats.api.queue.StatsQueue
import net.pedegie.stats.api.queue.TestExpectedException
import net.pedegie.stats.api.queue.TestQueueUtil
import net.pedegie.stats.api.queue.TestTailer
import spock.lang.Specification

import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicBoolean

class ProbeTailerSchedulerTest extends Specification
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

    def "closing scheduler should close all tailers"()
    {
        given:
            Path path1 = Paths.get(TestQueueUtil.PATH.toString() + "_1")
            Path path2 = Paths.get(TestQueueUtil.PATH.toString() + "_2")
            ProbeTailer tailer1 = TailerFactory.tailerFor(path1)
            ProbeTailer tailer2 = TailerFactory.tailerFor(path2)
        when:
            ProbeTailerScheduler scheduler = ProbeTailerScheduler.create(2, 5)
            scheduler.addTailer(tailer1)
            scheduler.addTailer(tailer2)
        and:
            scheduler.close()
        then:
            tailer1.isClosed()
            tailer2.isClosed()
    }

    def "should correctly close tailers if exception throws during closing particular one"()
    {
        given: "3 tailers configurations"
            Path path1 = Paths.get(TestQueueUtil.PATH.toString() + "_1")
            Path path2 = Paths.get(TestQueueUtil.PATH.toString() + "_2")
            Path path3 = Paths.get(TestQueueUtil.PATH.toString() + "_2")

            TestTailer testTailer1 = new TestTailer()
            TestTailer testTailer2 = new TestTailer()
            TestTailer testTailer3 = new TestTailer()

            TailerConfiguration configuration1 = TailerConfiguration.builder()
                    .tailer(testTailer1)
                    .path(path1)
                    .build()
            TailerConfiguration configuration2 = configuration1
                    .withTailer(testTailer2)
                    .withPath(path2)
            TailerConfiguration configuration3 = configuration1
                    .withTailer(testTailer3)
                    .withPath(path3)

            ProbeTailerScheduler scheduler = ProbeTailerScheduler.create()

        and: "two tailers throwing exception during first close"
            AtomicBoolean closed1 = new AtomicBoolean()
            ProbeTailer tailer1 = Mock(ProbeTailerImpl, constructorArgs: [configuration1])
            tailer1.close() >> { throw new TestExpectedException() } >> { closed1.set(true) }

            AtomicBoolean closed2 = new AtomicBoolean()
            ProbeTailer tailer2 = Mock(ProbeTailerImpl, constructorArgs: [configuration2])
            tailer2.close() >> { throw new TestExpectedException() } >> { closed2.set(true) }
        and: "one tailer which close successfully"
            ProbeTailer tailer3 = ProbeTailer.from(configuration3)

            scheduler.addTailer(tailer1)
            scheduler.addTailer(tailer2)
            scheduler.addTailer(tailer3)
        when: "first scheduler close"
            scheduler.close()
        then: "one tailer closed"
            !tailer1.isClosed()
            !tailer2.isClosed()
            tailer3.isClosed()
        when: "second scheduler close"
            scheduler.close()
        then: "all tailers closed"
            closed1.get()
            closed2.get()
            tailer3.isClosed()
    }
}
