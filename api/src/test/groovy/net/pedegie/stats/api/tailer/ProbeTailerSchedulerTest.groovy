package net.pedegie.stats.api.tailer

import net.pedegie.stats.api.queue.FileUtils
import net.pedegie.stats.api.queue.StatsQueue
import net.pedegie.stats.api.queue.TestQueueUtil
import spock.lang.Specification

import java.nio.file.Path
import java.nio.file.Paths

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
}
