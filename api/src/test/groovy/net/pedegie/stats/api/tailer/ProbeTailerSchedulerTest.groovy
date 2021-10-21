package net.pedegie.stats.api.tailer

import net.pedegie.stats.api.queue.BusyWaiter
import net.pedegie.stats.api.queue.FileUtils
import net.pedegie.stats.api.queue.TestQueueUtil
import spock.lang.Specification

import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicInteger

import static net.pedegie.stats.api.tailer.ProbeTailerTest.writeElementsTo

class ProbeTailerSchedulerTest extends Specification
{
    def setup()
    {
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

    def "should dispatch and concurrently process all handlers on all threads"()
    {
        given: "write probes to 4 files"
            Path path1 = Paths.get(TestQueueUtil.PATH.toString() + "_1")
            Path path2 = Paths.get(TestQueueUtil.PATH.toString() + "_2")
            Path path3 = Paths.get(TestQueueUtil.PATH.toString() + "_3")
            Path path4 = Paths.get(TestQueueUtil.PATH.toString() + "_4")
            writeElementsTo(10, path1)
            writeElementsTo(15, path2)
            writeElementsTo(20, path3)
            writeElementsTo(25, path4)
        and: "4 tailers which counts read(n) invocations, to ensure there is no starvation"
            CountingInvocationsTailer tailer1 = new CountingInvocationsTailer(TailerFactory.tailerFor(path1))
            CountingInvocationsTailer tailer2 = new CountingInvocationsTailer(TailerFactory.tailerFor(path2))
            CountingInvocationsTailer tailer3 = new CountingInvocationsTailer(TailerFactory.tailerFor(path3))
            CountingInvocationsTailer tailer4 = new CountingInvocationsTailer(TailerFactory.tailerFor(path4))
        and: "scheduler with 2 threads and 5 probes on single read"
            ProbeTailerScheduler scheduler = ProbeTailerScheduler.create(2, 5)
        when: "schedule all tailers"
            scheduler.addTailer(tailer1)
            scheduler.addTailer(tailer2)
            scheduler.addTailer(tailer3)
            scheduler.addTailer(tailer4)
        then: "each tailer should read all probes"
            waitUntilRead(tailer1, 10)
            waitUntilRead(tailer2, 15)
            waitUntilRead(tailer3, 20)
            waitUntilRead(tailer4, 25)
        and: "tailers were fairly scheduled"
            tailer1.invoked.size() == 2
            tailer2.invoked.size() == 3
            tailer3.invoked.size() == 4
            tailer4.invoked.size() == 5
            invocationsNotHappenedOneByOne(tailer1)
            invocationsNotHappenedOneByOne(tailer2)
            invocationsNotHappenedOneByOne(tailer3)
            invocationsNotHappenedOneByOne(tailer4)
        cleanup:
            scheduler.close()
    }

    static void waitUntilRead(CountingInvocationsTailer tailer, int requiredElements)
    {
        BusyWaiter.busyWait({ tailer.readElements >= requiredElements }, "Waiting until tailer read all elements " + requiredElements)
    }

    boolean invocationsNotHappenedOneByOne(CountingInvocationsTailer invocations)
    {
        invocations.invoked.removeLast()
        if(invocations.invoked.isEmpty())
            return true

        boolean notOneByOne = true

        Iterator<Integer> elems = invocations.invoked.iterator()
        int previousElement = elems.next()
        while(elems.hasNext() && notOneByOne)
        {
            int nextElement = elems.next()
            notOneByOne =  nextElement - previousElement > 1
            previousElement = nextElement
        }

        return notOneByOne
    }

    private static class CountingInvocationsTailer implements ProbeTailer
    {
        private static final AtomicInteger invocationCounter = new AtomicInteger(0)
        List<Integer> invoked = new ArrayList<>(10)
        volatile int readElements = 0

        private final ProbeTailer originalTailer

        CountingInvocationsTailer(ProbeTailer probeTailer)
        {
            this.originalTailer = probeTailer
        }

        @Override
        boolean read(long amount)
        {
            boolean readAll = originalTailer.read(amount)
            if (readAll)
            {
                invoked.add(invocationCounter.incrementAndGet())
            }
            readElements += amount
            return readAll
        }

        @Override
        void read()
        {
            originalTailer.read()
        }

        @Override
        void readFromStart()
        {
            originalTailer.readFromStart()
        }

        @Override
        long probes()
        {
            return originalTailer.probes()
        }

        @Override
        boolean isClosed()
        {
            return originalTailer.isClosed()
        }

        @Override
        void close()
        {
            originalTailer.close()
        }
    }
}
