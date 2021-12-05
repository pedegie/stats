package net.pedegie.stats.jmh;

import net.pedegie.stats.api.queue.Batching;
import net.pedegie.stats.api.queue.BusyWaiter;
import net.pedegie.stats.api.queue.FileUtils;
import net.pedegie.stats.api.queue.QueueConfiguration;
import net.pedegie.stats.api.queue.StatsQueue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static net.pedegie.stats.jmh.Benchmark.randomPath;

public class QueueStatsVsLinkedList
{
    @Fork(value = 1)
    @Warmup(iterations = 5, time = 5)
    @Measurement(iterations = 4, time = 5)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @BenchmarkMode({Mode.AverageTime})
    @State(Scope.Benchmark)
    @Timeout(time = 120)
    public static class TestBenchmark2
    {
        @Benchmark
        public void StatsQueueLinkedList1usDelay(QueueConfiguration2 queueConfiguration)
        {
            queueConfiguration.statsQueueLinkedList1usDelayBenchmark.get();
        }

        @Benchmark
        public void LinkedList1usDelay(QueueConfiguration2 queueConfiguration)
        {
            queueConfiguration.linkedList1usDelayBenchmark.get();
        }

        @Benchmark
        public void StatsQueueLinkedListNoDelay(QueueConfiguration2 queueConfiguration)
        {
            queueConfiguration.statsQueueLinkedListNoDelayBenchmark.get();
        }

        @Benchmark
        public void LinkedListNoDelay(QueueConfiguration2 queueConfiguration)
        {
            queueConfiguration.linkedListNoDelayBenchmark.get();
        }

        @State(Scope.Benchmark)
        public static class QueueConfiguration2
        {
            Supplier<Void> statsQueueLinkedList1usDelayBenchmark;
            Supplier<Void> linkedList1usDelayBenchmark;
            Supplier<Void> statsQueueLinkedListNoDelayBenchmark;
            Supplier<Void> linkedListNoDelayBenchmark;

            StatsQueue<Integer> statsQueue;

            @Setup(Level.Trial)
            public void setUp()
            {
                FileUtils.cleanDirectory(net.pedegie.stats.jmh.Benchmark.testQueuePath);
                var queueConfiguration = QueueConfiguration.builder()
                        .path(randomPath())
                        .preTouch(true)
                        .mmapSize(4L << 30)
                        .batching(new Batching(20000, Long.MAX_VALUE))
                        .disableSynchronization(true)
                        .build();

                statsQueue = StatsQueue.queue(new LinkedList<>(), queueConfiguration);
                statsQueueLinkedList1usDelayBenchmark = benchmarkFor(statsQueue, 1000);
                linkedList1usDelayBenchmark = benchmarkFor(new LinkedList<>(), 1000);
                statsQueueLinkedListNoDelayBenchmark = benchmarkFor(statsQueue);
                linkedListNoDelayBenchmark = benchmarkFor(new LinkedList<>());

            }

            @TearDown(Level.Trial)
            public void teardownTrial()
            {
                statsQueue.close();
            }

            private Supplier<Void> benchmarkFor(Queue<Integer> queue)
            {
                return () ->
                {
                    for (int i = 0; i < 5_000; i++)
                        queue.add(i);

                    for (int i = 0; i < 5_000; i++)
                        queue.poll();

                    return null;
                };
            }

            private Supplier<Void> benchmarkFor(Queue<Integer> queue, long nanosDelay)
            {
                return () ->
                {
                    for (int i = 0; i < 5_000; i++)
                    {
                        queue.add(i);
                        BusyWaiter.busyWaitNanos(nanosDelay);
                    }

                    for (int i = 0; i < 5_000; i++)
                        queue.poll();

                    return null;
                };
            }
        }
    }

    public static void main(String[] args) throws RunnerException
    {

        Options options = new OptionsBuilder()
                .include(QueueStatsVsLinkedList.class.getSimpleName())
                .build();
        new Runner(options).run();
    }
}
