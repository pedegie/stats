package net.pedegie.stats.jmh;

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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/*
Benchmark                                                                Mode  Cnt    Score     Error  Units
QueueStatsVsLinkedList.TestBenchmark2.AStatsQueueLinkedList              avgt    4  487.371 ±  29.007  us/op
QueueStatsVsLinkedList.TestBenchmark2.AStatsQueueLinkedListDisabledSync  avgt    4  372.918 ± 121.043  us/op
QueueStatsVsLinkedList.TestBenchmark2.LinkedList                         avgt    4   62.174 ±   0.161  us/op
*/

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
        public void AStatsQueueLinkedList(QueueConfiguration2 queueConfiguration)
        {
            queueConfiguration.statsQueueLinkedListBenchmark.get();
        }

        @Benchmark
        public void AStatsQueueLinkedListDisabledSync(QueueConfiguration2 queueConfiguration)
        {
            queueConfiguration.statsQueueLinkedListDisabledSyncBenchmark.get();
        }

        @Benchmark
        public void LinkedList(QueueConfiguration2 queueConfiguration)
        {
            queueConfiguration.LinkedListBenchmark.get();
        }

        @State(Scope.Benchmark)
        public static class QueueConfiguration2
        {
            private static final Path testQueuePath = Paths.get(System.getProperty("java.io.tmpdir"), "stats_queue", "stats_queue.log").toAbsolutePath();

            Supplier<Void> statsQueueLinkedListDisabledSyncBenchmark;
            Supplier<Void> statsQueueLinkedListBenchmark;
            Supplier<Void> LinkedListBenchmark;

            StatsQueue<Integer> statsQueueDisabledSync;
            StatsQueue<Integer> statsQueue;

            @Setup(Level.Trial)
            public void setUp()
            {
                FileUtils.cleanDirectory(testQueuePath.getParent());
                var queueConfiguration = QueueConfiguration.builder()
                        .path(randomPath())
                        .preTouch(true)
                        .mmapSize(4L << 30)
                        .batchSize(20000)
                        .disableSynchronization(true)
                        .build();

                statsQueueDisabledSync = StatsQueue.<Integer>builder()
                        .queue(new LinkedList<>())
                        .queueConfiguration(queueConfiguration)
                        .build();
                statsQueue = StatsQueue.<Integer>builder()
                        .queue(new LinkedList<>())
                        .queueConfiguration(queueConfiguration
                                .withPath(randomPath())
                                .withDisableSynchronization(false))
                        .build();

                statsQueueLinkedListDisabledSyncBenchmark = benchmarkFor(statsQueueDisabledSync);
                statsQueueLinkedListBenchmark = benchmarkFor(statsQueue);
                LinkedListBenchmark = benchmarkFor(new LinkedList<>());

            }

            private Path randomPath()
            {
                return testQueuePath.getParent().resolve(Paths.get(testQueuePath.getFileName() + UUID.randomUUID().toString()));
            }


            @TearDown(Level.Trial)
            public void teardownTrial()
            {
                statsQueueDisabledSync.close();
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
