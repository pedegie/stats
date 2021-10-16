package net.pedegie.stats.jmh;

import net.pedegie.stats.api.queue.FileUtils;
import net.pedegie.stats.api.queue.StatsQueue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
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
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static net.pedegie.stats.jmh.Benchmark.runBenchmarkForQueue;

/*
Benchmark                                                                        (threads)  Mode  Cnt     Score      Error  Units
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                    1  avgt    4     5.895 ±    0.494  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                    2  avgt    4    21.882 ±   14.012  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                    4  avgt    4    34.899 ±    1.647  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                    8  avgt    4   159.042 ±    5.050  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                   16  avgt    4   388.343 ±  106.886  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                   32  avgt    4   790.953 ±  216.111  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                   64  avgt    4  1676.783 ±  335.230  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                  128  avgt    4  2287.866 ±   85.530  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.StatsQueueConcurrentLinkedQueue          1  avgt    4    14.575 ±    5.231  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.StatsQueueConcurrentLinkedQueue          2  avgt    4    36.946 ±    1.668  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.StatsQueueConcurrentLinkedQueue          4  avgt    4    73.907 ±    4.084  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.StatsQueueConcurrentLinkedQueue          8  avgt    4   190.110 ±   69.062  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.StatsQueueConcurrentLinkedQueue         16  avgt    4   414.898 ±  181.509  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.StatsQueueConcurrentLinkedQueue         32  avgt    4   870.168 ±  371.649  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.StatsQueueConcurrentLinkedQueue         64  avgt    4  1808.156 ±  563.902  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.StatsQueueConcurrentLinkedQueue        128  avgt    4  3853.431 ± 1105.405  ms/op

Process finished with exit code 0

*/


public class QueueStatsVsConcurrentLinkedQueue
{
    @Fork(value = 1)
    @Warmup(iterations = 5)
    @Measurement(iterations = 4)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode({Mode.AverageTime})
    @State(Scope.Benchmark)
    @Timeout(time = 120)
    public static class TestBenchmark
    {
        @Benchmark
        public void AStatsQueueConcurrentLinkedQueue(QueueConfiguration queueConfiguration)
        {
            queueConfiguration.statsQueueConcurrentLinkedQueueBenchmark.get();
        }

        @Benchmark
        public void ConcurrentLinkedQueue(QueueConfiguration queueConfiguration)
        {
            queueConfiguration.concurrentLinkedQueueBenchmark.get();
        }

        @State(Scope.Benchmark)
        public static class QueueConfiguration
        {
            private static final Path testQueuePath = Paths.get(System.getProperty("java.io.tmpdir"), "stats_queue", "stats_queue.log").toAbsolutePath();

            @Param({"1"})
            public int threads;

            Supplier<Void> statsQueueConcurrentLinkedQueueBenchmark;
            Supplier<Void> concurrentLinkedQueueBenchmark;

            ExecutorService producerThreadPool;
            ExecutorService consumerThreadPool;

            @Setup(Level.Trial)
            public void setUp()
            {
                System.out.println("STARTING POOL");
                producerThreadPool = Executors.newFixedThreadPool(threads, new net.pedegie.stats.jmh.Benchmark.NamedThreadFactory("producer_pool-%d"));
                consumerThreadPool = Executors.newFixedThreadPool(threads, new net.pedegie.stats.jmh.Benchmark.NamedThreadFactory("consumer_pool-%d"));
                FileUtils.cleanDirectory(testQueuePath.getParent());

                var queueConfiguration = net.pedegie.stats.api.queue.QueueConfiguration.builder()
                        .path(testQueuePath.getParent().resolve(Paths.get(testQueuePath.getFileName() + UUID.randomUUID().toString())))
                        .preTouch(true)
                        .unmapOnClose(true)
                        .mmapSize(Integer.MAX_VALUE)
                        .build();

                StatsQueue<Integer> statsQueue = StatsQueue.<Integer>builder()
                        .queue(new ConcurrentLinkedQueue<>())
                        .queueConfiguration(queueConfiguration)
                        .build();
                statsQueueConcurrentLinkedQueueBenchmark = runBenchmarkForQueue(statsQueue, threads, producerThreadPool, consumerThreadPool);
                concurrentLinkedQueueBenchmark = runBenchmarkForQueue(new ConcurrentLinkedQueue<>(), threads, producerThreadPool, consumerThreadPool);
            }


            @TearDown(Level.Trial)
            public void teardown()
            {
                producerThreadPool.shutdown();
                consumerThreadPool.shutdown();
                try
                {
                    producerThreadPool.awaitTermination(60, TimeUnit.SECONDS);
                    consumerThreadPool.awaitTermination(60, TimeUnit.SECONDS);

                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }

        }


        public static void main(String[] args) throws RunnerException
        {

            Options options = new OptionsBuilder()
                    .include(QueueStatsVsConcurrentLinkedQueue.class.getSimpleName())
                          .jvmArgs("-Xlog:codecache+sweep*=trace," +
                                  "class+unload," +
                                  "class+load," +
                                  "os+thread," +
                                  "safepoint," +
                                  "gc*," +
                                  "gc+stringdedup=debug," +
                                  "gc+ergo=trace," +
                                  "gc+age=trace," +
                                  "gc+phases=trace," +
                                  "gc+humongous=trace," +
                                  "jit+compilation=debug" +
                                  ":file=/tmp/app.log" +
                                  ":level,tags,time,uptime" +
                                  ":filesize=104857600,filecount=5")
                    .build();
            new Runner(options).run();
        }
    }


}
