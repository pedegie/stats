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
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.AStatsQueueConcurrentLinkedQueue          1  avgt    4     6.858 ±   0.255  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.AStatsQueueConcurrentLinkedQueue          2  avgt    4    27.704 ±   0.181  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.AStatsQueueConcurrentLinkedQueue          4  avgt    4    79.120 ±   0.176  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.AStatsQueueConcurrentLinkedQueue          8  avgt    4   190.710 ±   5.323  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.AStatsQueueConcurrentLinkedQueue         16  avgt    4   398.185 ±   8.719  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.AStatsQueueConcurrentLinkedQueue         32  avgt    4   806.083 ±  12.033  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.AStatsQueueConcurrentLinkedQueue         64  avgt    4  1620.848 ±  21.684  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.AStatsQueueConcurrentLinkedQueue        128  avgt    4  3315.942 ± 143.496  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                     1  avgt    4     4.787 ±   0.122  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                     2  avgt    4    22.304 ±   1.239  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                     4  avgt    4    60.345 ±   8.981  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                     8  avgt    4   168.159 ±   9.305  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                    16  avgt    4   370.877 ±   9.365  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                    32  avgt    4   778.762 ±  68.289  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                    64  avgt    4  1578.408 ±  69.265  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                   128  avgt    4  2059.477 ± 104.889  ms/op
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

            @Param({"1", "2", "4", "8", "16", "32", "64", "128"})
            public int threads;

            Supplier<Void> statsQueueConcurrentLinkedQueueBenchmark;
            Supplier<Void> concurrentLinkedQueueBenchmark;

            ExecutorService producerThreadPool;
            ExecutorService consumerThreadPool;
            StatsQueue<Integer> statsQueue;

            @Setup(Level.Trial)
            public void setUp()
            {
                producerThreadPool = Executors.newFixedThreadPool(threads, new net.pedegie.stats.jmh.Benchmark.NamedThreadFactory("producer_pool-%d"));
                consumerThreadPool = Executors.newFixedThreadPool(threads, new net.pedegie.stats.jmh.Benchmark.NamedThreadFactory("consumer_pool-%d"));
                FileUtils.cleanDirectory(testQueuePath.getParent());

                var queueConfiguration = net.pedegie.stats.api.queue.QueueConfiguration.builder()
                        .path(testQueuePath.getParent().resolve(Paths.get(testQueuePath.getFileName() + UUID.randomUUID().toString())))
                        .preTouch(true)
                        .mmapSize(Integer.MAX_VALUE)
                        .build();

                statsQueue = StatsQueue.<Integer>builder()
                        .queue(new ConcurrentLinkedQueue<>())
                        .queueConfiguration(queueConfiguration)
                        .build();
                statsQueueConcurrentLinkedQueueBenchmark = runBenchmarkForQueue(statsQueue, threads, producerThreadPool, consumerThreadPool);
                concurrentLinkedQueueBenchmark = runBenchmarkForQueue(new ConcurrentLinkedQueue<>(), threads, producerThreadPool, consumerThreadPool);
            }


            @TearDown(Level.Trial)
            public void teardownTrial()
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
                statsQueue.close();
            }
        }


        public static void main(String[] args) throws RunnerException
        {

            Options options = new OptionsBuilder()
                    .include(QueueStatsVsConcurrentLinkedQueue.class.getSimpleName())
                    /*              .jvmArgs("-Xlog:codecache+sweep*=trace," +
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
                                          ":filesize=104857600,filecount=5")*/
                    .build();
            new Runner(options).run();
        }
    }


}
