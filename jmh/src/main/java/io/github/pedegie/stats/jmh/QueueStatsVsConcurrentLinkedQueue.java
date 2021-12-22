package io.github.pedegie.stats.jmh;

import io.github.pedegie.stats.api.queue.FileUtils;
import io.github.pedegie.stats.api.queue.StatsQueue;
import io.github.pedegie.stats.api.queue.WriteThreshold;
import io.github.pedegie.stats.api.queue.Batching;
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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.github.pedegie.stats.jmh.BenchmarkUtils.randomPath;
import static io.github.pedegie.stats.jmh.BenchmarkUtils.runBenchmarkForQueue;

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
        public void StatsQueueConcurrentLinkedQueue(QueueConfiguration queueConfiguration)
        {
            queueConfiguration.statsQueueConcurrentLinkedQueueBenchmark.get();
        }

        @Benchmark
        public void ConcurrentLinkedQueue(QueueConfiguration queueConfiguration)
        {
            queueConfiguration.concurrentLinkedQueueBenchmark.get();
        }

        @Benchmark
        public void StatsQueueConcurrentLinkedQueue1usDelay(QueueConfiguration queueConfiguration)
        {
            queueConfiguration.statsQueueConcurrentLinkedQueue1usDelayBenchmark.get();
        }

        @Benchmark
        public void ConcurrentLinkedQueue1usDelay(QueueConfiguration queueConfiguration)
        {
            queueConfiguration.concurrentLinkedQueue1usDelayBenchmark.get();
        }

        @State(Scope.Benchmark)
        public static class QueueConfiguration
        {
            @Param({"1", "2", "4", "8", "16", "32", "64", "128"})
            public int threads;

            Supplier<Void> statsQueueConcurrentLinkedQueueBenchmark;
            Supplier<Void> concurrentLinkedQueueBenchmark;
            Supplier<Void> statsQueueConcurrentLinkedQueue1usDelayBenchmark;
            Supplier<Void> concurrentLinkedQueue1usDelayBenchmark;

            ExecutorService producerThreadPool;
            ExecutorService consumerThreadPool;
            StatsQueue<Integer> statsQueue;

            @Setup(Level.Trial)
            public void setUp()
            {
                producerThreadPool = Executors.newFixedThreadPool(threads, new BenchmarkUtils.NamedThreadFactory("producer_pool-%d"));
                consumerThreadPool = Executors.newFixedThreadPool(threads, new BenchmarkUtils.NamedThreadFactory("consumer_pool-%d"));
                FileUtils.cleanDirectory(BenchmarkUtils.testQueuePath.getParent());
                var queueConfiguration = io.github.pedegie.stats.api.queue.QueueConfiguration.builder()
                        .path(randomPath())
                        .preTouch(true)
                        .mmapSize(Integer.MAX_VALUE)
                        .writeThreshold(WriteThreshold.minSizeDifference(2))
                        .batching(new Batching(20000, 5000))
                        .build();

                statsQueue = StatsQueue.queue(new ConcurrentLinkedQueue<>(), queueConfiguration);
                statsQueueConcurrentLinkedQueueBenchmark = runBenchmarkForQueue(statsQueue, threads, producerThreadPool, consumerThreadPool);
                concurrentLinkedQueueBenchmark = runBenchmarkForQueue(new ConcurrentLinkedQueue<>(), threads, producerThreadPool, consumerThreadPool);
                statsQueueConcurrentLinkedQueue1usDelayBenchmark = runBenchmarkForQueue(statsQueue, threads, producerThreadPool, consumerThreadPool, 1000);
                concurrentLinkedQueue1usDelayBenchmark = runBenchmarkForQueue(new ConcurrentLinkedQueue<>(), threads, producerThreadPool, consumerThreadPool, 1000);
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
