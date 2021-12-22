package io.github.pedegie.stats.jmh;

import io.github.pedegie.stats.api.queue.Batching;
import io.github.pedegie.stats.api.queue.FileUtils;
import io.github.pedegie.stats.api.queue.QueueConfiguration;
import io.github.pedegie.stats.api.queue.StatsQueue;
import io.github.pedegie.stats.api.queue.WriteThreshold;
import lombok.SneakyThrows;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.github.pedegie.stats.jmh.BenchmarkUtils.randomPath;
import static io.github.pedegie.stats.jmh.BenchmarkUtils.runBenchmarkForQueue;

/*
Benchmark                                                      (queues)  (threads)  Mode  Cnt     Score     Error  Units
MultipleQueues.TestBenchmark3.ConcurrentLinkedQueuesBenchmark         4          4  avgt    4   126.189 ±   8.004  ms/op
MultipleQueues.TestBenchmark3.ConcurrentLinkedQueuesBenchmark         4          8  avgt    4   243.128 ±  15.189  ms/op
MultipleQueues.TestBenchmark3.ConcurrentLinkedQueuesBenchmark        16          4  avgt    4   387.985 ±  33.080  ms/op
MultipleQueues.TestBenchmark3.ConcurrentLinkedQueuesBenchmark        16          8  avgt    4   759.992 ±  71.315  ms/op
MultipleQueues.TestBenchmark3.ConcurrentLinkedQueuesBenchmark        32          4  avgt    4   718.364 ±  48.413  ms/op
MultipleQueues.TestBenchmark3.ConcurrentLinkedQueuesBenchmark        32          8  avgt    4  1418.596 ± 191.812  ms/op

MultipleQueues.TestBenchmark3.StatsQueuesBenchmark                    4          4  avgt    4   129.127 ±   3.614  ms/op
MultipleQueues.TestBenchmark3.StatsQueuesBenchmark                    4          8  avgt    4   240.777 ±  18.499  ms/op
MultipleQueues.TestBenchmark3.StatsQueuesBenchmark                   16          4  avgt    4   399.550 ±  36.386  ms/op
MultipleQueues.TestBenchmark3.StatsQueuesBenchmark                   16          8  avgt    4   771.666 ± 112.810  ms/op
MultipleQueues.TestBenchmark3.StatsQueuesBenchmark                   32          4  avgt    4   739.291 ±  61.835  ms/op
MultipleQueues.TestBenchmark3.StatsQueuesBenchmark                   32          8  avgt    4  1481.046 ± 339.950  ms/op

*/

public class MultipleQueues
{
    @Fork(value = 1)
    @Warmup(iterations = 5)
    @Measurement(iterations = 4)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode({Mode.AverageTime})
    @State(Scope.Benchmark)
    @Timeout(time = 120)
    public static class TestBenchmark3
    {
        @Benchmark
        public void StatsQueuesBenchmark(QueueConfiguration3 queueConfiguration)
        {
            runBenchmark(queueConfiguration.statsQueueBenchmarks);
        }

        @Benchmark
        public void ConcurrentLinkedQueuesBenchmark(QueueConfiguration3 queueConfiguration) throws InterruptedException
        {
            runBenchmark(queueConfiguration.concurrentQueueBenchmarks);
        }

        @SneakyThrows
        private void runBenchmark(List<Supplier<Void>> concurrentQueueBenchmarks)
        {
            List<Thread> threads = new ArrayList<>();
            for (Supplier<Void> benchmark : concurrentQueueBenchmarks)
            {
                Thread thread = new Thread(benchmark::get);
                threads.add(thread);
                thread.start();
            }

            for (Thread thread : threads)
            {
                thread.join();
            }
        }

        @State(Scope.Benchmark)
        public static class QueueConfiguration3
        {
            @Param({"4", "8"})
            public int threads;

            @Param({"4", "16", "32"})
            public int queues;

            List<Supplier<Void>> statsQueueBenchmarks = new ArrayList<>();
            List<Supplier<Void>> concurrentQueueBenchmarks = new ArrayList<>();
            List<StatsQueue<Integer>> statsQueues = new ArrayList<>();
            List<ExecutorService> producerPools = new ArrayList<>();
            List<ExecutorService> consumerPools = new ArrayList<>();

            @Setup(Level.Trial)
            public void setUp()
            {
                FileUtils.cleanDirectory(BenchmarkUtils.testQueuePath.getParent());

                for (int i = 0; i < queues; i++)
                {
                    var producerPool = Executors.newFixedThreadPool(threads, new BenchmarkUtils.NamedThreadFactory("producer_pool-%d"));
                    var consumerPool = Executors.newFixedThreadPool(threads, new BenchmarkUtils.NamedThreadFactory("consumer_pool-%d"));
                    producerPools.add(producerPool);
                    consumerPools.add(consumerPool);

                    var queueConfiguration = QueueConfiguration.builder()
                            .path(randomPath())
                            .mmapSize(Integer.MAX_VALUE)
                            .writeThreshold(WriteThreshold.minSizeDifference(2))
                            .batching(new Batching(20000, 5000))
                            .build();

                    var statsQueue = StatsQueue.queue(new ConcurrentLinkedQueue<Integer>(), queueConfiguration);
                    var concurrentLinkedQueue = new ConcurrentLinkedQueue<Integer>();

                    Supplier<Void> statsQueueBenchmark = runBenchmarkForQueue(statsQueue, threads, producerPool, consumerPool, 1000);
                    Supplier<Void> concurrentLinkedQueueBenchmark = runBenchmarkForQueue(concurrentLinkedQueue, threads, producerPool, consumerPool, 1000);

                    statsQueueBenchmarks.add(statsQueueBenchmark);
                    concurrentQueueBenchmarks.add(concurrentLinkedQueueBenchmark);
                    statsQueues.add(statsQueue);
                }
            }


            @TearDown(Level.Trial)
            public void teardownTrial()
            {
                producerPools.forEach(this::closePool);
                consumerPools.forEach(this::closePool);
                statsQueues.forEach(StatsQueue::close);

                statsQueueBenchmarks.clear();
                concurrentQueueBenchmarks.clear();
                statsQueues.clear();
                producerPools.clear();
                consumerPools.clear();
            }

            private void closePool(ExecutorService executorService)
            {
                executorService.shutdown();
                try
                {
                    executorService.awaitTermination(60, TimeUnit.SECONDS);
                } catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws RunnerException
    {

        Options options = new OptionsBuilder()
                .include(MultipleQueues.class.getSimpleName())
                .build();
        new Runner(options).run();
    }
}
