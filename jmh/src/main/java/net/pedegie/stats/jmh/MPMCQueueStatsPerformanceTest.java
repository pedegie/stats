package net.pedegie.stats.jmh;

import net.pedegie.stats.api.queue.MPMCQueueStats;
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
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import java.util.stream.IntStream;


/*Benchmark                                                          (threads)  Mode  Cnt      Score   Error  Units
MPMCQueueStatsPerformanceTest.TestBenchmark.ArrayBlockingQueue            16  avgt        4345.183          ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.ArrayBlockingQueue            32  avgt        4468.968          ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.ArrayBlockingQueue            64  avgt        4934.040          ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.ArrayBlockingQueue           128  avgt        5055.858          ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.ConcurrentLinkedQueue         16  avgt        4246.027          ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.ConcurrentLinkedQueue         32  avgt        4389.823          ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.ConcurrentLinkedQueue         64  avgt        5439.647          ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.ConcurrentLinkedQueue        128  avgt        9069.135          ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.MPMCQueueStatsThreads         16  avgt        4705.416          ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.MPMCQueueStatsThreads         32  avgt       10031.043          ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.MPMCQueueStatsThreads         64  avgt       10026.469          ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.MPMCQueueStatsThreads        128  avgt       10195.592          ms/op*/


public class MPMCQueueStatsPerformanceTest
{
    @Fork(value = 1)
    @Warmup(iterations = 2, time = 5)
    @Measurement(iterations = 5, time = 5)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode({Mode.AverageTime})
    @State(Scope.Benchmark)
    @Timeout(time = 10)
    public static class TestBenchmark
    {

        static
        {
            long pid = ProcessHandle.current().pid();
            System.out.println("PID: "+ pid);
        }
        @Benchmark
        public void MPMCQueueStatsThreads(QueueConfiguration queueConfiguration)
        {
            queueConfiguration.mpmcQueueStatsBenchmark.get();
        }

        @Benchmark
        public void ConcurrentLinkedQueue(QueueConfiguration queueConfiguration)
        {
            queueConfiguration.concurrentLinkedQueueBenchmark.get();
        }

        @Benchmark
        public void ArrayBlockingQueue(QueueConfiguration queueConfiguration)
        {
            queueConfiguration.arrayBlockingQueueBenchmark.get();
        }


        @State(Scope.Benchmark)
        public static class QueueConfiguration
        {
            private static final Path testQueuePath = Paths.get(System.getProperty("java.io.tmpdir"), "stats_queue").toAbsolutePath();

            @Param({/*"1", "2", "4",*/"8", "16", "32",  "64", "128"})
            public int threads;

            Supplier<Void> mpmcQueueStatsBenchmark;
            Supplier<Void> concurrentLinkedQueueBenchmark;
            Supplier<Void> arrayBlockingQueueBenchmark;

            @Setup(Level.Trial)
            public void setUp()
            {
                MPMCQueueStats<Integer> mpmcQueueStats;
                ConcurrentLinkedQueue<Integer> concurrentLinkedQueue;
                ArrayBlockingQueue<Integer> arrayBlockingQueue;

                var dir = new File(testQueuePath.toString());
                File[] files = dir.listFiles();
                if (files != null)
                {
                    for (File file : files)
                        if (!file.isDirectory())
                            file.delete();
                }

                mpmcQueueStats = MPMCQueueStats.<Integer>builder()
                        .queue(new ConcurrentLinkedQueue<>())
                        .fileName(testQueuePath)
                        .build();

                concurrentLinkedQueue = new ConcurrentLinkedQueue<>();
                arrayBlockingQueue = new ArrayBlockingQueue<>(1024 << 4);


                mpmcQueueStatsBenchmark = runBenchmarkForQueue(mpmcQueueStats, threads);
                concurrentLinkedQueueBenchmark = runBenchmarkForQueue(concurrentLinkedQueue, threads);
                arrayBlockingQueueBenchmark = runBenchmarkForQueue(arrayBlockingQueue, threads);

            }
        }

        private static Supplier<Void> runBenchmarkForQueue(Queue<Integer> queue, int threads)
        {
            Runnable producer = () ->
            {
                for (int i = 1; i <= 4000; i++)
                {
                    LockSupport.parkNanos(1000000); // 1 milli
                    if (i % 40 == 0)
                    {
                        queue.add(i);
                    } else
                    {
                        queue.offer(i);
                    }
                }

                IntStream.range(0, threads).forEach(i -> queue.add(-1));
            };
            Runnable consumer = () ->
            {
                List<Integer> blackHole = new ArrayList<>(1024 << 5);
                while (true)
                {
                    Integer element = queue.poll();
                    if (element != null)
                    {
                        if (element == -1)
                        {
                            break;
                        }
                        blackHole.add(element);
                    }
                }
            };

            return () ->
            {
                var producerThreadPool = Executors.newFixedThreadPool(threads);
                var consumerThreadPool = Executors.newFixedThreadPool(threads);

                List<CompletableFuture<?>> futures = new ArrayList<>(threads * 2);
                IntStream.range(0, threads).forEach(index ->
                {
                    futures.add(CompletableFuture.supplyAsync(() ->
                    {
                        producer.run();
                        return null;
                    }, producerThreadPool));
                    futures.add(CompletableFuture.supplyAsync(() ->
                    {
                        consumer.run();
                        return null;
                    }, consumerThreadPool));
                });

                producerThreadPool.shutdown();
                consumerThreadPool.shutdown();
                try
                {
                    producerThreadPool.awaitTermination(5, TimeUnit.SECONDS);
                    consumerThreadPool.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
                return null;
            };
        }

        public static void main(String[] args) throws RunnerException
        {

            Options options = new OptionsBuilder()
                    .include(TestBenchmark.class.getSimpleName())
                    .jvmArgs("--enable-preview", "-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintAssembly", "-XX:+LogCompilation", "-XX:PrintAssemblyOptions=amd64")
                    .build();
            new Runner(options).run();
        }
    }
}
