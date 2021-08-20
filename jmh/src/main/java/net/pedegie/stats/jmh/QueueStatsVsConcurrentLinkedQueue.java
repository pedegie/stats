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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static net.pedegie.stats.jmh.Benchmark.runBenchmarkForQueue;

/*Benchmark                                                          (threads)  Mode  Cnt     Score      Error  Units
MPMCQueueStatsPerformanceTest.TestBenchmark.ConcurrentLinkedQueue          1  avgt    3  2713.791 ±   86.120  ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.ConcurrentLinkedQueue          2  avgt    3  2730.953 ±  112.431  ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.ConcurrentLinkedQueue          4  avgt    3  2742.108 ±   91.329  ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.ConcurrentLinkedQueue          8  avgt    3  2884.171 ± 2333.550  ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.ConcurrentLinkedQueue         16  avgt    3  2937.123 ±  633.752  ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.ConcurrentLinkedQueue         32  avgt    3  3103.091 ± 1886.084  ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.ConcurrentLinkedQueue         64  avgt    3  3201.386 ± 4283.620  ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.ConcurrentLinkedQueue        128  avgt    3  3811.099 ± 3760.723  ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.MPMCQueueStatsThreads          1  avgt    3  2724.187 ±   68.131  ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.MPMCQueueStatsThreads          2  avgt    3  2749.065 ±   24.299  ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.MPMCQueueStatsThreads          4  avgt    3  2755.208 ±   40.718  ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.MPMCQueueStatsThreads          8  avgt    3  2891.358 ± 2116.058  ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.MPMCQueueStatsThreads         16  avgt    3  2949.689 ±  657.005  ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.MPMCQueueStatsThreads         32  avgt    3  3061.542 ± 2517.398  ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.MPMCQueueStatsThreads         64  avgt    3  3077.744 ± 1597.093  ms/op
MPMCQueueStatsPerformanceTest.TestBenchmark.MPMCQueueStatsThreads        128  avgt    3  3791.936 ± 4803.237  ms/op
*/


public class QueueStatsVsConcurrentLinkedQueue
{
    @Fork(value = 1)
    @Warmup(iterations = 2)
    @Measurement(iterations = 3)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode({Mode.AverageTime})
    @State(Scope.Benchmark)
    @Timeout(time = 120)
    public static class TestBenchmark
    {
        @Benchmark
        public void MPMCQueueStatsConcurrentLinkedQueue(QueueConfiguration queueConfiguration)
        {
            queueConfiguration.mpmcQueueStatsConcurrentLinkedQueueBenchmark.get();
        }

        @Benchmark
        public void ConcurrentLinkedQueue(QueueConfiguration queueConfiguration)
        {
            queueConfiguration.concurrentLinkedQueueBenchmark.get();
        }

        @State(Scope.Benchmark)
        public static class QueueConfiguration
        {
            private static final Path testQueuePath = Paths.get(System.getProperty("java.io.tmpdir"), "stats_queue").toAbsolutePath();

            @Param({"1", "2", "4", "8", "16", "32", "64", "128"})
            public int threads;

            Supplier<Void> mpmcQueueStatsConcurrentLinkedQueueBenchmark;
            Supplier<Void> concurrentLinkedQueueBenchmark;

            @Setup(Level.Trial)
            public void setUp()
            {


                var dir = new File(testQueuePath.toString());
                File[] files = dir.listFiles();
                if (files != null)
                {
                    for (File file : files)
                        if (!file.isDirectory())
                            file.delete();
                }

                MPMCQueueStats<Integer> mpmcQueueStatsConcurrentLinkedQueue = MPMCQueueStats.<Integer>builder()
                        .queue(new ConcurrentLinkedQueue<>())
                        .fileName(testQueuePath)
                        .build();
                mpmcQueueStatsConcurrentLinkedQueueBenchmark = runBenchmarkForQueue(mpmcQueueStatsConcurrentLinkedQueue, threads);
                concurrentLinkedQueueBenchmark = runBenchmarkForQueue(new ConcurrentLinkedQueue<>(), threads);
            }
        }


        public static void main(String[] args) throws RunnerException
        {

            Options options = new OptionsBuilder()
                    .include(TestBenchmark.class.getSimpleName())
              /*      .jvmArgs("--enable-preview", "-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintAssembly",
                            "-XX:+LogCompilation", "-XX:PrintAssemblyOptions=amd64",
                            "-XX:LogFile=/home/kacper/projects/pedegie/stats/jmh/target/jit_logs.txt")*/
                    .build();
            new Runner(options).run();
        }
    }


}
