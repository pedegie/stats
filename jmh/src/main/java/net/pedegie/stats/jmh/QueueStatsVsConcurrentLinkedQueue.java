package net.pedegie.stats.jmh;

import net.pedegie.stats.api.queue.LogFileConfiguration;
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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static net.pedegie.stats.jmh.Benchmark.runBenchmarkForQueue;

/*
Benchmark                                                                            (threads)  Mode  Cnt     Score      Error  Units
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                        1  avgt    3     6.643 ±    1.001  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                        2  avgt    3    24.140 ±   26.591  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                        4  avgt    3    37.187 ±   79.630  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                        8  avgt    3   109.204 ±  584.272  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                       16  avgt    3   352.127 ±  349.541  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                       32  avgt    3   652.617 ± 2484.780  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                       64  avgt    3  1442.987 ± 1378.706  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.ConcurrentLinkedQueue                      128  avgt    3  2909.990 ± 3230.944  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.MPMCQueueStatsConcurrentLinkedQueue          1  avgt    3    17.692 ±   21.788  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.MPMCQueueStatsConcurrentLinkedQueue          2  avgt    3    39.007 ±   14.640  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.MPMCQueueStatsConcurrentLinkedQueue          4  avgt    3    74.890 ±   85.103  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.MPMCQueueStatsConcurrentLinkedQueue          8  avgt    3   189.647 ±   65.109  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.MPMCQueueStatsConcurrentLinkedQueue         16  avgt    3   415.680 ±   53.561  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.MPMCQueueStatsConcurrentLinkedQueue         32  avgt    3   822.984 ±  690.411  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.MPMCQueueStatsConcurrentLinkedQueue         64  avgt    3  1774.190 ±  201.418  ms/op
QueueStatsVsConcurrentLinkedQueue.TestBenchmark.MPMCQueueStatsConcurrentLinkedQueue        128  avgt    3  4035.729 ± 2351.217  ms/op
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
                var logFileConfiguration = LogFileConfiguration.builder()
                        .path(testQueuePath)
                        .override(true)
                        .mmapSize(Integer.MAX_VALUE)
                        .build();

                MPMCQueueStats<Integer> mpmcQueueStatsConcurrentLinkedQueue = MPMCQueueStats.<Integer>builder()
                        .queue(new ConcurrentLinkedQueue<>())
                        .logFileConfiguration(logFileConfiguration)
                        .build();
                mpmcQueueStatsConcurrentLinkedQueueBenchmark = runBenchmarkForQueue(mpmcQueueStatsConcurrentLinkedQueue, threads);
                concurrentLinkedQueueBenchmark = runBenchmarkForQueue(new ConcurrentLinkedQueue<>(), threads);
            }
        }


        public static void main(String[] args) throws RunnerException
        {

            Options options = new OptionsBuilder()
                    .include(QueueStatsVsConcurrentLinkedQueue.class.getSimpleName())
                    /*      .jvmArgs("--enable-preview", "-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintAssembly",
                                  "-XX:+LogCompilation", "-XX:PrintAssemblyOptions=amd64",
                                  "-XX:LogFile=jit_logs.txt")*/
                    .build();
            new Runner(options).run();
        }
    }


}
