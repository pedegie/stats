package net.pedegie.stats.jmh;

import net.pedegie.stats.api.queue.LogFileConfiguration;
import net.pedegie.stats.api.queue.MPMCQueueStats;
import org.jctools.queues.SpscArrayQueue;
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
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static net.pedegie.stats.jmh.Benchmark.runBenchmarkForQueue;

/*
Benchmark                                                               Mode  Cnt     Score    Error  Units
QueueStatsVsSpscArrayQueue.TestBenchmark2.MPMCQueueStatsSpscArrayQueue  avgt    3  2708.351 ± 50.284  ms/op
QueueStatsVsSpscArrayQueue.TestBenchmark2.spscArrayQueue                avgt    3  2704.034 ±  7.238  ms/op
*/

public class QueueStatsVsSpscArrayQueue
{
    @Fork(value = 1)
    @Warmup(iterations = 2)
    @Measurement(iterations = 3)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode({Mode.AverageTime})
    @State(Scope.Benchmark)
    @Timeout(time = 120)
    public static class TestBenchmark2
    {

        @Benchmark
        public void spscArrayQueue(QueueConfiguration queueConfiguration)
        {
            queueConfiguration.spscArrayQueueBenchmark.get();
        }

        @Benchmark
        public void MPMCQueueStatsSpscArrayQueue(QueueConfiguration queueConfiguration)
        {
            queueConfiguration.mpmcQueueStatsSpscArrayQueueBenchmark.get();
        }

        @State(Scope.Benchmark)
        public static class QueueConfiguration
        {
            private static final Path testQueuePath = Paths.get(System.getProperty("java.io.tmpdir"), "stats_queue").toAbsolutePath();

            Supplier<Void> mpmcQueueStatsSpscArrayQueueBenchmark;
            Supplier<Void> spscArrayQueueBenchmark;

            @Setup(Level.Trial)
            public void setUp()
            {
                var logFileConfiguration = LogFileConfiguration.builder()
                        .path(testQueuePath)
                        .override(true)
                        .build();

                MPMCQueueStats<Integer> mpmcQueueStatsSpscArrayQueue = MPMCQueueStats.<Integer>builder()
                        .queue(new SpscArrayQueue<>(50000))
                        .logFileConfiguration(logFileConfiguration)
                        .build();
                mpmcQueueStatsSpscArrayQueueBenchmark = runBenchmarkForQueue(mpmcQueueStatsSpscArrayQueue, 1);
                spscArrayQueueBenchmark = runBenchmarkForQueue(new SpscArrayQueue<>(50000), 1);

            }
        }

        public static void main(String[] args) throws RunnerException
        {

            Options options = new OptionsBuilder()
                    .include(TestBenchmark2.class.getSimpleName())
                    /*              .jvmArgs("--enable-preview", "-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintAssembly",
                                          "-XX:+LogCompilation", "-XX:PrintAssemblyOptions=amd64",
                                          "-XX:LogFile=jit_logs.txt")*/
                    .build();
            new Runner(options).run();
        }
    }
}
