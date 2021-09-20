package net.pedegie.stats.jmh;

import net.pedegie.stats.api.queue.FileUtils;
import net.pedegie.stats.api.queue.StatsQueue;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static net.pedegie.stats.jmh.Benchmark.runBenchmarkForQueue;

/*
Benchmark                                                           Mode  Cnt   Score   Error  Units
QueueStatsVsSpscArrayQueue.TestBenchmark2.StatsQueueSpscArrayQueue  avgt    4  11.598 ± 7.451  ms/op
QueueStatsVsSpscArrayQueue.TestBenchmark2.spscArrayQueue            avgt    4   2.082 ± 0.361  ms/op
*/

public class QueueStatsVsSpscArrayQueue
{
    @Fork(value = 1)
    @Warmup(iterations = 5)
    @Measurement(iterations = 4)
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
        public void StatsQueueSpscArrayQueue(QueueConfiguration queueConfiguration)
        {
            queueConfiguration.statsQueueSpscArrayQueueBenchmark.get();
        }

        @State(Scope.Benchmark)
        public static class QueueConfiguration
        {
            private static final Path testQueuePath = Paths.get(System.getProperty("java.io.tmpdir"), "stats_queue", "stats_queue.log").toAbsolutePath();

            Supplier<Void> statsQueueSpscArrayQueueBenchmark;
            Supplier<Void> spscArrayQueueBenchmark;

            @Setup(Level.Trial)
            public void setUp()
            {
                FileUtils.cleanDirectory(testQueuePath.getParent());

                var queueConfiguration = net.pedegie.stats.api.queue.QueueConfiguration.builder()
                        .path(testQueuePath.getParent().resolve(Paths.get(testQueuePath.getFileName() + UUID.randomUUID().toString())))
                        .mmapSize(Integer.MAX_VALUE)
                        .build();

                StatsQueue<Integer> queue = StatsQueue.<Integer>builder()
                        .queue(new SpscArrayQueue<>(50000))
                        .queueConfiguration(queueConfiguration)
                        .build();
                statsQueueSpscArrayQueueBenchmark = runBenchmarkForQueue(queue, 1);
                spscArrayQueueBenchmark = runBenchmarkForQueue(new SpscArrayQueue<>(50000), 1);

            }
        }

        public static void main(String[] args) throws RunnerException
        {

            Options options = new OptionsBuilder()
                    .include(QueueStatsVsSpscArrayQueue.class.getSimpleName())
                    /*              .jvmArgs("--enable-preview", "-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintAssembly",
                                          "-XX:+LogCompilation", "-XX:PrintAssemblyOptions=amd64",
                                          "-XX:LogFile=jit_logs.txt")*/
                    .build();
            new Runner(options).run();
        }
    }
}
