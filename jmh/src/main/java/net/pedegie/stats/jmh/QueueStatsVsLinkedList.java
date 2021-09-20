package net.pedegie.stats.jmh;

import lombok.SneakyThrows;
import net.pedegie.stats.api.queue.FileUtils;
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
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
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

/*
Benchmark                                                                          Mode  Cnt  Score   Error  Units
QueueStatsVsLinkedList.TestBenchmark3.LinkedList                                   avgt    5  0.783 ± 0.017  ms/op
QueueStatsVsLinkedList.TestBenchmark3.StatsQueueDisabledSynchronizationLinkedList  avgt    5  5.158 ± 1.283  ms/op
QueueStatsVsLinkedList.TestBenchmark3.StatsQueueLinkedList                         avgt    5  5.524 ± 1.574  ms/op
*/

public class QueueStatsVsLinkedList
{
    @Fork(value = 1)
    @Warmup(iterations = 5)
    @Measurement(iterations = 5)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode({Mode.AverageTime})
    @State(Scope.Benchmark)
    @Timeout(time = 120)
    public static class TestBenchmark3
    {
        public static int OPERATIONS = 50_000;

        @Benchmark
        public void LinkedList(Blackhole blackhole, QueueConfiguration queueConfiguration)
        {
            runBenchmark(queueConfiguration.linkedList, blackhole);
        }

        @Benchmark
        public void StatsQueueLinkedList(Blackhole blackhole, QueueConfiguration queueConfiguration)
        {
            runBenchmark(queueConfiguration.statsQueueLinkedList, blackhole);
        }

        @Benchmark
        public void StatsQueueDisabledSynchronizationLinkedList(Blackhole blackhole, QueueConfiguration queueConfiguration)
        {
            runBenchmark(queueConfiguration.statsQueueDisabledSynchronizationLinkedList, blackhole);
        }

        private static void runBenchmark(Queue<Integer> queue, Blackhole blackhole)
        {
            int operation = 0;

            while (operation < OPERATIONS)
            {
                int messages = 100;

                for (int i = 0; i < messages && i < OPERATIONS; i++)
                {
                    queue.add(i);
                }

                for (int i = 0; i < messages && i < OPERATIONS; i++)
                {
                    blackhole.consume(queue.poll());
                }

                operation += messages;
            }
        }

        @State(Scope.Benchmark)
        public static class QueueConfiguration
        {
            private static final Path testQueuePath = Paths.get(System.getProperty("java.io.tmpdir"), "stats_queue", "stats_queue.log").toAbsolutePath();

            StatsQueue<Integer> statsQueueLinkedList;
            StatsQueue<Integer> statsQueueDisabledSynchronizationLinkedList;
            LinkedList<Integer> linkedList;

            @Setup(Level.Trial)
            @SneakyThrows
            public void setUp()
            {
                FileUtils.cleanDirectory(testQueuePath.getParent());

                var queueConfiguration = net.pedegie.stats.api.queue.QueueConfiguration.builder()
                        .path(testQueuePath.getParent().resolve(Paths.get(testQueuePath.getFileName() + UUID.randomUUID().toString())))
                        .mmapSize(Integer.MAX_VALUE)
                        .build();
                statsQueueLinkedList = StatsQueue.<Integer>builder()
                        .queue(new LinkedList<>())
                        .queueConfiguration(queueConfiguration)
                        .build();

                var queueConfigurationDisabledSync = net.pedegie.stats.api.queue.QueueConfiguration.builder()
                        .path(testQueuePath.getParent().resolve(Paths.get(testQueuePath.getFileName() + UUID.randomUUID().toString())))
                        .disableSynchronization(true)
                        .mmapSize(Integer.MAX_VALUE)
                        .build();

                statsQueueDisabledSynchronizationLinkedList = StatsQueue.<Integer>builder()
                        .queue(new LinkedList<>())
                        .queueConfiguration(queueConfigurationDisabledSync)
                        .build();

                linkedList = new LinkedList<>();
            }
        }

    }

    public static void main(String[] args) throws RunnerException
    {

        Options options = new OptionsBuilder()
                .include(QueueStatsVsLinkedList.class.getSimpleName())
                /*      .jvmArgs("--enable-preview", "-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintAssembly",
                              "-XX:+LogCompilation", "-XX:PrintAssemblyOptions=amd64",
                              "-XX:LogFile=jit_logs.txt")*/
                .build();
        new Runner(options).run();
    }
}
