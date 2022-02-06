package io.github.pedegie.stats.jmh;

import io.github.pedegie.stats.api.queue.Batching;
import io.github.pedegie.stats.api.queue.FileUtils;
import io.github.pedegie.stats.api.queue.QueueConfiguration;
import io.github.pedegie.stats.api.queue.StatsQueue;
import io.github.pedegie.stats.api.queue.probe.Probe;
import io.github.pedegie.stats.api.tailer.ProbeTailer;
import io.github.pedegie.stats.api.tailer.Tailer;
import io.github.pedegie.stats.api.tailer.TailerConfiguration;
import lombok.SneakyThrows;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.file.Path;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static io.github.pedegie.stats.jmh.BenchmarkUtils.randomPath;

public class ProbeTailerJMH
{
    private static final int PROBES = 10_000_000;

    @Fork(value = 1)
    @Warmup(iterations = 5)
    @Measurement(iterations = 4)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode({Mode.AverageTime})
    @State(Scope.Benchmark)
    @Timeout(time = 120)
    public static class TestBenchmark5
    {
        @Benchmark
        public void ProbeTailerBenchmark(QueueConfiguration5 queueConfiguration)
        {
            int readSize = 50;
            for (int i = 0; i < PROBES; i += readSize)
                queueConfiguration.probeTailer.read(readSize);
        }
    }

    @State(Scope.Benchmark)
    public static class QueueConfiguration5
    {
        StatsQueue<Integer> statsQueue;
        ProbeTailer probeTailer;

        @Setup(Level.Iteration)
        public void setUp()
        {

            FileUtils.cleanDirectory(BenchmarkUtils.testQueuePath);
            Path path = randomPath();
            var queueConfiguration = QueueConfiguration.builder()
                    .path(path)
                    .preTouch(true)
                    .mmapSize(4L << 30)
                    .batching(new Batching(100, Long.MAX_VALUE))
                    .disableSynchronization(true)
                    .build();

            statsQueue = StatsQueue.queue(new LinkedList<>(), queueConfiguration);
            IntStream.range(0, PROBES).forEach(statsQueue::add);
            statsQueue.close();

            TailerConfiguration configuration = TailerConfiguration.builder()
                    .tailer(new JMHTailer())
                    .path(path)
                    .build();

            probeTailer = ProbeTailer.from(configuration);
        }

        @TearDown(Level.Iteration)
        @SneakyThrows
        public void teardownTrial()
        {
            probeTailer.close();
        }
    }

    public static void main(String[] args) throws RunnerException
    {

        Options options = new OptionsBuilder()
                .include(ProbeTailerJMH.class.getSimpleName())
                .build();
        new Runner(options).run();
    }

    static class JMHTailer implements Tailer
    {
        @Override
        public void onProbe(Probe probe)
        {
        }
    }
}
