package net.pedegie.stats.jmh;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

public class MPMCQueuePerformanceTest
{
    @Fork(value = 1)
    @Warmup(iterations = 0, time = 10)
    @Measurement(iterations = 5, time = 10)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode({Mode.AverageTime})
    @State(Scope.Benchmark)
    @Timeout(time = 1)
    public static class TestBenchmark
    {

        @Benchmark
        public int test()
        {
            return 5;

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
