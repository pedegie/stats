/*
package io.github.pedegie.stats.jmh.profiler.flusher;

import org.openjdk.jmh.profile.AsyncProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class FlusherProfilerRunner
{
    public static void main(String[] args) throws RunnerException
    {
        Options options = new OptionsBuilder()
                .include(StaticIntervalFlusherProfiler.class.getSimpleName())
                .include(DynamicIntervalFlusherProfiler.class.getSimpleName())
                .addProfiler(AsyncProfiler.class, "event=cpu;output=flamegraph")
                .build();
        new Runner(options).run();
    }
}
*/
