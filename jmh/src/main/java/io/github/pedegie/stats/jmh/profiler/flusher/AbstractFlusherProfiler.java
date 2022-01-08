/*
package io.github.pedegie.stats.jmh.profiler.flusher;

import io.github.pedegie.stats.api.queue.Flusher;
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

import java.util.concurrent.TimeUnit;

@Fork(value = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode({Mode.SingleShotTime})
@State(Scope.Benchmark)
@Timeout(time = 120)
public abstract class AbstractFlusherProfiler
{
    private static final int PROFILING_TIME_SECONDS = 60;
    private static final int ADDITIONAL_FLUSHABLE_ADDITION_INTERVAL_SECONDS = 6;

    protected int numberOfAdditionalFlushables = PROFILING_TIME_SECONDS / ADDITIONAL_FLUSHABLE_ADDITION_INTERVAL_SECONDS;

    @Param({"128", "256", "512"})
    protected int numberOfFlushables;

    private Flusher flusher;

    @Setup(Level.Iteration)
    public void setup()
    {
        beforeStart();
        flusher = new Flusher();
        flusher.start();
        addFlushables(flusher);
    }

    @TearDown(Level.Iteration)
    public void tearDown()
    {
        flusher.stop();
    }

    @Benchmark
    public void benchmark() throws InterruptedException
    {
        for (int i = 0; i < numberOfAdditionalFlushables; i++)
        {
            flusher.addFlushable(new TestFlushable(getInterval()));
            TimeUnit.SECONDS.sleep(ADDITIONAL_FLUSHABLE_ADDITION_INTERVAL_SECONDS);
        }
    }

    abstract long getInterval();

    void beforeStart()
    {

    }

    private void addFlushables(Flusher flusher)
    {
        for (int i = 0; i < numberOfFlushables; i++)
        {
            flusher.addFlushable(new TestFlushable(getInterval()));
        }
    }
}
*/
