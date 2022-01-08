/*
package io.github.pedegie.stats.jmh.profiler.flusher;

import org.openjdk.jmh.annotations.Param;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

public class DynamicIntervalFlusherProfiler extends AbstractFlusherProfiler
{
    @Param({"10-1000"})
    private String intervalRange;

    private Queue<Long> intervals;

    @Override
    public void beforeStart()
    {
        var lower = Integer.parseInt(intervalRange.split("-")[0]);
        var upper = Integer.parseInt(intervalRange.split("-")[1]);
        var queueSize = numberOfFlushables + numberOfAdditionalFlushables;
        intervals = new ArrayDeque<>(queueSize);
        IntStream.range(0, queueSize)
                .forEach(x -> intervals.add(ThreadLocalRandom.current().nextLong(lower, upper)));

        System.out.println("Running with: " + intervals.size() + " " + intervals);
    }

    @Override
    long getInterval()
    {
        return intervals.remove();
    }
}
*/
