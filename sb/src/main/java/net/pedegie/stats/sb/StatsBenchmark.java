package net.pedegie.stats.sb;

import net.pedegie.stats.api.queue.MPMCQueueStats;
import net.pedegie.stats.sb.cli.ProgramArguments;

import java.nio.file.Paths;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

public class StatsBenchmark
{
    public static void main(String[] args)
    {
        var programArguments = ProgramArguments.initialize(args);

        var queueStats = MPMCQueueStats.<Integer>builder()
                .queue(new ConcurrentLinkedQueue<>())
                .fileName(Paths.get(System.getProperty("java.io.tmpdir"), "stats_queue").toAbsolutePath())
                .tailer((aLong, integer) -> System.out.println("Received: " + integer + " - " + aLong))
                .build();

        
        Runnable putIntegers = () -> IntStream.range(0, 2500).forEach(value -> {
            queueStats.add(value);
            LockSupport.parkNanos(15000000);
        });
        putIntegers.run();
    }
}
