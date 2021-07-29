package net.pedegie.stats.api;

import net.pedegie.stats.api.queue.MPMCQueueStats;

import java.nio.file.Paths;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;

public class Stats
{
    public static void main(String[] args)
    {
        var queueStats = MPMCQueueStats.<Integer>builder()
                .queue(new ConcurrentLinkedQueue<>())
                .fileName(Paths.get(System.getProperty("java.io.tmpdir"), "stats_queue").toAbsolutePath())
                .queueReader(System.out::println)
                .build();


        Runnable putIntegers = () -> IntStream.range(0, 500).forEach(queueStats::add);
        putIntegers.run();
    }
}
