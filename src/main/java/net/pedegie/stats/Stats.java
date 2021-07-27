package net.pedegie.stats;

import net.pedegie.stats.queue.MPMCQueueStats;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;

public class Stats
{
    public static void main(String[] args)
    {
        var queueStats = MPMCQueueStats.<Integer>builder()
                .queue(new ConcurrentLinkedQueue<>())
                .fileName("/home/kacper/kolejka")
                .queueReader(System.out::println)
                .build();


        Runnable putIntegers = () -> IntStream.range(0, 500).forEach(queueStats::add);
        putIntegers.run();
    }
}
