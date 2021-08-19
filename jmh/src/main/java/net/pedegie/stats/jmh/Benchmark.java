package net.pedegie.stats.jmh;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class Benchmark
{
    public static Supplier<Void> runBenchmarkForQueue(Queue<Integer> queue, int threads)
    {
        int messagesToSendPerThread = 50000;
        Runnable producer = () ->
        {
            for (int i = 1; i <= messagesToSendPerThread; i++)
            {
                queue.add(i);
                LockSupport.parkNanos(1_000);
            }

            IntStream.range(0, threads).forEach(i -> queue.add(-1));
        };
        Runnable consumer = () ->
        {
            while (true)
            {
                Integer element = queue.poll();
                if (element != null)
                {
                    if (element == -1)
                    {
                        break;
                    }
                }
            }
        };

        return () ->
        {
            var producerThreadPool = Executors.newFixedThreadPool(threads, new NamedThreadFactory("producer_pool"));
            var consumerThreadPool = Executors.newFixedThreadPool(threads, new NamedThreadFactory("consumer_pool"));

            List<CompletableFuture<?>> futures = new ArrayList<>(threads * 2);
            IntStream.range(0, threads).forEach(index ->
            {
                futures.add(CompletableFuture.supplyAsync(() ->
                {
                    producer.run();
                    return null;
                }, producerThreadPool));
                futures.add(CompletableFuture.supplyAsync(() ->
                {
                    consumer.run();
                    return null;
                }, consumerThreadPool));
            });

            producerThreadPool.shutdown();
            consumerThreadPool.shutdown();
            try
            {
                producerThreadPool.awaitTermination(60, TimeUnit.SECONDS);
                consumerThreadPool.awaitTermination(60, TimeUnit.SECONDS);

            } catch (InterruptedException e)
            {
                e.printStackTrace();
            }
            return null;
        };
    }

    static class NamedThreadFactory implements ThreadFactory
    {
        private final String name;

        public NamedThreadFactory(String name)
        {
            this.name = name;
        }

        public Thread newThread(@NotNull Runnable r)
        {
            return new Thread(r, name);
        }
    }
}
