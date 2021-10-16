package net.pedegie.stats.jmh;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class Benchmark
{
    private static final int POISON_PILL = -11;

    public static Supplier<Void> runBenchmarkForQueue(Queue<Integer> queue, int threads, ExecutorService producerPool, ExecutorService consumerPool)
    {
        int messagesToSendPerThread = 50000;
        Runnable producer = () ->
        {
            for (int i = 1; i <= messagesToSendPerThread; i++)
            {
                queue.add(i);
            }
            queue.add(POISON_PILL);
        };
        Runnable consumer = () ->
        {
            while (true)
            {
                Integer element = queue.poll();
                if (element != null)
                {
                    if (element == POISON_PILL)
                    {
                        break;
                    }
                }
            }
        };

        return () ->
        {
            List<CompletableFuture<?>> futures = new ArrayList<>(threads * 2);
            IntStream.range(0, threads).forEach(index ->
            {
                futures.add(CompletableFuture.supplyAsync(() ->
                {
                    producer.run();
                    return null;
                }, producerPool));
                futures.add(CompletableFuture.supplyAsync(() ->
                {
                    consumer.run();
                    return null;
                }, consumerPool));
            });

            try
            {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[]{})).get(60, TimeUnit.SECONDS);
            } catch (InterruptedException e)
            {
                e.printStackTrace();
            } catch (ExecutionException e)
            {
                e.printStackTrace();
            } catch (TimeoutException e)
            {
                e.printStackTrace();
            }
            return null;
        };
    }

    static class NamedThreadFactory implements ThreadFactory
    {
        private final AtomicInteger threadNumber = new AtomicInteger(0);

        private final String name;

        public NamedThreadFactory(String name)
        {
            this.name = name;
        }

        public Thread newThread(@NotNull Runnable r)
        {
            return new Thread(r, String.format(name, threadNumber.incrementAndGet()));
        }
    }
}
