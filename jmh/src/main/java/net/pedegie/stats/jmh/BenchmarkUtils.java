package net.pedegie.stats.jmh;

import net.pedegie.stats.api.queue.BusyWaiter;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class BenchmarkUtils
{
    private static final int POISON_PILL = -11;

    public static Supplier<Void> runBenchmarkForQueue(Queue<Integer> queue, int threads, ExecutorService producerPool, ExecutorService consumerPool)
    {
        return runBenchmarkForQueue(queue, threads, producerPool, consumerPool, -1L);
    }

    public static Supplier<Void> runBenchmarkForQueue(Queue<Integer> queue, int threads, ExecutorService producerPool, ExecutorService consumerPool, long delayNanos)
    {
        int messagesToSendPerThread = 50000;
        Runnable producer = delayNanos > 0 ? delayedWriter(queue, messagesToSendPerThread, delayNanos) : writer(queue, messagesToSendPerThread);
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
            } catch (Exception e)
            {
                e.printStackTrace();
            }
            return null;
        };
    }

    private static Runnable writer(Queue<Integer> queue, int messagesToSendPerThread)
    {
        return () ->
        {
            for (int i = 1; i <= messagesToSendPerThread; i++)
            {
                queue.add(i);
            }
            queue.add(POISON_PILL);
        };
    }

    private static Runnable delayedWriter(Queue<Integer> queue, int messagesToSendPerThread, long nanos)
    {
        return () ->
        {
            for (int i = 1; i <= messagesToSendPerThread; i++)
            {
                queue.add(i);
                BusyWaiter.busyWaitNanos(nanos);
            }
            queue.add(POISON_PILL);
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

    public static final Path testQueuePath = Paths.get(System.getProperty("java.io.tmpdir"), "stats_queue", "stats_queue.log").toAbsolutePath();

    public static Path randomPath()
    {
        return testQueuePath.getParent().resolve(Paths.get(testQueuePath.getFileName() + UUID.randomUUID().toString()));
    }
}
