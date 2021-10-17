package net.pedegie.stats.sb;

import lombok.SneakyThrows;
import net.pedegie.stats.api.queue.FileUtils;
import net.pedegie.stats.api.queue.QueueConfiguration;
import net.pedegie.stats.api.queue.StatsQueue;
import net.pedegie.stats.sb.cli.ProgramArguments;
import net.pedegie.stats.sb.timeout.Timeout;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

public class StatsBenchmark
{
    private static final int REAL_BENCHMARK = Integer.MIN_VALUE;

    private static final int POISON_PILL = -1;
    private static final Logger log = LogManager.getLogger(StatsBenchmark.class);

    private static final Path statsQueue = Paths.get(System.getProperty("java.io.tmpdir"), "stats_queue", "stats_queue.log").toAbsolutePath();
    private static final Timeout BENCHMARK_TIMEOUT = new Timeout(TimeUnit.SECONDS, 120);

    public static void main(String[] args)
    {
        var programArguments = ProgramArguments.initialize(args);
        double[] benchmarkResultsInMillis = new double[programArguments.getWarmupIterations() + 1];

        for (int i = 0; i < programArguments.getWarmupIterations(); i++)
        {
            var benchmarkDuration = runBenchmark(programArguments, i + 1);
            benchmarkResultsInMillis[i] = benchmarkDuration;
        }
        var benchmarkDuration = runBenchmark(programArguments, REAL_BENCHMARK);
        benchmarkResultsInMillis[programArguments.getWarmupIterations()] = benchmarkDuration;

        StringBuilder summary = new StringBuilder();
        for (int i = 0; i < programArguments.getWarmupIterations(); i++)
        {
            summary.append(i + 1).append(" warmup iteration take ").append(benchmarkResultsInMillis[i]).append(" ms\n");
        }
        summary.append("Real benchmark take ").append(benchmarkResultsInMillis[programArguments.getWarmupIterations()]).append(" ms");
        log.info("Benchmark finished, summary:\n{}", summary.toString());
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

    @SneakyThrows
    private static double runBenchmark(ProgramArguments programArguments, int iteration)
    {
        Queue<Integer> queue = createStatsQueue();
        var producer = producer(programArguments, queue);
        var consumer = consumer(queue);

        var producerPool = Executors.newFixedThreadPool(programArguments.getProducerThreads(), new NamedThreadFactory("producer_pool_%d"));
        var consumerPool = Executors.newFixedThreadPool(programArguments.getConsumerThreads(), new NamedThreadFactory("consumer_pool_%d"));

        int consumerAndProducerThreads = programArguments.getProducerThreads() + programArguments.getConsumerThreads();
        CompletableFuture<?>[] futures = new CompletableFuture[consumerAndProducerThreads];

        IntStream.range(0, programArguments.getProducerThreads())
                .forEach(index -> futures[index] = runOn(producer, producerPool));

        IntStream.range(programArguments.getProducerThreads(), consumerAndProducerThreads)
                .forEach(index -> futures[index] = runOn(consumer, consumerPool));

        var startTimestamp = System.nanoTime();

        if (iteration == REAL_BENCHMARK)
        {
            log.info("Started Real Benchmark");
        } else
        {
            log.info("Started {} warmup iteration", iteration);
        }
        CompletableFuture.allOf(futures).exceptionally(t ->
        {
            log.error("", t);
            return null;
        }).get(BENCHMARK_TIMEOUT.getTimeout(), BENCHMARK_TIMEOUT.getUnit());

        var benchmarkDuration = toMillis(System.nanoTime() - startTimestamp);
        if (iteration == REAL_BENCHMARK)
        {
            log.info("Real Benchmark take {} ms", benchmarkDuration);
        } else
        {
            log.info("Warmup iteration take {} ms", benchmarkDuration);
        }

        producerPool.shutdown();
        consumerPool.shutdown();
        boolean producerTerminated = producerPool.awaitTermination(BENCHMARK_TIMEOUT.getTimeout(), BENCHMARK_TIMEOUT.getUnit());
        boolean consumerTerminated = consumerPool.awaitTermination(BENCHMARK_TIMEOUT.getTimeout(), BENCHMARK_TIMEOUT.getUnit());

        if (!(producerTerminated && consumerTerminated))
        {
            log.error("Timeouted, hardcoded timeout: {} {}", BENCHMARK_TIMEOUT.getTimeout(), BENCHMARK_TIMEOUT.getUnit());
            System.exit(1);
        }

        if (queue instanceof StatsQueue)
        {
            ((StatsQueue) queue).close();
        }

        return benchmarkDuration;
    }

    @SneakyThrows
    private static StatsQueue<Integer> createStatsQueue()
    {
        FileUtils.cleanDirectory(statsQueue.getParent());

        var queueConfiguration = QueueConfiguration.builder()
                .path(statsQueue)
                .mmapSize(Integer.MAX_VALUE)
                .build();

        return StatsQueue.<Integer>builder()
                .queue(new ConcurrentLinkedQueue<>())
                .queueConfiguration(queueConfiguration)
                .build();
    }

    private static Runnable producer(ProgramArguments programArguments, Queue<Integer> queueStats)
    {
        return () ->
        {
            IntStream.range(0, programArguments.getMessagesToSendPerThread()).forEach(value ->
            {
                queueStats.add(value);
                LockSupport.parkNanos(toNanos(programArguments.getDelayMillisToWaitBetweenMessages()));
            });
            queueStats.add(POISON_PILL);
        };
    }

    private static long toNanos(double delayMillisToWaitBetweenMessages)
    {
        return (long) (delayMillisToWaitBetweenMessages * 1000000.0);
    }

    private static Runnable consumer(Queue<Integer> queueStats)
    {
        return () ->
        {
            while (true)
            {
                var elem = queueStats.poll();
                if (elem != null)
                {
                    if (elem == POISON_PILL)
                    {
                        break;
                    }
                }

            }
        };
    }

    private static CompletableFuture<Object> runOn(Runnable runnable, ExecutorService pool)
    {
        return CompletableFuture.supplyAsync(() ->
        {
            runnable.run();
            return null;
        }, pool).exceptionally(t ->
        {
            log.error("", t);
            return null;
        });
    }

    private static double toMillis(long nanoSeconds)
    {
        return nanoSeconds / 1000000.0;
    }
}
