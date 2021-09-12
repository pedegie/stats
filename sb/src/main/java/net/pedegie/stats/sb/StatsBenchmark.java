package net.pedegie.stats.sb;

import lombok.SneakyThrows;
import net.pedegie.stats.api.queue.LogFileConfiguration;
import net.pedegie.stats.api.queue.MPMCQueueStats;
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
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

public class StatsBenchmark
{
    private static final int POISON_PILL = -1;
    private static final Logger log = LogManager.getLogger(StatsBenchmark.class);

    private static final Path statsQueue = Paths.get(System.getProperty("java.io.tmpdir"), "stats_queue").toAbsolutePath();
    private static final Timeout BENCHMARK_TIMEOUT = new Timeout(TimeUnit.SECONDS, 120);

    public static void main(String[] args)
    {
        var programArguments = ProgramArguments.initialize(args);
        for (int i = 0; i < programArguments.getWarmupIterations(); i++)
        {
            log.info("Started {} warmup iteration", i + 1);
            var benchmarkDurationNS = runBenchmark(programArguments);
            log.info("Warmup iteration take {} ms", benchmarkDurationNS / 1000000.0);
        }
        log.info("Started Real Benchmark");
        var benchmarkDurationNS = runBenchmark(programArguments);
        log.info("Real Benchmark take {} millis", benchmarkDurationNS / 1000000.0);

        log.info("Benchmark finished");
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

    @SneakyThrows
    private static long runBenchmark(ProgramArguments programArguments)
    {
        Queue<Integer> queue = createStatsQueue();
        var producer = producer(programArguments, queue);
        var consumer = consumer(queue);

        var producerPool = Executors.newFixedThreadPool(programArguments.getProducerThreads(), new NamedThreadFactory("producer_pool"));
        var consumerPool = Executors.newFixedThreadPool(programArguments.getConsumerThreads(), new NamedThreadFactory("consumer_pool"));

        int consumerAndProducerThreads = programArguments.getProducerThreads() + programArguments.getConsumerThreads();
        CompletableFuture<?>[] futures = new CompletableFuture[consumerAndProducerThreads];

        IntStream.range(0, programArguments.getProducerThreads())
                .forEach(index -> futures[index] = runOn(producer, producerPool));

        IntStream.range(programArguments.getProducerThreads(), consumerAndProducerThreads)
                .forEach(index -> futures[index] = runOn(consumer, consumerPool));

        var startTimestamp = System.nanoTime();
        CompletableFuture.allOf(futures).get(BENCHMARK_TIMEOUT.getTimeout(), BENCHMARK_TIMEOUT.getUnit());
        var benchmarkDuration = System.nanoTime() - startTimestamp;

        if (queue instanceof MPMCQueueStats)
        {
            ((MPMCQueueStats) queue).close();
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
        return benchmarkDuration;
    }

    private static MPMCQueueStats<Integer> createStatsQueue()
    {
        var logFileConfiguration = LogFileConfiguration.builder()
                .path(statsQueue)
                .mmapSize(Integer.MAX_VALUE)
                .build();

        return MPMCQueueStats.<Integer>builder()
                .queue(new ConcurrentLinkedQueue<>())
                .logFileConfiguration(logFileConfiguration)
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
        }, pool);
    }
}
