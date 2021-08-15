package net.pedegie.stats.sb;

import net.openhft.chronicle.threads.BusyPauser;
import net.pedegie.stats.api.queue.MPMCQueueStats;
import net.pedegie.stats.sb.cli.ProgramArguments;
import net.pedegie.stats.sb.timeout.Timeout;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

public class StatsBenchmark
{
    private static final int POISON_PILL = -1;
    private static final Logger log = LogManager.getLogger(StatsBenchmark.class);

    private static final Path statsQueue = Paths.get(System.getProperty("java.io.tmpdir"), "stats_queue").toAbsolutePath();
    private static final Timeout BENCHMARK_TIMEOUT = new Timeout(TimeUnit.SECONDS, 60);

    public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException
    {
        var programArguments = ProgramArguments.initialize(args);
        cleanStatsQueueDirectory();
        var queueStats = createStatsQueue();

        Runnable putIntegers = () ->
        {
            IntStream.range(0, programArguments.getMessagesToSendPerThread()).forEach(value ->
            {
                log.debug("Adding {}", value);
                queueStats.add(value);
                LockSupport.parkNanos(toNanos(programArguments.getDelayMillisToWaitBetweenMessages()));
            });
            queueStats.add(POISON_PILL);
        };

        Runnable consumeIntegers = () ->
        {
            while (true)
            {
                var elem = queueStats.poll();
                if (elem != null)
                {
                    log.debug("Consuming {}", elem);
                    if (elem == POISON_PILL)
                    {
                        break;
                    }
                } else
                {
                    BusyPauser.INSTANCE.pause();
                }

            }
        };

        var producerPool = Executors.newFixedThreadPool(programArguments.getProducerThreads(), new NamedThreadFactory("producer_pool"));
        var consumerPool = Executors.newFixedThreadPool(programArguments.getConsumerThreads(), new NamedThreadFactory("consumer_pool"));

        int consumerAndProducerThreads = programArguments.getProducerThreads() + programArguments.getConsumerThreads();

        CompletableFuture<?>[] futures = new CompletableFuture[consumerAndProducerThreads];

        for (int i = 0; i < programArguments.getWarmupIterations(); i++)
        {
            log.info("Started {} warmup iteration", i + 1);

            IntStream.range(0, programArguments.getProducerThreads())
                    .forEach(index -> futures[index] = runOn(putIntegers, producerPool));

            IntStream.range(programArguments.getProducerThreads(), consumerAndProducerThreads)
                    .forEach(index -> futures[index] = runOn(consumeIntegers, consumerPool));

            CompletableFuture.allOf(futures).get(BENCHMARK_TIMEOUT.getTimeout(), BENCHMARK_TIMEOUT.getUnit());

        }
        log.info("Started real benchmark");

        IntStream.range(0, programArguments.getProducerThreads())
                .forEach(index -> futures[index] = runOn(putIntegers, producerPool));

        IntStream.range(programArguments.getProducerThreads(), consumerAndProducerThreads)
                .forEach(index -> futures[index] = runOn(consumeIntegers, consumerPool));

        producerPool.shutdown();
        consumerPool.shutdown();


        boolean producerTerminated = producerPool.awaitTermination(BENCHMARK_TIMEOUT.getTimeout(), BENCHMARK_TIMEOUT.getUnit());
        boolean consumerTerminated = consumerPool.awaitTermination(BENCHMARK_TIMEOUT.getTimeout(), BENCHMARK_TIMEOUT.getUnit());

        if (!(producerTerminated && consumerTerminated))
        {
            log.error("Timeouted, hardcoded timeout: {} {}", BENCHMARK_TIMEOUT.getTimeout(), BENCHMARK_TIMEOUT.getUnit());
            System.exit(1);
        }

        log.info("Benchmark finished");
    }

    private static CompletableFuture<Object> runOn(Runnable runnable, ExecutorService pool)
    {
        return CompletableFuture.supplyAsync(() ->
        {
            runnable.run();
            return null;
        }, pool);
    }

    private static MPMCQueueStats<Integer> createStatsQueue()
    {
        return MPMCQueueStats.<Integer>builder()
                .queue(new ConcurrentLinkedQueue<>())
                .fileName(statsQueue)
                .build();
    }

    private static void cleanStatsQueueDirectory()
    {
        var dir = new File(statsQueue.toString());
        File[] files = dir.listFiles();
        if (files != null)
        {
            for (File file : files)
                if (!file.isDirectory())
                    if (!file.delete())
                        throw new IllegalStateException("Cannot delete " + file.getAbsolutePath());
        }
    }

    private static long toNanos(double delayMillisToWaitBetweenMessages)
    {
        return (long) (delayMillisToWaitBetweenMessages * 1000000.0);
    }

    static class NamedThreadFactory implements ThreadFactory
    {
        private final String name;

        public NamedThreadFactory(String name)
        {
            this.name = name;
        }

        public Thread newThread(Runnable r) {
            return new Thread(r, name);
        }
    }
}
