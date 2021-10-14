package net.pedegie.stats.api.queue;

import lombok.SneakyThrows;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class TimeoutedFuture
{
    private static volatile ScheduledExecutorService schedulerExecutor;

    public static void ensureIsStarted()
    {
        if (schedulerExecutor == null || schedulerExecutor.isTerminated())
            schedulerExecutor = Executors.newScheduledThreadPool(10, ThreadPools.namedThreadFactory("transaction-timeout"));
    }

    public static <T> CompletableFuture<T> supplyAsync(
            Transaction<T> transaction, long timeoutMillis, ExecutorService pool)
    {

        CompletableFuture<T> cf = new CompletableFuture<>();
        CompletableFuture<?> withinTimeout = new CompletableFuture<>();

        pool.submit(() ->
        {
            ScheduledFuture<?> timer = null;

            try
            {
                transaction.setup();

                timer = schedulerExecutor.schedule(() ->
                {
                    if (!withinTimeout.isDone())
                    {
                        cf.completeExceptionally(new TimeoutException());
                    }

                }, timeoutMillis, TimeUnit.MILLISECONDS);

                transaction.withinTimeout();
                withinTimeout.complete(null);

                if (!cf.isCompletedExceptionally())
                    cf.complete(transaction.commit());

            } catch (Throwable ex)
            {
                cf.completeExceptionally(ex);
            } finally
            {
                if (timer != null)
                {
                    timer.cancel(false);

                }
            }
        });


        return cf;
    }

    @SneakyThrows
    public static boolean shutdownBlocking(long timeoutmillis)
    {
        schedulerExecutor.shutdown();
        return schedulerExecutor.awaitTermination(timeoutmillis, TimeUnit.MILLISECONDS);
    }
}
