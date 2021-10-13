package net.pedegie.stats.api.queue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class TimeoutedFuture
{
	private static final ScheduledExecutorService schedulerExecutor =
			Executors.newScheduledThreadPool(10, ThreadPools.namedThreadFactory("transaction_timeout"));

	public static <T> CompletableFuture<T> supplyAsync(
			Transaction<T> transaction, long timeoutMillis, ExecutorService pool)
	{

		CompletableFuture<T> cf = new CompletableFuture<>();
		CompletableFuture<?> withinTimeout = new CompletableFuture<>();

		pool.submit(() ->
		{
			try
			{
				transaction.setup();

				schedulerExecutor.schedule(() ->
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
			}
		});


		return cf;
	}
}
