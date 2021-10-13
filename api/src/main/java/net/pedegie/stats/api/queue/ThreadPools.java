package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class ThreadPools
{
    static ExecutorService boundedSingleThreadPool(String poolName)
    {
        return new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(512), //todo decorate MPMC queue from jctools in BlockingQueue
                namedThreadFactory(poolName),
                new RejectedExecutionHandlerImpl());
    }

    static ExecutorService singleThreadPool(String poolName)
    {
        return new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                namedThreadFactory(poolName));
    }

    public static ThreadFactory namedThreadFactory(String poolName)
    {
        return r -> new Thread(r, poolName);
    }

    private static class RejectedExecutionHandlerImpl implements RejectedExecutionHandler
    {

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor)
        {
            log.error("Rejected {} due to full Probes queue", r.toString());
        }

    }
}
