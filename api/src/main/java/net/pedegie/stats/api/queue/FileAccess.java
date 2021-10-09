package net.pedegie.stats.api.queue;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@Slf4j
class FileAccess
{
    static int FILE_ALREADY_EXISTS = 0;
    static int ERROR_DURING_INITIALIZATION = -2;
    static int CLOSE_FILE_MESSAGE_ID = -1;

    private static final CompletableFuture<Object> COMPLETED = CompletableFuture.completedFuture(null);
    private static final Tuple<Integer, Semaphore> FILE_ALREADY_EXISTS_TUPLE = new Tuple<>(FILE_ALREADY_EXISTS, null);
    private static final Tuple<Integer, Semaphore> ERROR_DURING_INIT = new Tuple<>(ERROR_DURING_INITIALIZATION, null);
    private final int timeoutMillis;

    ExecutorService closeAndRegisterThreadPool = ThreadPools.singleThreadPool("closeAndRegisterThreadPool_" + UUID.randomUUID());
    ExecutorService lowPriorityThreadPool = ThreadPools.boundedSingleThreadPool("lowPriorityThreadPool _" + UUID.randomUUID());

    TIntObjectMap<FileAccessContext> files;
    InternalFileAccess internalFileAccess;

    FileAccess(InternalFileAccess internalFileAccess)
    {
        files = new TIntObjectHashMap<>(8);
        this.internalFileAccess = internalFileAccess;
        this.timeoutMillis = Properties.get("fileaccess.timeoutthresholdmillis", 30_000);
    }

    public void writeProbe(Probe probe)
    {
        log.trace("Received probe: {}", probe);

        var fileAccess = files.get(probe.getAccessId());

        if (fileAccess == null)
            return;

        if (isCloseFileMessage(probe))
        {
            closeAccessAsync(probe.getAccessId());
        } else if (fileAccess.writesEnabled())
        {
            if (needRecycle(fileAccess, probe))
            {
                recycle(fileAccess, probe);
            } else if (fileAccess.needResize())
            {
                resize(fileAccess, probe);
            } else
            {
                writeProbeToFile(fileAccess, probe);
            }
        }

    }

    private void writeProbeToFile(FileAccessContext fileAccess, Probe probe)
    {
        try
        {
            log.trace("Writing probe: {}", probe);
            fileAccess.writeProbe(probe);
            log.trace("Written probe: {}", probe);
        } catch (Exception e)
        {
            if (handleError(fileAccess, e))
            {
                closeAccessAsync(probe.getAccessId());
            }
        }
    }

    private boolean needRecycle(FileAccessContext fileAccess, Probe probe)
    {
        return probe.getTimestamp() >= fileAccess.getNextCycleTimestampMillis();
    }

    private CompletableFuture<Object> closeAccessAsync(int accessId)
    {
        var context = files.get(accessId);
        if (context.acquireClose())
        {
            log.trace("Acquired close for {}", accessId);

            return CompletableFuture.supplyAsync(() ->
            {
                try
                {
                    if (context.isTerminated())
                        return null;

                    BusyWaiter.busyWait(() -> context.availablePermits() == 1, "closing file " + context.availablePermits());
                    closeAccess(context, accessId);
                } finally
                {
                    context.releaseClose();
                }
                return null;
            }, closeAndRegisterThreadPool).orTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
        }

        return COMPLETED;
    }

    private void closeAccess(FileAccessContext fileAccess, int accessId)
    {
        try
        {
            log.debug("Closing {}", accessId);
            internalFileAccess.closeAccess(fileAccess);
            var accessContext = files.remove(accessId);
            assert accessContext != null;
            log.debug("Closed {}", accessId);
        } catch (Exception e)
        {
            log.error("Error occurred during closing file, removing file but resources may be not released! (leak)");
            handleError(fileAccess, e);
        } finally
        {
            fileAccess.terminate();
        }
    }

    private boolean isCloseFileMessage(Probe probe)
    {
        return CLOSE_FILE_MESSAGE_ID == probe.getProbe();
    }

    public CompletableFuture<Tuple<Integer, Semaphore>> registerFile(QueueConfiguration conf)
    {
        return CompletableFuture.supplyAsync(() ->
        {
            var id = conf.getPath().hashCode();
            if (files.containsKey(id))
            {
                return FILE_ALREADY_EXISTS_TUPLE;
            }
            log.debug("Registering file {}", id);

            FileAccessContext accessContext = accessContext(conf);
            if (accessContext == null)
            {
                return ERROR_DURING_INIT;
            }
            files.put(id, accessContext);
            log.debug("File registered {}", id);
            return new Tuple<>(id, accessContext.getState());

        }, closeAndRegisterThreadPool);
    }

    private FileAccessContext accessContext(QueueConfiguration conf)
    {
        FileAccessContext accessContext = null;
        try
        {
            accessContext = internalFileAccess.accessContext(conf);
            preTouch(accessContext);
            return accessContext;
        } catch (Exception e)
        {
            if (conf.getErrorHandler().handle(e) && accessContext != null)
                accessContext.close();
            return null;
        }
    }

    @SneakyThrows
    private void recycle(FileAccessContext fileAccess, Probe probe)
    {
        fileAccess.acquireWrites();
        asyncWork(fileAccess, () ->
        {
            log.debug("Recycling file {}", probe.getAccessId());
            internalFileAccess.recycle(fileAccess);
            preTouch(fileAccess);
            log.debug("File recycled {}", probe.getAccessId());
        }, probe.getAccessId(), lowPriorityThreadPool).orTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    @SneakyThrows
    private void resize(FileAccessContext fileAccess, Probe probe)
    {
        fileAccess.acquireWrites();
        asyncWork(fileAccess, () ->
        {
            log.debug("Resizing file {}", probe.getAccessId());
            internalFileAccess.resize(fileAccess);
            preTouch(fileAccess);
            log.debug("File resized {}", probe.getAccessId());
        }, probe.getAccessId(), lowPriorityThreadPool).orTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    private CompletableFuture<Object> asyncWork(FileAccessContext fileAccess, Runnable work, int accessId, Executor pool)
    {
        return CompletableFuture.supplyAsync(() ->
        {
            work.run();
            fileAccess.releaseWrites();
            return null;
        }, pool).exceptionally(throwable ->
        {
            if (handleError(fileAccess, throwable))
            {
                if (fileAccess.acquireClose())
                {
                    closeAccess(fileAccess, accessId);
                }
            }
            fileAccess.releaseWrites();
            return null;
        });
    }

    private boolean handleError(FileAccessContext fileAccess, Throwable throwable)
    {
        return fileAccess.getQueueConfiguration().getErrorHandler().handle(throwable);
    }

    @SneakyThrows
    public void closeAllBlocking()
    {
        var keys = files.keys();

        log.debug("Closing {} files", keys.length);

        tryCloseBlocking(keys);

        closeAndRegisterThreadPool.shutdown();
        lowPriorityThreadPool.shutdown();

        var size = files.size();
        var timeout = size == 0 ? 100 : timeoutMillis * size;
        boolean closed1 = closeAndRegisterThreadPool.awaitTermination(timeout, TimeUnit.MILLISECONDS);
        size = files.size();
        timeout = size == 0 ? 100 : timeoutMillis * size;
        boolean closed2 = lowPriorityThreadPool.awaitTermination(timeout, TimeUnit.MILLISECONDS);

        keys = files.keys();

        for (int accessId : keys)
        {
            var accessContextWhichCannotBeClosedForSomeReason = files.remove(accessId);
            if (accessContextWhichCannotBeClosedForSomeReason != null)
            {
                log.error("Cannot close {}. Removing file and releasing thread resources, file might still be mapped! (leak)", accessId);
                accessContextWhichCannotBeClosedForSomeReason.terminate();
            }
        }

        assert files.size() == 0;

        if (!closed1)
        {
            log.error("Cannot close pool {}", "closeAndRegisterThreadPool");
        }

        if (!closed2)
        {
            log.error("Cannot close pool {}", "lowPriorityThreadPool");
        }
    }

    private void tryCloseBlocking(int[] keys)
    {
        for (int accessId : keys)
        {
            closeAccessAsync(accessId).handle((unused, throwable) ->
            {
                if (throwable != null)
                {
                    log.error("Timeout during closing file {}, trying again in a while", accessId, throwable);
                }
                return unused;
            }).join();
        }

    }

    private void preTouch(FileAccessContext accessContext)
    {
        if (accessContext.getQueueConfiguration().isPreTouch())
            PreToucher.preTouch(accessContext.getBuffer());
    }
}
