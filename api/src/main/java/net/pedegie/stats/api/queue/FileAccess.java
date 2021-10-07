package net.pedegie.stats.api.queue;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

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
    private static final CompletableFuture<Object> COMPLETED = CompletableFuture.completedFuture(null);
    private static final Tuple<Integer, Semaphore> FILE_ALREADY_EXISTS_TUPLE = new Tuple<>(FILE_ALREADY_EXISTS, null);
    private static final Tuple<Integer, Semaphore> ERROR_DURING_INIT = new Tuple<>(ERROR_DURING_INITIALIZATION, null);
    private static final int TIMEOUT_SECONDS = 30;

    static int CLOSE_FILE_MESSAGE_ID = -1;

    ExecutorService closeAndRegisterThreadPool = ThreadPools.singleThreadPool("closeAndRegisterThreadPool");
    ExecutorService lowPriorityThreadPool = ThreadPools.boundedSingleThreadPool("lowPriorityThreadPool");

    TIntObjectMap<FileAccessContext> files;
    InternalFileAccess internalFileAccess;

    FileAccess(InternalFileAccess internalFileAccess)
    {
        files = new TIntObjectHashMap<>(8);
        this.internalFileAccess = internalFileAccess;
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
            return asyncWork(context, () ->
            {
                try
                {
                    BusyWaiter.busyWait(() -> context.availablePermits() == 1, "closing file ");
                    if (context.isTerminated())
                        return;

                    closeAccess(accessId);
                } catch (Exception e)
                {
                    handleError(context, e);
                    context.terminate();
                } finally
                {
                    context.releaseWrites();
                }
            }, accessId, closeAndRegisterThreadPool).orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }

        return COMPLETED;
    }

    private void closeAccess(int accessId)
    {
        log.debug("Closing {}", accessId);

        var accessContext = files.remove(accessId);
        assert accessContext != null;
        internalFileAccess.closeAccess(accessContext);

        log.debug("Closed {}", accessId);
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
            writeProbeToFile(fileAccess, probe);
            log.debug("File recycled {}", probe.getAccessId());
        }, probe.getAccessId(), lowPriorityThreadPool).orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
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
            writeProbeToFile(fileAccess, probe);
            log.debug("File resized {}", probe.getAccessId());
        }, probe.getAccessId(), lowPriorityThreadPool).orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
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
                    closeAccess(accessId);
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
        CompletableFuture[] contexts = new CompletableFuture[files.size()];
        var keys = files.keys();
        var size = keys.length;

        for (int i = 0; i < size; i++)
        {
            contexts[i] = closeAccessAsync(keys[i]);
        }

        CompletableFuture.allOf(contexts).get((long) TIMEOUT_SECONDS * size, TimeUnit.SECONDS); // todo close files and release pools
        closeAndRegisterThreadPool.shutdown();
        lowPriorityThreadPool.shutdown();

        boolean closed1 = closeAndRegisterThreadPool.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        boolean closed2 = lowPriorityThreadPool.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        if (!closed1)
        {
            throw new IllegalStateException("Cannot close pool " + "closeAndRegisterThreadPool");
        }

        if (!closed2)
        {
            throw new IllegalStateException("Cannot close pool " + "lowPriorityThreadPool");
        }

        assert files.size() == 0;
    }

    private void preTouch(FileAccessContext accessContext)
    {
        if (accessContext.getQueueConfiguration().isPreTouch())
            PreToucher.preTouch(accessContext.getBuffer());
    }
}
