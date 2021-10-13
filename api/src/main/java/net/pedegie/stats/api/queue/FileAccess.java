package net.pedegie.stats.api.queue;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@Slf4j
class FileAccess
{
    static final int FILE_ALREADY_EXISTS = 0;
    static final int ERROR = -2;
    static final int CLOSE_FILE_MESSAGE_ID = -1;

    private static final CompletableFuture<Void> COMPLETED = CompletableFuture.completedFuture(null);
    private final int timeoutMillis;

    ExecutorService closeAndRegisterThreadPool = ThreadPools.singleThreadPool("close-register-pool");
    ExecutorService recycleResizeThreadPool = ThreadPools.boundedSingleThreadPool("recycle-resize-pool");

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
            internalFileAccess.writeProbe(fileAccess, probe);
            log.trace("Written probe: {}", probe);
        } catch (Exception e)
        {
            if (fileAccess.getQueueConfiguration().getErrorHandler().errorOnProbeWrite(e))
            {
                closeAccessAsync(probe.getAccessId());
            }
        }
    }

    private boolean needRecycle(FileAccessContext fileAccess, Probe probe)
    {
        return probe.getTimestamp() >= fileAccess.getNextCycleTimestampMillis();
    }

    public CompletableFuture<RegisterFileResponse> registerFile(QueueConfiguration conf)
    {
        var registerFileTransaction = new RegisterFileTransaction(conf, internalFileAccess, files);

        return TimeoutedFuture
                .supplyAsync(registerFileTransaction, timeoutMillis, closeAndRegisterThreadPool)
                .exceptionally(throwable ->
                {
                    conf.getErrorHandler().errorOnCreatingFile(throwable);
                    return RegisterFileResponse.errorDuringInit();
                });
    }

    private CompletableFuture<Void> closeAccessAsync(int accessId)
    {
        var context = files.get(accessId);
        if (context.acquireClose())
        {
            log.trace("Acquired close for {}", accessId);
            var closeFileTransaction = new CloseFileTransaction(context, timeoutMillis, accessId, internalFileAccess, files);
            return TimeoutedFuture
                    .supplyAsync(closeFileTransaction, timeoutMillis, closeAndRegisterThreadPool)
                    .exceptionally(throwable ->
                    {
                        log.error("Error occurred during closing file {}, file becomes CLOSE_ONLY - " +
                                "you can try to close it again or leave leaked.", accessId);
                        try
                        {
                            context.getQueueConfiguration().getErrorHandler().errorOnClosingFile(throwable);
                        } finally
                        {
                            context.closeOnly();
                        }
                        return null;
                    });
        }
        return COMPLETED;
    }

    private boolean isCloseFileMessage(Probe probe)
    {
        return CLOSE_FILE_MESSAGE_ID == probe.getProbe();
    }

    @SneakyThrows
    private void resize(FileAccessContext fileAccess, Probe probe)
    {
        fileAccess.acquireWrites();
        TimeoutedFuture.supplyAsync(() ->
                {
                    log.debug("Resizing file {}", probe.getAccessId());
                    internalFileAccess.resize(fileAccess);
                    preTouch(fileAccess);
                    log.debug("File resized {}", probe.getAccessId());
                }, timeoutMillis, recycleResizeThreadPool)
                .exceptionally(throwable ->
                {
                    if (fileAccess.getQueueConfiguration().getErrorHandler().errorOnResize(throwable))
                        closeAccessAsync(probe.getAccessId());
                    return null;
                })
                .thenAccept(ignore -> fileAccess.releaseWrites());
    }

    @SneakyThrows
    private void recycle(FileAccessContext fileAccess, Probe probe)
    {
        fileAccess.acquireWrites();
        TimeoutedFuture.supplyAsync(() ->
                {
                    log.debug("Recycling file {}", probe.getAccessId());
                    internalFileAccess.recycle(fileAccess);
                    preTouch(fileAccess);
                    log.debug("File recycled {}", probe.getAccessId());
                }, timeoutMillis, recycleResizeThreadPool)
                .exceptionally(throwable ->
                {
                    if (fileAccess.getQueueConfiguration().getErrorHandler().errorOnRecycle(throwable))
                        closeAccessAsync(probe.getAccessId());
                    return null;
                })
                .thenAccept(ignore -> fileAccess.releaseWrites());
    }

    @SneakyThrows
    public void closeAllBlocking()
    {
        var keys = files.keys();

        if (keys.length != 0)
            log.debug("Closing {} files", keys.length);

        tryCloseBlocking(keys);

        closeAndRegisterThreadPool.shutdown();
        recycleResizeThreadPool.shutdown();

        var size = files.size();
        var timeout = size == 0 ? 100 : timeoutMillis * size;
        boolean closed1 = closeAndRegisterThreadPool.awaitTermination(timeout, TimeUnit.MILLISECONDS);
        size = files.size();
        timeout = size == 0 ? 100 : timeoutMillis * size;
        boolean closed2 = recycleResizeThreadPool.awaitTermination(timeout, TimeUnit.MILLISECONDS);

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
