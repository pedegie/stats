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
    private static final CompletableFuture<Object> COMPLETED = CompletableFuture.completedFuture(null);
    private static final Tuple<Integer, Semaphore> FILE_ALREADY_EXISTS_TUPLE = new Tuple<>(FILE_ALREADY_EXISTS, null);
    private static final int TIMEOUT_SECONDS = 30;

    static int CLOSE_FILE_MESSAGE_ID = -1;

    ExecutorService closeAndRegisterThreadPool = ThreadPools.singleThreadPool("closeAndRegisterThreadPool");
    ExecutorService lowPriorityThreadPool = ThreadPools.boundedSingleThreadPool("lowPriorityThreadPool");

    TIntObjectMap<FileAccessContext> files;

    FileAccess()
    {
        files = new TIntObjectHashMap<>(8);
    }

    public void writeProbe(Probe probe)
    {
        log.trace("Received probe: {}", probe);

        var fileAccess = files.get(probe.getAccessId());

        if (fileAccess == null)
            return;

        try
        {
            if (isCloseFileMessage(probe))
            {
                closeFile(probe.getAccessId()).orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
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
        } catch (Exception e)
        {
            log.error("Closing file access due to error while processing probe: " + probe, e);
            //todo error handler instead of closing
            closeFile(probe.getAccessId()).orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    private void writeProbeToFile(FileAccessContext fileAccess, Probe probe)
    {
        log.trace("Writing probe: {}", probe);
        fileAccess.writeProbe(probe);
        log.trace("Written probe: {}", probe);
    }

    private boolean needRecycle(FileAccessContext fileAccess, Probe probe)
    {
        return probe.getTimestamp() >= fileAccess.getNextCycleTimestampMillis();
    }

    private CompletableFuture<Object> closeFile(int accessId)
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

                    log.debug("Closing {}", accessId);

                    var accessContext = files.remove(accessId);
                    assert accessContext != null;

                    context.close();
                    context.terminate();
                    log.debug("Closed {}", accessId);
                } finally
                {
                    context.releaseWrites();
                }
            }, closeAndRegisterThreadPool);
        }

        return COMPLETED;
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

            var accessContext = FileAccessStrategy.fileAccess(conf);
            preTouch(accessContext);
            files.put(id, accessContext);

            log.debug("File registered {}", id);
            return new Tuple<>(id, accessContext.getState());

        }, closeAndRegisterThreadPool).exceptionally(throwable ->
        {
            log.error("", throwable);
            return null;
        });
    }

    @SneakyThrows
    private void recycle(FileAccessContext fileAccess, Probe probe)
    {
        fileAccess.acquireWrites();
        asyncWork(fileAccess, () ->
        {
            log.debug("Recycling file {}", probe.getAccessId());
            fileAccess.close();
            FileAccessStrategy.recycle(fileAccess);
            preTouch(fileAccess);
            writeProbeToFile(fileAccess, probe);
            log.debug("File recycled {}", probe.getAccessId());
        }, lowPriorityThreadPool).orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @SneakyThrows
    private void resize(FileAccessContext fileAccess, Probe probe)
    {
        fileAccess.acquireWrites();
        asyncWork(fileAccess, () ->
        {
            log.debug("Resizing file {}", probe.getAccessId());
            fileAccess.close();
            fileAccess.mmapNextSlice();
            preTouch(fileAccess);
            writeProbeToFile(fileAccess, probe);
            log.debug("File resized {}", probe.getAccessId());
        }, lowPriorityThreadPool).orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private CompletableFuture<Object> asyncWork(FileAccessContext fileAccess, Runnable work, Executor pool)
    {
        return CompletableFuture.supplyAsync(() ->
        {
            work.run();
            fileAccess.releaseWrites();
            return null;
        }, pool).exceptionally(throwable ->
        {
            log.error("", throwable);
            fileAccess.releaseWrites();
            return null;
        });
    }

    @SneakyThrows
    public void closeAllBlocking()
    {
        CompletableFuture[] contexts = new CompletableFuture[files.size()];
        var keys = files.keys();
        var size = keys.length;

        for (int i = 0; i < size; i++)
        {
            contexts[i] = closeFile(keys[i]);
        }

        CompletableFuture.allOf(contexts).get((long) TIMEOUT_SECONDS * size, TimeUnit.SECONDS);
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
