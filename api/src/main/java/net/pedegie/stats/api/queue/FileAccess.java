package net.pedegie.stats.api.queue;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@Slf4j
class FileAccess
{
    private static final CompletableFuture<Object> COMPLETED = CompletableFuture.completedFuture(null);
    private static final int TIMEOUT_SECONDS = 30;

    static int CLOSE_FILE_MESSAGE_ID = -1;
    static int FILE_ALREADY_EXISTS = 0;

    Executor pool = Executors.newSingleThreadExecutor();
    Executor registerFilePool = Executors.newSingleThreadExecutor();
    TIntObjectMap<FileAccessContext> files;

    FileAccess()
    {
        files = new TIntObjectHashMap<>(8);
    }

    public void writeProbe(Probe probe)
    {
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
                    fileAccess.writeProbe(probe);
                }
            }
        } catch (Exception e)
        {
            log.error("Closing file access due to error while processing probe: " + probe, e);
            closeFile(probe.getAccessId()).orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    private boolean needRecycle(FileAccessContext fileAccess, Probe probe)
    {
        return probe.getTimestamp() >= fileAccess.getNextCycleTimestampMillis();
    }

    private CompletableFuture<Object> closeFile(int accessId)
    {
        BooleanSupplier waitCondition = () ->
        {
            var context = files.get(accessId);
            return !context.getTerminated().get() && !context.writesEnabled();
        };
        BusyWaiter.busyWait(waitCondition, "closing file");

        var accessContext = files.remove(accessId);
        if (accessContext.getTerminated().get())
            return COMPLETED;

        return asyncWork(accessContext, () ->
        {
            accessContext.close();
            accessContext.terminate();
            return accessContext;
        });
    }

    private boolean isCloseFileMessage(Probe probe)
    {
        return CLOSE_FILE_MESSAGE_ID == probe.getProbe();
    }

    public CompletableFuture<Tuple<Integer, AtomicBoolean>> registerFile(QueueConfiguration conf)
    {
        return CompletableFuture.supplyAsync(() ->
        {
            var id = conf.getPath().hashCode();
            if(files.containsKey(id))
                return new Tuple<Integer, AtomicBoolean>(FILE_ALREADY_EXISTS, null);

            var accessContext = FileAccessStrategy.fileAccess(conf);
            preTouch(accessContext);
            files.put(id, accessContext);
            accessContext.enableWrites();

            return new Tuple<>(id, accessContext.getTerminated());
        }, registerFilePool).exceptionally(throwable ->
        {
            log.error("", throwable);
            return null;
        });
    }

    @SneakyThrows
    private void recycle(FileAccessContext fileAccess, Probe probe)
    {
        asyncWork(fileAccess, () ->
        {
            fileAccess.close();

            var accessContext = FileAccessStrategy.fileAccess(fileAccess.getQueueConfiguration(), fileAccess.getTerminated());
            preTouch(accessContext);
            files.put(probe.getAccessId(), accessContext);
            accessContext.writeProbe(probe);
            return accessContext;
        }).orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @SneakyThrows
    private void resize(FileAccessContext fileAccess, Probe probe)
    {
        asyncWork(fileAccess, () ->
        {
            fileAccess.close();
            fileAccess.mmapNextSlice();
            preTouch(fileAccess);
            fileAccess.writeProbe(probe);
            return fileAccess;
        }).orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private CompletableFuture<Object> asyncWork(FileAccessContext fileAccess, Supplier<FileAccessContext> work)
    {
        fileAccess.disableWrites();
        return CompletableFuture.supplyAsync(() ->
        {
            var accessContext = work.get();
            accessContext.enableWrites();
            return null;
        }, pool).exceptionally(throwable ->
        {
            log.error("", throwable);
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
        assert files.size() == 0;
    }

    private void preTouch(FileAccessContext accessContext)
    {
        if (accessContext.getQueueConfiguration().isPreTouch())
            PreToucher.preTouch(accessContext.getBuffer());
    }
}
