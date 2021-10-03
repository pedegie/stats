package net.pedegie.stats.api.queue;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.core.Jvm;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@Slf4j
class FileAccess
{
    static int CLOSE_FILE_MESSAGE_ID = -1;

    Executor pool = Executors.newSingleThreadExecutor();
    TIntObjectMap<FileAccessContext> files;

    Counter idSequence;

    FileAccess()
    {
        files = new TIntObjectHashMap<>(8);
        idSequence = Synchronizer.CONCURRENT.newCounter();
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
                closeFile(probe);
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
            closeFile(probe);
        }
    }

    private boolean needRecycle(FileAccessContext fileAccess, Probe probe)
    {
        return probe.getTimestamp() >= fileAccess.getNextCycleTimestampMillis();
    }

    private void closeFile(Probe probe)
    {
        while(true)
        {
            var context = files.get(probe.getAccessId());
            if(context.getTerminated().get())
                return;

            if(context.writesEnabled())
                break;
            else
                busyWait(1e3);
        }

        var accessContext = files.remove(probe.getAccessId());

        if(accessContext.getTerminated().get())
            return;

        asyncWork(accessContext, () ->
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
            var accessContext = FileAccessStrategy.fileAccess(conf);
            PreToucher.preTouch(accessContext.getBuffer());
            var id = idSequence.incrementAndGet();
            files.put(id, accessContext);
            accessContext.enableWrites();
            return new Tuple<>(id, accessContext.getTerminated());
        }, pool).exceptionally(throwable ->
        {
            log.error("", throwable);
            return null;
        });
    }

    private void recycle(FileAccessContext fileAccess, Probe probe)
    {
        asyncWork(fileAccess, () ->
        {
            fileAccess.close();
            var accessContext = FileAccessStrategy.fileAccess(fileAccess.getQueueConfiguration(), fileAccess.getTerminated());
            PreToucher.preTouch(accessContext.getBuffer());
            files.put(probe.getAccessId(), accessContext);
            accessContext.writeProbe(probe);
            return accessContext;
        });
    }

    private void resize(FileAccessContext fileAccess, Probe probe)
    {
        asyncWork(fileAccess, () ->
        {
            fileAccess.close();
            fileAccess.mmapNextSlice();
            PreToucher.preTouch(fileAccess.getBuffer());
            fileAccess.writeProbe(probe);
            return fileAccess;
        });
    }

    private void asyncWork(FileAccessContext fileAccess, Supplier<FileAccessContext> work)
    {
        fileAccess.disableWrites();
        CompletableFuture.supplyAsync(() ->
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

    public void closeAll()
    {
        files.forEachValue(accessContext ->
        {
            while (!accessContext.writesEnabled())
            {
                busyWait(1e3);
            }
            accessContext.close();
            accessContext.terminate();
            return true;
        });

        files.clear();
    }

    private static void busyWait(double nanos)
    {
        long start = System.nanoTime();
        while (System.nanoTime() - start < nanos)
        {
            Jvm.safepoint();
        }
    }
}
