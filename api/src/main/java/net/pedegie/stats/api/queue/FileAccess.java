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
import java.util.function.Supplier;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@Slf4j
class FileAccess
{
    static int CLOSE_FILE_ID = -1;
    static int CLOSE_ALL_FILES_ID = -2;

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
                closeFile(files.remove(probe.getAccessId()));
            } else if (fileAccess.writesEnabled())
            {
                if (needRecycle(fileAccess, probe))
                {
                    recycle(fileAccess, probe.getAccessId());
                } else if (fileAccess.needResize())
                {
                    resize(fileAccess);
                } else
                {
                    fileAccess.writeProbe(probe);
                }
            }
            // drop probes during recycle or resize
        } catch (Exception e)
        {
            log.error("Closing file access due to error while processing probe: " + probe, e);
            closeFile(files.remove(probe.getAccessId()));
        }
    }

    private boolean needRecycle(FileAccessContext fileAccess, Probe probe)
    {
        return probe.getTimestamp() >= fileAccess.getNextCycleTimestampMillis();
    }

    private void closeFile(FileAccessContext accessContext)
    {
        while (!accessContext.writesEnabled())
        {
            busyWait(1e3);
        }

        asyncWork(accessContext, () ->
        {
            accessContext.close();
            return null;
        });
    }

    private boolean isCloseFileMessage(Probe probe)
    {
        return CLOSE_FILE_ID == probe.getProbe();
    }

    public CompletableFuture<Integer> registerFile(QueueConfiguration conf)
    {
        return CompletableFuture.supplyAsync(() ->
        {
            var accessContext = FileAccessStrategy.fileAccess(conf);
            var id = idSequence.incrementAndGet();
            files.put(id, accessContext);
            accessContext.enableWrites();
            return id;
        }, pool);
    }

    private void recycle(FileAccessContext fileAccess, int accessId)
    {
        asyncWork(fileAccess, () ->
        {
            fileAccess.close();
            var accessContext = FileAccessStrategy.fileAccess(fileAccess.getQueueConfiguration());
            files.put(accessId, accessContext);
            return null;
        });
    }

    private void resize(FileAccessContext fileAccess)
    {
        asyncWork(fileAccess, () ->
        {
            fileAccess.close();
            fileAccess.mmapNextSlice();
            return null;
        });
    }

    private void asyncWork(FileAccessContext fileAccess, Supplier<Void> work)
    {
        fileAccess.disableWrites();
        CompletableFuture.supplyAsync(() ->
        {
            work.get();
            PreToucher.preTouch(fileAccess.getBuffer());
            fileAccess.enableWrites();
            return null;
        }, pool);
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
