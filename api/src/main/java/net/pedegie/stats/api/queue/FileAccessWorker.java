package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.core.Jvm;
import org.jctools.queues.MpscArrayQueue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Slf4j
class FileAccessWorker implements Runnable
{
    private static final Executor singleThreadPool = Executors.newSingleThreadExecutor();

    private static final byte NOT_RUNNING = 0, RUNNING = 1, SHUTDOWN = 2, FORCE_SHUTDOWN = 3;
    @NonFinal
    private volatile int isRunning = NOT_RUNNING;
    private static final AtomicIntegerFieldUpdater<FileAccessWorker> isRunningFieldUpdater =
            AtomicIntegerFieldUpdater.newUpdater(FileAccessWorker.class, "isRunning");

    MpscArrayQueue<Probe> probes = new MpscArrayQueue<>(2 << 15);
    FileAccess fileAccess;

    public FileAccessWorker()
    {
        this.fileAccess = new FileAccess();
        singleThreadPool.execute(this);
    }

    @Override
    public void run()
    {
        if (isRunning())
        {
            return;
        }

        runMainLoop();

        if (isForceShutdown())
        {
            fileAccess.closeAll();
            setNonRunning();
            return;
        }

        Probe probe;
        while ((probe = probes.poll()) != null)
        {
            fileAccess.writeProbe(probe);
        }

        setNonRunning();
    }

    private void runMainLoop()
    {
        while (notClosed())
        {
            Probe probe = probes.poll();
            if (null != probe)
            {
                fileAccess.writeProbe(probe);
            }
        }
    }

    public void writeProbe(Probe probe)
    {
        probes.failFastOffer(probe);
    }

    public CompletableFuture<Integer> registerFile(QueueConfiguration queueConfiguration)
    {
        return fileAccess.registerFile(queueConfiguration);
    }

    /**
     * Shutdowns worker after it finish processing all pending tasks on its queue
     */
    private void shutdown()
    {
        isRunningFieldUpdater.compareAndSet(this, RUNNING, SHUTDOWN);
    }

    /**
     * Shutdowns worker after it finish currently processing task. Pending tasks on queue are not handled
     */
    private void shutdownForce()
    {
        isRunningFieldUpdater.compareAndSet(this, RUNNING, FORCE_SHUTDOWN);
    }

    private void setNonRunning()
    {
        isRunningFieldUpdater.set(this, NOT_RUNNING);
    }

    private boolean isForceShutdown()
    {
        return isRunningFieldUpdater.get(this) == FORCE_SHUTDOWN;
    }

    private boolean isRunning()
    {
        return isRunningFieldUpdater.getAndSet(this, RUNNING) == RUNNING;
    }

    private boolean notClosed()
    {
        return isRunningFieldUpdater.get(this) == RUNNING;
    }

    public void close(int fileAccessId)
    {
        var closeFileMessage = Probe.closeFileMessage(fileAccessId);
        while (!probes.offer(closeFileMessage))
        {
            log.warn("Send queue full, cannot send close file message for {}", fileAccessId);
            busyWait(1e3);
        }
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
