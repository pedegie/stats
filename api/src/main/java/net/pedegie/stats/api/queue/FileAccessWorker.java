package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import org.jctools.queues.MpscArrayQueue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Slf4j
class FileAccessWorker implements Runnable
{
	@NonFinal
	private ExecutorService singleThreadPool;

	private static final byte NOT_RUNNING = 0, RUNNING = 1, SHUTDOWN = 2, FORCE_SHUTDOWN = 3;
	@NonFinal
	private volatile int isRunning = NOT_RUNNING;
	private static final AtomicIntegerFieldUpdater<FileAccessWorker> isRunningFieldUpdater =
			AtomicIntegerFieldUpdater.newUpdater(FileAccessWorker.class, "isRunning");

	MpscArrayQueue<Probe> probes = new MpscArrayQueue<>(2 << 15);
	@NonFinal
	FileAccess fileAccess;

	public void start(InternalFileAccess internalFileAccess)
	{
		if (isRunning())
		{
			return;
		}
		log.debug("STARTING {}", this.getClass().getSimpleName());
		probes.clear();

		fileAccess = new FileAccess(internalFileAccess);
		assert singleThreadPool == null || singleThreadPool.isTerminated();
		singleThreadPool = ThreadPools.singleThreadPool("file-access-worker-main");
		singleThreadPool.execute(this);
	}

	@Override
	public void run()
	{
		runMainLoop();

		if (isForceShutdown())
		{
			fileAccess.closeAllBlocking();
			setNonRunning();
			return;
		}

		Probe probe;
		while ((probe = probes.poll()) != null)
		{
			fileAccess.writeProbe(probe);
		}
		fileAccess.closeAllBlocking();
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

	public CompletableFuture<RegisterFileResponse> registerFile(QueueConfiguration queueConfiguration)
	{
		return fileAccess.registerFile(queueConfiguration);
	}

	/**
	 * Shutdowns worker after it finish processing all pending tasks on its queue
	 */
	public void shutdown()
	{
		if (isRunningFieldUpdater.compareAndSet(this, RUNNING, SHUTDOWN))
			waitUntilTerminated();
	}

	/**
	 * Shutdowns worker after it finish currently processing task. Pending tasks on queue are not handled
	 */
	public void shutdownForce()
	{
		if (isRunningFieldUpdater.compareAndSet(this, RUNNING, FORCE_SHUTDOWN))
			waitUntilTerminated();
	}

	@SneakyThrows
	private void waitUntilTerminated()
	{
		log.trace("Waits until terminated");

		BusyWaiter.busyWait(() -> isRunningFieldUpdater.get(this) == NOT_RUNNING, "waiting for access worker termination");
		fileAccess = null;
		singleThreadPool.shutdown();
		boolean terminated = singleThreadPool.awaitTermination(30, TimeUnit.SECONDS);
		if (!terminated)
		{
			throw new IllegalStateException("Cannot close file-access-worker-main pool");
		}
		log.trace("{} terminated.", this.getClass().getSimpleName());
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
		return isRunningFieldUpdater.getAndSet(this, RUNNING) != NOT_RUNNING;
	}

	private boolean notClosed()
	{
		return isRunningFieldUpdater.get(this) == RUNNING;
	}

	public void close(int fileAccessId)
	{
		sendCloseFileMessage(Probe.closeFileMessage(fileAccessId));
	}

	private void sendCloseFileMessage(Probe closeFileMessage)
	{
		BusyWaiter.busyWait(() -> probes.offer(closeFileMessage), "sending close file message");
	}
}
