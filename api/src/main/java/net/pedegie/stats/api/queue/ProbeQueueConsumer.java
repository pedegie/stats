package net.pedegie.stats.api.queue;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.threads.Pauser;
import org.jctools.queues.MpscArrayQueue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProbeQueueConsumer
{
    protected final ExecutorService singleThreadPool = Executors.newSingleThreadExecutor();
    protected final MpscArrayQueue<Tuple<Integer, Long>> probeQueue;
    protected final ExcerptAppender fileAppender;
    protected volatile boolean isRunning = true;
    protected final Runnable statsQueueConsumer = consumeLoop();

    public ProbeQueueConsumer(MpscArrayQueue<Tuple<Integer, Long>> probeQueue, ExcerptAppender appender)
    {
        this.probeQueue = probeQueue;
        this.fileAppender = appender;
    }

    public void start()
    {
        singleThreadPool.submit(statsQueueConsumer);
    }

    private Runnable consumeLoop()
    {
        return () ->
        {
            while (!Thread.interrupted() && isRunning)
            {
                Tuple<Integer, Long> probe = probeQueue.poll();
                if (probe == null)
                {
                    Pauser.balancedUpToMillis(5).pause();
                } else
                {
                    System.out.println("WRITIING");
                    fileAppender.writeBytes(b -> b
                            .writeLong(probe.getT2())
                            .writeInt(probe.getT1()));
                }
            }

            System.out.println("ALL EATEN " + probeQueue.size());
            Tuple<Integer, Long> message;
            while ((message = probeQueue.poll()) != null)
            {
                Tuple<Integer, Long> finalMessage = message;
                fileAppender.writeBytes(b -> b
                        .writeLong(finalMessage.getT2())
                        .writeInt(finalMessage.getT1()));
            }

            System.out.println("ALL EATEN " + probeQueue.size());
            isRunning = true;
        };
    }

    public void stop()
    {
        isRunning = false;
        while (!isRunning)
        {
            Pauser.balancedUpToMillis(5).pause();
        }
    }

    public void close() {
        isRunning = false;
        singleThreadPool.shutdown();
        try
        {
            singleThreadPool.awaitTermination(15, TimeUnit.SECONDS);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
}
