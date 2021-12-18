package net.pedegie.stats.api.tailer;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.threads.MediumEventLoop;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.threads.VanillaEventLoop;

import java.util.EnumSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Slf4j
public class ProbeTailerScheduler
{
    private static final int DEFAULT_PROBES_READ_ON_SINGLE_ACTION = 50;
    private static final int DEFAULT_THREADS = 1;

    CopyOnWriteArrayList<ProbeTailer> tailers = new CopyOnWriteArrayList<>();
    int probeReadOnSingleAction;
    MediumEventLoop[] threadLoops;
    AtomicInteger roundRobinIndex = new AtomicInteger();

    private ProbeTailerScheduler(int threads, int probeReadOnSingleAction)
    {
        System.setProperty("disable.thread.safety", "true");
        this.probeReadOnSingleAction = probeReadOnSingleAction;
        this.threadLoops = new MediumEventLoop[threads];
        for (int i = 0; i < threads; i++)
        {
            MediumEventLoop eventLoop = new VanillaEventLoop(null,
                    "probe_event_loop_" + (i + 1),
                    Pauser.balanced(),
                    100,
                    false,
                    "none",
                    EnumSet.of(HandlerPriority.MEDIUM));
            threadLoops[i] = eventLoop;
            eventLoop.start();
        }
    }

    public void addTailer(ProbeTailer tailer)
    {
        threadLoops[roundRobinIndex.getAndIncrement() % threadLoops.length].addHandler(() -> tailer.read(probeReadOnSingleAction));
        tailers.add(tailer);
    }

    public void close()
    {
        for (MediumEventLoop threadLoop : threadLoops)
        {
            threadLoop.close();
        }

        tailers.forEach(this::close);
    }

    private void close(ProbeTailer tailer)
    {
        try
        {
            tailer.close();
        } catch (Exception e)
        {
            log.error("Error during closing ProbeTailer", e);
        }
    }

    public static ProbeTailerScheduler create()
    {
        return create(DEFAULT_THREADS, DEFAULT_PROBES_READ_ON_SINGLE_ACTION);
    }

    public static ProbeTailerScheduler create(int threads)
    {
        return create(threads, DEFAULT_PROBES_READ_ON_SINGLE_ACTION);
    }

    public static ProbeTailerScheduler create(int threads, int probeReadOnSingleAction)
    {
        return new ProbeTailerScheduler(threads, probeReadOnSingleAction);
    }
}
