package net.pedegie.stats.api.tailer;

import lombok.SneakyThrows;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.threads.MediumEventLoop;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.threads.VanillaEventLoop;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class ProbeTailerScheduler
{
    private static final int DEFAULT_PROBES_READ_ON_SINGLE_ACTION = 50;
    private static final int DEFAULT_THREADS = 1;

    private final List<ProbeTailer> tailers = new ArrayList<>(16);

    private final int probeReadOnSingleAction;
    private final MediumEventLoop[] threadLoops;
    int roundRobinIndex = 0;

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
        threadLoops[roundRobinIndex++ % threadLoops.length].addHandler(() -> tailer.read(probeReadOnSingleAction));
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

    @SneakyThrows
    private void close(ProbeTailer tailer)
    {
        tailer.close();
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
