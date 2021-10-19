package net.pedegie.stats.api.tailer;

import lombok.SneakyThrows;
import net.openhft.chronicle.threads.MediumEventLoop;
import net.openhft.chronicle.threads.Pauser;

import java.util.ArrayList;
import java.util.List;

public class ProbeTailerScheduler
{
    private static final int DEFAULT_PROBES_READ_ON_SINGLE_ACTION = 50;

    private final List<ProbeTailer> tailers = new ArrayList<>(16);

    private final int probeReadOnSingleAction;
    private final MediumEventLoop[] threadLoops;
    int roundRobinIndex = 0;

    private ProbeTailerScheduler(int threads, int probeReadOnSingleAction)
    {
        this.probeReadOnSingleAction = probeReadOnSingleAction;
        this.threadLoops = new MediumEventLoop[threads];
        for (int i = 0; i < threads; i++)
        {
            MediumEventLoop eventLoop = new MediumEventLoop(null, "probe_event_loop_" + (i + 1), Pauser.balanced(), false, null);
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

    public static ProbeTailerScheduler create(int threads)
    {
        return create(threads, DEFAULT_PROBES_READ_ON_SINGLE_ACTION);
    }

    public static ProbeTailerScheduler create(int threads, int probeReadOnSingleAction)
    {
        return new ProbeTailerScheduler(threads, probeReadOnSingleAction);
    }
}
