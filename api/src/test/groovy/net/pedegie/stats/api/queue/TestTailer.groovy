package net.pedegie.stats.api.queue

import net.pedegie.stats.api.queue.probe.Probe
import net.pedegie.stats.api.tailer.Tailer

class TestTailer implements Tailer
{
    List<Probe> probes = new ArrayList<>(100_000)

    @Override
    void onProbe(Probe probe)
    {
        probes.add(probe.copyForStore())
    }
}
