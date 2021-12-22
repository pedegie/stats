package io.github.pedegie.stats.api.queue

import io.github.pedegie.stats.api.queue.probe.Probe
import io.github.pedegie.stats.api.tailer.Tailer

class TestTailer implements Tailer
{
    List<Probe> probes = new ArrayList<>(100_000)

    @Override
    void onProbe(Probe probe)
    {
        probes.add(probe.copyForStore())
    }
}
