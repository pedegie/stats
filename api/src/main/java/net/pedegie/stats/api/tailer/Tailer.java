package net.pedegie.stats.api.tailer;

import net.pedegie.stats.api.queue.probe.Probe;

public interface Tailer
{
    void onProbe(Probe probe);
}
