package net.pedegie.stats.api.tailer;

import net.pedegie.stats.api.queue.probe.Probe;

@FunctionalInterface
public interface Tailer
{
    void onProbe(Probe probe);
}
