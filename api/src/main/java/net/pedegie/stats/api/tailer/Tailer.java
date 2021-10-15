package net.pedegie.stats.api.tailer;

import net.pedegie.stats.api.queue.Probe;

@FunctionalInterface
public interface Tailer
{
    void onProbe(Probe probe);
}
