package net.pedegie.stats.api.tailer;

import net.pedegie.stats.api.queue.probe.Probe;

@FunctionalInterface
public interface Tailer
{
    /**
     * {@link Probe} is mutable, use {@link Probe#copyForStore()} if you
     * are going to keep this object somewhere because it's shared for every read
     *
     * @param probe read by {@link ProbeTailer}.
     */
    void onProbe(Probe probe);

    /**
     * Invoked after closing {@link ProbeTailer}
     */
    default void onClose()
    {
    }
}
