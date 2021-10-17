package net.pedegie.stats.api.queue.probe;

public interface Probe
{
    int getCount();

    long getTimestamp();

    Probe copyForStore();
}
