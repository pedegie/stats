package net.pedegie.stats.api.queue;

public class Probe
{
    private final int count;
    private final long timestamp;

    public Probe(int count, long timestamp)
    {
        this.count = count;
        this.timestamp = timestamp;
    }

    public int getCount()
    {
        return count;
    }

    public long getTimestamp()
    {
        return timestamp;
    }
}
