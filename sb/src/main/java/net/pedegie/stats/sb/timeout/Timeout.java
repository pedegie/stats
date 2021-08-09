package net.pedegie.stats.sb.timeout;

import java.util.concurrent.TimeUnit;

public class Timeout
{
    private final TimeUnit unit;
    private final long timeout;

    public Timeout(TimeUnit unit, long timeout)
    {
        this.unit = unit;
        this.timeout = timeout;
    }

    public TimeUnit getUnit()
    {
        return unit;
    }

    public long getTimeout()
    {
        return timeout;
    }
}
