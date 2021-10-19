package net.pedegie.stats.api.tailer;

import java.io.Closeable;

public interface ProbeTailer extends Closeable
{
    boolean read(long amount);

    void read();

    void readFromStart();

    long probes();

    static ProbeTailer from(TailerConfiguration tailerConfiguration)
    {
        return new ProbeTailerImpl(tailerConfiguration);
    }
}
