package net.pedegie.stats.api.tailer;

import java.io.Closeable;

public interface ProbeTailer extends Closeable
{
    /**
     * Continues reading probes from last read probe for this ProbeTailer
     *
     * @param amount requested {@code amount} of probes to read
     * @return {@code true} if {@code ProbeTailer} was able to read all requested probes, {@code false} otherwise
     */
    boolean read(long amount);

    /**
     * Continues reading probes from last read probe for this {@code ProbeTailer}.
     * Reads all available probes
     */
    void read();

    /**
     * Reads all available probes from beginning of file
     */
    void readFromStart();

    /**
     * @return probes available to read
     */
    long probes();

    boolean isClosed();

    static ProbeTailer from(TailerConfiguration tailerConfiguration)
    {
        return new ProbeTailerImpl(tailerConfiguration);
    }
}
