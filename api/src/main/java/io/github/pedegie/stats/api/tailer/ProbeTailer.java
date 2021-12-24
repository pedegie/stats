package io.github.pedegie.stats.api.tailer;

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
     * This is really expensive method, it's not as simple as subtracting two indexes. It has to access every each
     * batch because their sizes may vary. Rather used for debugging or testing. You shouldn't rely on it in
     * loops-logic, instead use any of read() methods.
     *
     * @return probes available to read
     */
    long probes();

    boolean isClosed();

    /**
     * {@code ProbeTailer} IS NOT THREAD SAFE!
     * A {@code ProbeTailer} can be created by one thread and might be used by at most one other thread.
     * Sharing a {@code ProbeTailer} across threads is unsafe and will inevitably lead to errors and unspecified behaviour.
     * <p>
     * You can use it within {@link ProbeTailerScheduler}
     *
     * @param tailerConfiguration Probe Tailer Configuration
     * @return {@code ProbeTailer}
     */
    static ProbeTailer from(TailerConfiguration tailerConfiguration)
    {
        return new ProbeTailerImpl(tailerConfiguration);
    }
}
