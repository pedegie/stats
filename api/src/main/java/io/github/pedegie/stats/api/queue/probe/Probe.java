package io.github.pedegie.stats.api.queue.probe;

public interface Probe
{
    /**
     * @return value for this {@code Probe}
     */
    int getCount();

    /**
     * @return timestamp in milliseconds when {@code Probe} was created
     */
    long getTimestamp();

    /**
     * Due to mutability nature of {@code Probe} you should use this method if you are going to store
     * this object somewhere. It makes simple copy.
     *
     * @return copy of this {@code Probe}
     */
    Probe copyForStore();
}
