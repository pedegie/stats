package io.github.pedegie.stats.api.queue;

@FunctionalInterface
public interface WriteFilter
{
    boolean shouldWrite(int size, long timestamp);

    static WriteFilter acceptAll()
    {
        return WriteFilterFactory.acceptAllFilter();
    }

    static WriteFilter acceptWhenSizeHigherThan(int size)
    {
        return WriteFilterFactory.acceptWhenSizeHigherThan(size);
    }
}
