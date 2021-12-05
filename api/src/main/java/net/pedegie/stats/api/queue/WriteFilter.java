package net.pedegie.stats.api.queue;

@FunctionalInterface
public interface WriteFilter
{
    boolean shouldWrite(int size, long timestamp);

    static WriteFilter acceptAllFilter()
    {
        return (size, timestamp) -> true;
    }

    static WriteFilter acceptWhenSizeHigherThan(int size)
    {
        return (sizee, timestamp) -> sizee > size;
    }
}
