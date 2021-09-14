package net.pedegie.stats.api.queue;

@FunctionalInterface
public interface WriteFilter
{
    boolean shouldWrite(int size, long time);

    static WriteFilter acceptAllFilter()
    {
        return (size, time) -> true;
    }

    static WriteFilter acceptWhenSizeHigherThan(int size)
    {
        return (sizee, time) -> sizee > size;
    }
}
