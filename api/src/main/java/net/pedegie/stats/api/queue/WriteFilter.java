package net.pedegie.stats.api.queue;

@FunctionalInterface
public interface WriteFilter
{
    boolean shouldWrite(int size);

    static WriteFilter acceptAllFilter()
    {
        return size -> true;
    }

    static WriteFilter acceptWhenSizeHigherThan(int size)
    {
        return sizee -> sizee > size;
    }
}
