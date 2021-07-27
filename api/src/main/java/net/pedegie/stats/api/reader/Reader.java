package net.pedegie.stats.api.reader;

@FunctionalInterface
public interface Reader<T>
{
    void read(T t);
}
