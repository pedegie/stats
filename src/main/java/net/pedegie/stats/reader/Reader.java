package net.pedegie.stats.reader;

@FunctionalInterface
public interface Reader<T>
{
    void read(T t);
}
