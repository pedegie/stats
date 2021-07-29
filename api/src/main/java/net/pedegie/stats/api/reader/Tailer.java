package net.pedegie.stats.api.reader;

@FunctionalInterface
public interface Tailer<T, E>
{
    void read(T t, E e);
}
