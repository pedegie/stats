package net.pedegie.stats.api.tailer;

@FunctionalInterface
public interface Tailer<T, E>
{
    void read(T t, E e);
}
