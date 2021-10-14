package net.pedegie.stats.api.queue;

interface Counter
{
    int incrementAndGet();

    int decrementAndGet();

    int addAndGet(int size);

    void set(int currentSize);

    int get();
}
