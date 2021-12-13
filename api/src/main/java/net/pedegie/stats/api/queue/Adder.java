package net.pedegie.stats.api.queue;

interface Adder
{
    void increment();

    void decrement();

    void add(long size);

    int intValue();
}
