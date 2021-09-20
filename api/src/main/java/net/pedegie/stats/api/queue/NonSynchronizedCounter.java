package net.pedegie.stats.api.queue;

class NonSynchronizedCounter implements Counter
{
    int counter;

    @Override
    public int incrementAndGet()
    {
        return ++counter;
    }

    @Override
    public int decrementAndGet()
    {
        return --counter;
    }

    @Override
    public int addAndGet(int size)
    {
        return counter += size;
    }

    @Override
    public void set(int currentSize)
    {
        counter = currentSize;
    }

    @Override
    public int getAndAdd(int probeSize)
    {
        var tmp = counter;
        counter += probeSize;
        return tmp;
    }

    @Override
    public int get()
    {
        return counter;
    }
}
