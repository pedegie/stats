package net.pedegie.stats.api.queue;

import net.openhft.chronicle.core.Jvm;

class BusyWaiter
{
    static void busyWait(double nanos)
    {
        long start = System.nanoTime();
        while (System.nanoTime() - start < nanos)
        {
            Jvm.safepoint();
        }
    }
}
