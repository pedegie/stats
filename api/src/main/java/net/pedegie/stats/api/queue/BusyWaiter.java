package net.pedegie.stats.api.queue;

import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.core.Jvm;

import java.util.function.BooleanSupplier;

@Slf4j
class BusyWaiter
{
    public static void busyWait(BooleanSupplier condition)
    {
        double waits = 0;
        int seconds = 1;

        while(condition.getAsBoolean())
        {
            busyWait(1e3);
            waits += 1e3;

            if(waits >= 1e9)
            {
                log.warn("Busy wait exceeds {} seconds", seconds++);
                waits = 0;
            }
        }
    }

    private static void busyWait(double nanos)
    {
        long start = System.nanoTime();
        while (System.nanoTime() - start < nanos)
        {
            Jvm.safepoint();
        }
    }
}
