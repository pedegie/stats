package net.pedegie.stats.api.queue;

import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.core.Jvm;

import java.util.function.BooleanSupplier;

@Slf4j
public class BusyWaiter
{
    public static boolean busyWait(BooleanSupplier condition, int maxWaitSeconds, String conditionDescription)
    {
        double waits = 0;
        int seconds = 1;

        while (!Thread.currentThread().isInterrupted() && !condition.getAsBoolean())
        {
            busyWait(1e3);
            waits += 1e3;

            if (waits >= 1e9)
            {
                log.warn("Busy wait exceeds {} seconds for {}", seconds++, conditionDescription);
                if (seconds >= maxWaitSeconds)
                {
                    return false;
                }
                waits = 0;
            }
        }
        return true;
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
