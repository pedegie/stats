package net.pedegie.stats.api.queue;

import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.core.Jvm;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

@Slf4j
public class BusyWaiter
{
    public static boolean busyWait(BooleanSupplier condition, int maxWaitMillis, String conditionDescription)
    {
        double waits = 0;
        long maxWaitNanos = TimeUnit.MILLISECONDS.toNanos(maxWaitMillis);

        while (!Thread.currentThread().isInterrupted() && !condition.getAsBoolean())
        {
            busyWait(1e3);
            waits += 1e3;

            if (waits >= maxWaitNanos)
            {
                log.warn("Busy wait exceeds {} millis for {}", maxWaitMillis, conditionDescription);
                return false;
            }
        }
        return true;
    }

    public static void busyWait(long millis)
    {
        busyWait(millis * 1e6);
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
