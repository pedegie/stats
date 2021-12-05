package net.pedegie.stats.api.queue;

import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.core.Jvm;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

@Slf4j
public class BusyWaiter
{
    public static boolean busyWaitMillis(BooleanSupplier condition, int maxWaitMillis, String conditionDescription)
    {
        long maxWaitNanos = TimeUnit.MILLISECONDS.toNanos(maxWaitMillis);

        long elapsed = 0;
        var start = System.nanoTime();
        while (!(Thread.currentThread().isInterrupted() || condition.getAsBoolean()))
        {
            if (elapsed >= maxWaitNanos)
            {
                log.warn("Busy wait exceeds {} millis for {}", maxWaitMillis, conditionDescription);
                return false;
            }
            elapsed = System.nanoTime() - start;
            Jvm.safepoint();

        }
        return true;
    }

    public static void busyWaitMillis(long millis)
    {
        busyWaitNanos(millis * 1e6);
    }

    public static void busyWaitNanos(double nanos)
    {
        long start = System.nanoTime();
        while (System.nanoTime() - start < nanos)
        {
            Jvm.safepoint();
        }
    }
}
