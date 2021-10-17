package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Getter
public class FlushThreshold
{
    private static final long DEFAULT_DELAY_MILLIS = 5000;
    private static final int DEFAULT_MIN_SIZE_DIFFERENCE = 1;
    private static final FlushThreshold DEFAULT = FlushThreshold.of(DEFAULT_DELAY_MILLIS, DEFAULT_MIN_SIZE_DIFFERENCE);

    long delayBetweenWritesMillis;
    int minSizeDifference;

    public static FlushThreshold delayBetweenWrites(long delayBetweenWritesMillis)
    {
        return new FlushThreshold(delayBetweenWritesMillis, DEFAULT_MIN_SIZE_DIFFERENCE);
    }

    public static FlushThreshold minSizeDifference(int minSizeDifference)
    {
        return new FlushThreshold(DEFAULT_DELAY_MILLIS, minSizeDifference);
    }

    public static FlushThreshold of(long delayBetweenWritesMillis, int minSizeDifference)
    {
        return new FlushThreshold(delayBetweenWritesMillis, minSizeDifference);
    }

    static FlushThreshold defaultThreshold()
    {
        return DEFAULT;
    }

    static FlushThreshold flushOnEachWrite()
    {
        return FlushThreshold.of(0, 1);
    }
}
