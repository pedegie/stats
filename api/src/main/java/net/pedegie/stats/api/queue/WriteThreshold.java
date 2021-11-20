package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Getter
public class WriteThreshold
{
    private static final long DEFAULT_DELAY_MILLIS = 5000;
    private static final int DEFAULT_MIN_SIZE_DIFFERENCE = 1;
    private static final WriteThreshold DEFAULT = WriteThreshold.of(DEFAULT_DELAY_MILLIS, DEFAULT_MIN_SIZE_DIFFERENCE);

    long delayBetweenWritesMillis;
    int minSizeDifference;

    public static WriteThreshold delayBetweenWrites(long delayBetweenWritesMillis)
    {
        return new WriteThreshold(delayBetweenWritesMillis, DEFAULT_MIN_SIZE_DIFFERENCE);
    }

    public static WriteThreshold minSizeDifference(int minSizeDifference)
    {
        return new WriteThreshold(DEFAULT_DELAY_MILLIS, minSizeDifference);
    }

    public static WriteThreshold of(long delayBetweenWritesMillis, int minSizeDifference)
    {
        return new WriteThreshold(delayBetweenWritesMillis, minSizeDifference);
    }

    static WriteThreshold defaultThreshold()
    {
        return DEFAULT;
    }

    static WriteThreshold flushOnEachWrite()
    {
        return WriteThreshold.of(0, 1);
    }
}