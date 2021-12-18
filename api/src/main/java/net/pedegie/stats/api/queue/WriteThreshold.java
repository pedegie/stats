package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.FieldDefaults;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Getter
@ToString
public class WriteThreshold
{
    private static final long DEFAULT_DELAY_MILLIS = 5000;
    private static final int DEFAULT_MIN_SIZE_DIFFERENCE = 1;
    private static final WriteThreshold DEFAULT = WriteThreshold.of(DEFAULT_DELAY_MILLIS, DEFAULT_MIN_SIZE_DIFFERENCE);

    long minDelayBetweenWritesMillis;
    int minSizeDifference;

    public static WriteThreshold minDelayBetweenWritesMillis(long minDelayBetweenWritesMillis)
    {
        return new WriteThreshold(minDelayBetweenWritesMillis, DEFAULT_MIN_SIZE_DIFFERENCE);
    }

    public static WriteThreshold minSizeDifference(int minSizeDifference)
    {
        return new WriteThreshold(DEFAULT_DELAY_MILLIS, minSizeDifference);
    }

    public static WriteThreshold of(long minDelayBetweenWritesMillis, int minSizeDifference)
    {
        return new WriteThreshold(minDelayBetweenWritesMillis, minSizeDifference);
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
