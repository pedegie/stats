package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.FieldDefaults;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
@Getter
@ToString
public class Batching
{
    private static final long DEFAULT_FLUSH_MILLIS_THRESHOLD = 5000;
    private static final Batching DEFAULT_BATCHING = new Batching(50, DEFAULT_FLUSH_MILLIS_THRESHOLD);

    int batchSize;
    long flushMillisThreshold;

    Batching(int batchSize)
    {
        this.batchSize = batchSize;
        this.flushMillisThreshold = DEFAULT_FLUSH_MILLIS_THRESHOLD;
    }

    public static Batching defaultConfiguration()
    {
        return DEFAULT_BATCHING;
    }
}
