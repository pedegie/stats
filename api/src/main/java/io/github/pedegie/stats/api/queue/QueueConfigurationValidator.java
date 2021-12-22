package io.github.pedegie.stats.api.queue;

import net.openhft.chronicle.core.OS;

import java.util.Objects;

class QueueConfigurationValidator
{
    private static final String EXCEPTION_HEADER = "Wrong configuration of " + QueueConfiguration.class.getName() + "\n";

    public static void validate(QueueConfiguration queueConfiguration)
    {
        Objects.requireNonNull(queueConfiguration);
        Objects.requireNonNull(queueConfiguration.getPath());

        if (queueConfiguration.getMmapSize() < OS.pageSize())
        {
            throw new IllegalArgumentException(EXCEPTION_HEADER + "mmapSize: " + queueConfiguration.getMmapSize() + " cannot be less than page size: " + OS.pageSize());
        }

        if (queueConfiguration.getBatching().getBatchSize() < 1)
        {
            throw new IllegalArgumentException(EXCEPTION_HEADER + "batchSize: " + queueConfiguration.getBatching().getBatchSize() + " cannot be less than 1");
        }

        if (queueConfiguration.getBatching().getFlushMillisThreshold() < 10)
        {
            throw new IllegalArgumentException(EXCEPTION_HEADER + "flushMillisThreshold: " + queueConfiguration.getBatching().getFlushMillisThreshold() + " cannot be less than 10");
        }
    }
}
