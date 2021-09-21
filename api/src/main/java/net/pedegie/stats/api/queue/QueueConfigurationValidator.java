package net.pedegie.stats.api.queue;

import net.openhft.chronicle.core.OS;

import java.util.Objects;

class QueueConfigurationValidator
{
    private static final String EXCEPTION_HEADER = "Wrong configuration of " + QueueConfigurationValidator.class.getName() + "\n";

    public static void validate(QueueConfiguration queueConfiguration)
    {
        Objects.requireNonNull(queueConfiguration);
        Objects.requireNonNull(queueConfiguration.getPath());

        if (queueConfiguration.getMmapSize() != 0 && queueConfiguration.getMmapSize() < OS.pageSize())
        {
            throw new IllegalArgumentException(EXCEPTION_HEADER + "mmapSize: " + queueConfiguration.getMmapSize() + " cannot be less than page size: " + OS.pageSize());
        }

        if (queueConfiguration.getFileCycleDurationInMillis() < 60_000)
            throw new IllegalArgumentException(EXCEPTION_HEADER + "'fileCycleDuration' cannot be less than 1 minute");
    }
}