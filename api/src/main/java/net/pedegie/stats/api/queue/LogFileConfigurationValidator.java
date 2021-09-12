package net.pedegie.stats.api.queue;

import net.openhft.chronicle.core.OS;

import java.util.Objects;

class LogFileConfigurationValidator
{
    private static final String EXCEPTION_HEADER = "Wrong configuration of " + LogFileConfigurationValidator.class.getName() + "\n";

    public static void validate(LogFileConfiguration logFileConfiguration)
    {
        Objects.requireNonNull(logFileConfiguration);
        Objects.requireNonNull(logFileConfiguration.getPath());

        if (logFileConfiguration.getMmapSize() != 0 && logFileConfiguration.getMmapSize() < OS.pageSize())
        {
            throw new IllegalArgumentException(EXCEPTION_HEADER + "mmapSize: " + logFileConfiguration.getMmapSize() + " cannot be less than page size: " + OS.pageSize());
        }

        if (logFileConfiguration.getFileCycleDurationInMillis() < 60_000)
            throw new IllegalArgumentException(EXCEPTION_HEADER + "'fileCycleDuration' cannot be less than 1 minute");
    }
}
