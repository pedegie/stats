package net.pedegie.stats.api.queue;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

@Slf4j
class FileAccessStrategy
{
    @SneakyThrows
    public static FileAccessAndNextCycleTuple accept(LogFileConfiguration logFileConfiguration)
    {
        LogFileConfigurationValidator.validate(logFileConfiguration);

        var offsetDateTime = newFileOffset(logFileConfiguration);
        var fileAccess = fileAccess(logFileConfiguration, offsetDateTime);
        var nextCycleTimestampInMillis = offsetDateTime.toInstant().toEpochMilli() + logFileConfiguration.getFileCycleDurationInMillis();
        return new FileAccessAndNextCycleTuple(fileAccess, nextCycleTimestampInMillis);
    }

    private static ZonedDateTime newFileOffset(LogFileConfiguration configuration)
    {

        var time = Instant.now(configuration.getFileCycleClock()).toEpochMilli();
        var startIntervalTimestamp = time - (time % configuration.getFileCycleDurationInMillis());
        return Instant.ofEpochMilli(startIntervalTimestamp)
                .atZone(configuration.getFileCycleClock().getZone())
                .truncatedTo(ChronoUnit.MINUTES);
    }

    private static FileAccess fileAccess(LogFileConfiguration configuration, ZonedDateTime offsetDateTime)
    {
        var fileName = PathDateFormatter.appendDate(configuration.getPath(), offsetDateTime);

        var sizeIsEligibleForCompression = configuration.getFileCycleDurationInMillis() <= Integer.MAX_VALUE;
        var mmapSize = configuration.getMmapSize();
        if (sizeIsEligibleForCompression && !configuration.isDisableCompression())
        {
            try
            {
                return new CompressedFileAccess(offsetDateTime.toInstant().toEpochMilli(), fileName, mmapSize);
            } catch (CompressedFileAccessException exc)
            {
                log.warn("Compressed file overlaps with already existing file {} which wasn't recognized as compressed, creating non-compressed file access", fileName);
                return new DefaultFileAccess(fileName, mmapSize);
            }
        }
        return new DefaultFileAccess(fileName, mmapSize);
    }
}
