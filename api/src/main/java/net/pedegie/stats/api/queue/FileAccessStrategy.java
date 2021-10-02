package net.pedegie.stats.api.queue;

import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.function.Function;

@Slf4j
class FileAccessStrategy
{
    private static final int MB_500 = 1024 * 1024 * 512;

    public static FileAccessContext fileAccess(QueueConfiguration configuration)
    {
        var offsetDateTime = newFileOffset(configuration);
        var fileName = PathDateFormatter.appendDate(configuration.getPath(), offsetDateTime);
        var probeWriter = probeWriter(configuration, offsetDateTime);
        var nextCycleTimestampMillis = offsetDateTime.toInstant().toEpochMilli() + configuration.getFileCycleDuration().getSeconds() * 1000;
        var mmapSize = configuration.getMmapSize() == 0 ? MB_500 : FileUtils.roundToPageSize(configuration.getMmapSize());

        FileUtils.createFile(fileName);

        return FileAccessContext.builder()
                .fileName(fileName)
                .mmapSize(mmapSize)
                .probeWriter(probeWriter)
                .nextCycleTimestampMillis(nextCycleTimestampMillis)
                .queueConfiguration(configuration)
                .build();
    }

    private static ZonedDateTime newFileOffset(QueueConfiguration configuration)
    {
        var time = Instant.now(configuration.getFileCycleClock()).toEpochMilli();
        var startIntervalTimestamp = time - (time % configuration.getFileCycleDurationInMillis());

        return Instant.ofEpochMilli(startIntervalTimestamp)
                .atZone(configuration.getFileCycleClock().getZone())
                .truncatedTo(ChronoUnit.MINUTES);
    }

    private static Function<FileAccessContext, ProbeWriter> probeWriter(QueueConfiguration configuration, ZonedDateTime offsetDateTime)
    {
        if (configuration.getProbeWriter() != null)
        {
            return configuration.getProbeWriter();
        }

        var sizeIsEligibleForCompression = configuration.getFileCycleDurationInMillis() <= Integer.MAX_VALUE;

        if (sizeIsEligibleForCompression && !configuration.isDisableCompression())
        {
            return fileAccessContext ->
            {
                try
                {
                    return new CompressedProbeWriter(offsetDateTime, fileAccessContext);
                } catch (CompressedProbeWriterException exc)
                {
                    log.warn("Compressed file overlaps with already existing file {} which wasn't recognized as compressed, creating non-compressed file access", fileAccessContext.getFileName());
                    return new DefaultProbeWriter(fileAccessContext);
                }
            };
        }
        return ProbeWriter.defaultProbeWriter();
    }
}
