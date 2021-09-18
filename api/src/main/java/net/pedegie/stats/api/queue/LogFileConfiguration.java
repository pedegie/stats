package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.With;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.function.Function;

@Builder
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@With
public class LogFileConfiguration
{
    private static final Duration DEFAULT_CYCLE_DURATION = Duration.of(1, ChronoUnit.DAYS);
    @Getter
    Path path;
    @Getter
    int mmapSize;
    Duration fileCycleDuration;
    @NonFinal
    Clock fileCycleClock;
    @Getter
    boolean disableCompression;
    @Getter
    Function<FileAccessContext, ProbeWriter> probeWriter;

    public Clock getFileCycleClock()
    {
        return fileCycleClock == null ? fileCycleClock = Clock.systemDefaultZone() : fileCycleClock;
    }

    public long getFileCycleDurationInMillis()
    {
        return (fileCycleDuration == null ? DEFAULT_CYCLE_DURATION : fileCycleDuration).getSeconds() * 1000;
    }
}
