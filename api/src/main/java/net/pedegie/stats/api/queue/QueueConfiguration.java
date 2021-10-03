package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.function.Function;

@Builder
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class QueueConfiguration
{
    private static final Duration DEFAULT_CYCLE_DURATION = Duration.of(1, ChronoUnit.DAYS);
    private static final int MB_500 = 1024 * 1024 * 512;

    @Getter
    Path path;
    @Getter
    @Builder.Default
    int mmapSize = MB_500;
    @Getter
    @Builder.Default
    Duration fileCycleDuration = DEFAULT_CYCLE_DURATION;
    @Getter
    @Builder.Default
    Clock fileCycleClock = Clock.systemDefaultZone();
    @Getter
    boolean disableCompression;
    @Getter
    Function<FileAccessContext, ProbeWriter> probeWriter;
    @Getter
    boolean disableSynchronization;
    @Getter
    @Builder.Default
    boolean unmapOnClose = true;
    @Getter
    @Builder.Default
    boolean preTouch = true;
    @Getter
    @Builder.Default
    WriteFilter writeFilter = WriteFilter.acceptAllFilter();

    public long getFileCycleDurationInMillis()
    {
        return fileCycleDuration.getSeconds() * 1000;
    }

    public Synchronizer getSynchronizer()
    {
        return disableSynchronization ? Synchronizer.NON_SYNCHRONIZED : Synchronizer.CONCURRENT;
    }
}
