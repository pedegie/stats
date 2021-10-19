package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.With;
import lombok.experimental.FieldDefaults;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.pedegie.stats.api.queue.probe.ProbeAccess;

import java.nio.file.Path;

@Builder
@Getter
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@With
public class QueueConfiguration
{
    private static final int MB_500 = 1024 * 1024 * 512;

    Path path;
    @Builder.Default
    int mmapSize = MB_500;
    @Builder.Default
    RollCycle rollCycle = RollCycles.DAILY;
    boolean disableCompression;
    boolean disableSynchronization;
    @Builder.Default
    boolean preTouch = true;
    @Builder.Default
    WriteFilter writeFilter = WriteFilter.acceptAllFilter();
    @Builder.Default
    FileAccessErrorHandler errorHandler = FileAccessErrorHandler.logAndIgnore();
    @Builder.Default
    ProbeAccess probeAccess = ProbeAccess.defaultAccess();
    @Builder.Default
    InternalFileAccess internalFileAccess = InternalFileAccess.INSTANCE;
    @Builder.Default
    FlushThreshold flushThreshold = FlushThreshold.defaultThreshold();
}
