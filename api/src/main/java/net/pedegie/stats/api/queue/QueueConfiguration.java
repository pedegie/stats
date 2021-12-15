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
    private static final long MB_5 = 1024 * 1024 * 5;

    Path path;
    @Builder.Default
    long mmapSize = MB_5;
    @Builder.Default
    RollCycle rollCycle = RollCycles.DAILY;
    boolean disableCompression;
    boolean disableSynchronization;
    @Builder.Default
    boolean preTouch = true;
    @Builder.Default
    WriteFilter writeFilter = WriteFilter.acceptAllFilter;
    @Builder.Default
    FileAccessErrorHandler errorHandler = FileAccessErrorHandler.logAndIgnore();
    @Builder.Default
    ProbeAccess probeAccess = ProbeAccess.defaultAccess();
    @Builder.Default
    WriteThreshold writeThreshold = WriteThreshold.defaultThreshold();
    @Builder.Default
    Batching batching = Batching.defaultConfiguration();
    @Builder.Default
    InternalFileAccess internalFileAccess = InternalFileAccess.INSTANCE;

}
