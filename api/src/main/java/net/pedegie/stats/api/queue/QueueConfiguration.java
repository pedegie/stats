package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.pedegie.stats.api.queue.probe.ProbeAccess;
import net.pedegie.stats.api.tailer.Tailer;

import java.nio.file.Path;

@Builder
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class QueueConfiguration
{
    private static final int MB_500 = 1024 * 1024 * 512;

    @Getter
    Path path;
    @Getter
    @Builder.Default
    int mmapSize = MB_500;
    @Getter
    @Builder.Default
    RollCycle rollCycle = RollCycles.DAILY;
    @Getter
    boolean disableCompression;
    @Getter
    boolean disableSynchronization;
    @Getter
    @Builder.Default
    boolean preTouch = true;
    @Getter
    @Builder.Default
    WriteFilter writeFilter = WriteFilter.acceptAllFilter();
    @Builder.Default
    @Getter
    FileAccessErrorHandler errorHandler = FileAccessErrorHandler.logAndIgnore();
    @Getter
    @Builder.Default
    ProbeAccess probeAccess = new ProbeAccess()
    {
    };
    @Getter
    @Builder.Default
    InternalFileAccess internalFileAccess = InternalFileAccess.INSTANCE;
    @Getter
    @Builder.Default
    int delayBetweenWritesMillis = 5000;
    @Getter
    Tailer tailer;

    public QueueConfiguration withTailer(Tailer tailer)
    {
        return new QueueConfiguration(path, mmapSize, rollCycle, disableCompression, disableSynchronization, preTouch, writeFilter, errorHandler, probeAccess, internalFileAccess, delayBetweenWritesMillis, tailer);
    }
}
