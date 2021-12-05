package net.pedegie.stats.api.tailer;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.With;
import lombok.experimental.FieldDefaults;
import net.pedegie.stats.api.queue.probe.ProbeAccess;

import java.nio.file.Path;

@Builder
@Getter
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@With
public class TailerConfiguration
{
    private static final long MB_5 = 1024 * 1024 * 5;

    Path path;
    Tailer tailer;
    @Builder.Default
    ProbeAccess probeAccess = ProbeAccess.defaultAccess();
    @Builder.Default
    long mmapSize = MB_5;
    @Builder.Default
    int batchSize = 50;
}
