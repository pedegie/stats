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
    Path path;
    Tailer tailer;
    @Builder.Default
    ProbeAccess probeAccess = ProbeAccess.defaultAccess();
    int mmapSize;
    boolean preTouch;
    @Builder.Default
    int batchSize = 50;
}
