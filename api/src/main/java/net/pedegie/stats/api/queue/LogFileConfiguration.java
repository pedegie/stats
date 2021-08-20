package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

import java.nio.file.Path;

@Builder
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Getter
public class LogFileConfiguration
{
    Path path;
    boolean override;
    boolean append;
    boolean newFileWithDate;
}
