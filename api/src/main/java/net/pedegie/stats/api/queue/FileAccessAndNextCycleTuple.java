package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Getter
class FileAccessAndNextCycleTuple
{
    FileAccess fileAccess;
    long nextCycleTimestampMillis;
}
