package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Getter
@RequiredArgsConstructor
class FileAccessContext
{
    ByteBuffer buffer;
    AtomicInteger bufferOffset;
    Path fileName;

    void seekTo(int offset)
    {
        buffer.position(offset);
        bufferOffset.set(offset);
    }
}
