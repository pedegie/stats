package net.pedegie.stats.api.queue;

import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.function.Function;

public interface ProbeWriter
{
    void writeProbe(ByteBuffer buffer, int offset, int probe, long timestamp);

    int probeSize();

    static Function<FileAccessContext, ProbeWriter> defaultProbeWriter()
    {
        return DefaultProbeWriter::new;
    }

    static Function<FileAccessContext, ProbeWriter> compressedProbeWriter(ZonedDateTime zonedDateTime)
    {
        return fileAccessContext -> new CompressedProbeWriter(zonedDateTime, fileAccessContext);
    }
}
