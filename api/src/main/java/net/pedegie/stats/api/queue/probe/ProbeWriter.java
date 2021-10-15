package net.pedegie.stats.api.queue.probe;

import net.pedegie.stats.api.queue.FileAccessContext;
import net.pedegie.stats.api.queue.Probe;

import java.nio.ByteBuffer;
import java.time.ZonedDateTime;

public interface ProbeWriter
{
    void writeProbe(ByteBuffer buffer, Probe probe);

    int probeSize();

    static ProbeWriter defaultProbeWriter(FileAccessContext accessContext)
    {
        return new DefaultProbeWriter(accessContext);
    }

    static ProbeWriter compressedProbeWriter(FileAccessContext accessContext, ZonedDateTime zonedDateTime)
    {
        return new CompressedProbeWriter(zonedDateTime, accessContext);
    }

    static boolean isCompressedProbeWriter(ByteBuffer buffer)
    {
        return CompressedProbeWriter.compressedBuffer(buffer);
    }
}
