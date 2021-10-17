package net.pedegie.stats.api.queue.probe;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;

public interface ProbeAccess
{
    default void writeProbe(BytesOut<?> mmapedFile, int count, long timestamp)
    {
        mmapedFile.writeInt(count);
        mmapedFile.writeLong(timestamp);
    }

    default void readProbeInto(BytesIn<?> mmapedFile, ProbeHolder probe)
    {
        probe.setCount(mmapedFile.readInt());
        probe.setTimestamp(mmapedFile.readLong());
    }
}
