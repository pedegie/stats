package net.pedegie.stats.api.queue.probe;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;

class DefaultProbeAccess implements ProbeAccess
{
    public static final DefaultProbeAccess INSTANCE = new DefaultProbeAccess();

    @Override
    public void writeProbe(BytesOut<?> mmapedFile, int count, long timestamp)
    {
        mmapedFile.writeLong(timestamp);
        mmapedFile.writeInt(count);
    }

    @Override
    public void readProbeInto(BytesIn<?> mmapedFile, ProbeHolder probe)
    {
        probe.setTimestamp(mmapedFile.readLong());
        probe.setCount(mmapedFile.readInt());
    }
}
