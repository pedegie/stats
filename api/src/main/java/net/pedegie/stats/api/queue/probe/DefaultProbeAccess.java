package net.pedegie.stats.api.queue.probe;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;

class DefaultProbeAccess implements ProbeAccess
{
    public static final DefaultProbeAccess INSTANCE = new DefaultProbeAccess();

    @Override
    public void writeProbe(BytesOut<?> mmapedFile, int count, long timestamp)
    {
        mmapedFile.writeInt(count);
        mmapedFile.writeLong(timestamp);
    }

    @Override
    public void readProbeInto(BytesIn<?> mmapedFile, ProbeHolder probe)
    {
        probe.setCount(mmapedFile.readInt());
        probe.setTimestamp(mmapedFile.readLong());
    }

    @Override
    public void batchWrite(Bytes<?> bytes, Bytes<?> batchBytes)
    {
        bytes.write(batchBytes);
    }
}
