package io.github.pedegie.stats.api.queue.probe;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;

class DefaultProbeAccess implements ProbeAccess
{
    public static final DefaultProbeAccess INSTANCE = new DefaultProbeAccess();

    @Override
    public void writeProbe(BytesOut<?> batchBytes, int count, long timestamp)
    {
        batchBytes.writeLong(timestamp);
        batchBytes.writeInt(count);
    }

    @Override
    public void readProbeInto(BytesIn<?> batchBytes, ProbeHolder probe)
    {
        probe.setTimestamp(batchBytes.readLong());
        probe.setCount(batchBytes.readInt());
    }

    @Override
    public String toString()
    {
        return this.getClass().getName();
    }
}
