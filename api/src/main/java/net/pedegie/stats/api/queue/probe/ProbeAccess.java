package net.pedegie.stats.api.queue.probe;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;

public interface ProbeAccess
{
    void writeProbe(BytesOut<?> mmapedFile, int count, long timestamp);

    void readProbeInto(BytesIn<?> mmapedFile, ProbeHolder probe);

    static ProbeAccess defaultAccess()
    {
        return DefaultProbeAccess.INSTANCE;
    }
}
