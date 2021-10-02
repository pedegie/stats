package net.pedegie.stats.api.queue;

import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

@Slf4j
class DefaultProbeWriter implements ProbeWriter, Recoverable
{
    static final int PROBE_SIZE = 4;
    static final int TIMESTAMP_SIZE = 8;
    static final int PROBE_AND_TIMESTAMP_BYTES_SUM = PROBE_SIZE + TIMESTAMP_SIZE;

    public DefaultProbeWriter(FileAccessContext accessContext)
    {
        var buffer = accessContext.getBuffer();

        if (itsNotNewBuffer(buffer))
        {
            log.debug("Recover");
            var index = CrashRecovery.recover(accessContext, this);
            buffer.position(index);
        }
    }

    private boolean itsNotNewBuffer(ByteBuffer buffer)
    {
        return buffer.getInt(0) != 0;
    }

    @Override
    public void writeProbe(ByteBuffer buffer, Probe probe)
    {
        var value = probe.getProbe() == 0 ? (probe.getProbe() | Integer.MIN_VALUE) : probe.getProbe();
        buffer.putInt(value);
        buffer.putLong(probe.getTimestamp());
    }

    @Override
    public int probeSize()
    {
        return PROBE_AND_TIMESTAMP_BYTES_SUM;
    }

    @Override
    public int headerSize()
    {
        return 0;
    }

    @Override
    public boolean correctProbeOnLastPosition(ByteBuffer buffer)
    {
        var previousTimestampIsPresent = buffer.getLong(buffer.limit() - TIMESTAMP_SIZE) != 0;
        var previousProbeIsPresent = buffer.getInt(buffer.limit() - (PROBE_AND_TIMESTAMP_BYTES_SUM)) != 0;
        return previousTimestampIsPresent && previousProbeIsPresent;
    }
}
