package net.pedegie.stats.api.queue.probe;

import lombok.extern.slf4j.Slf4j;
import net.pedegie.stats.api.queue.FileAccessContext;
import net.pedegie.stats.api.queue.Probe;

import java.nio.ByteBuffer;
import java.time.ZonedDateTime;

@Slf4j
class CompressedProbeWriter implements ProbeWriter, Recoverable
{
    static final int HEADER_SIZE = 8;
    static final int PROBE_SIZE = 4;
    static final int TIMESTAMP_SIZE = 4;
    static final int PROBE_AND_TIMESTAMP_BYTES_SUM = PROBE_SIZE + TIMESTAMP_SIZE;

    private final long startCycleTimestamp;

    CompressedProbeWriter(ZonedDateTime offsetDateTime, FileAccessContext accessContext)
    {
        var startCycleTimestamp = offsetDateTime.toInstant().toEpochMilli();
        var timestamp = accessContext.getBuffer().getLong(0);
        if (itsNewFile(timestamp))
        {
            timestamp = startCycleTimestamp;
            long timestampWithFirstBitSetIndicatingItsCompressedFile = timestamp | Long.MIN_VALUE;

            accessContext.getBuffer().putLong(timestampWithFirstBitSetIndicatingItsCompressedFile);
            assert accessContext.getBuffer().position() == HEADER_SIZE;
            log.trace("Initialized new compressed file {}, starting at offset: {} with timestamp: {}", accessContext.getFileName(), 0, startCycleTimestamp);
        } else
        {
            log.trace("Found dirty header: 0x{}", Long.toHexString(timestamp));

            if ((timestamp & Long.MIN_VALUE) == 0)
            {
                throw new IllegalStateException("First bit not set, it's not CompressedFile");
            }

            timestamp = timestamp & Long.MAX_VALUE;
            if (timestamp != startCycleTimestamp)
            {
                throw new IllegalStateException("Passed timestamp differs from timestamp found in file");
            }

            log.trace("Recover");
            var index = CrashRecovery.recover(accessContext, this);
            accessContext.getBuffer().position(Math.max(index, HEADER_SIZE));
            log.trace("Found compressed file {}, appending to index: {}", accessContext.getFileName(), index);
        }
        this.startCycleTimestamp = startCycleTimestamp;
        log.trace("{} Initialized", this.getClass().getName());
    }

    private boolean itsNewFile(long timestamp)
    {
        return timestamp == 0;
    }

    @Override
    public void writeProbe(ByteBuffer buffer, Probe probe)
    {
        var value = probe.getProbe() == 0 ? (probe.getProbe() | Integer.MIN_VALUE) : probe.getProbe();
        var adjustedTimestamp = (int) (probe.getTimestamp() - startCycleTimestamp);

        if (adjustedTimestamp == 0)
            adjustedTimestamp |= Integer.MIN_VALUE;

        buffer.putInt(value);
        buffer.putInt(adjustedTimestamp);
    }

    @Override
    public int probeSize()
    {
        return PROBE_AND_TIMESTAMP_BYTES_SUM;
    }

    @Override
    public int headerSize()
    {
        return HEADER_SIZE;
    }

    @Override
    public boolean correctProbeOnLastPosition(ByteBuffer buffer)
    {
        var previousTimestampIsPresent = buffer.getInt(buffer.limit() - TIMESTAMP_SIZE) != 0;
        var previousProbeIsPresent = buffer.getInt(buffer.limit() - (PROBE_AND_TIMESTAMP_BYTES_SUM)) != 0;
        return previousTimestampIsPresent && previousProbeIsPresent;
    }

    static boolean compressedBuffer(ByteBuffer buffer)
    {
        long firstBytes = buffer.getLong(0);
        return firstBytes == 0 || (firstBytes & Long.MIN_VALUE) != 0;
    }
}
