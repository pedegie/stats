package net.pedegie.stats.api.queue;

import lombok.extern.slf4j.Slf4j;

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
            var offset = accessContext.getBufferOffset().getAndAdd(HEADER_SIZE);
            if (offset != 0)
            {
                throw new IllegalStateException("Unexpected offset incrementation during initialization, offset is: " + offset + ", should be 0");
            }

            long timestampWithFirstBitSetIndicatingItsCompressedFile = timestamp | Long.MIN_VALUE;
            accessContext.getBuffer().putLong(0, timestampWithFirstBitSetIndicatingItsCompressedFile);
            accessContext.seekTo(HEADER_SIZE);
            log.debug("Initialized new compressed file {}, starting at offset: {} with timestamp: {}", accessContext.getFileName(), 0, startCycleTimestamp);
        } else
        {
            log.debug("Found dirty header: 0x{}", Long.toHexString(timestamp));

            if ((timestamp & Long.MIN_VALUE) == 0)
            {
                throw new CompressedProbeWriterException("First bit not set, it's not CompressedFile");
            }

            timestamp = timestamp & Long.MAX_VALUE;
            if (timestamp != startCycleTimestamp)
            {
                throw new IllegalStateException("Passed timestamp differs from timestamp found in file");
            }

            log.debug("Recover");
            var index = CrashRecovery.recover(accessContext, this);
            accessContext.seekTo(index);
            log.debug("Found compressed file {}, appending to index: {}", accessContext.getFileName(), index);
        }
        this.startCycleTimestamp = startCycleTimestamp;

    }

    private boolean itsNewFile(long timestamp)
    {
        return timestamp == 0;
    }

    @Override
    public void writeProbe(ByteBuffer buffer, int offset, int probe, long timestamp)
    {
        if (probe == 0)
            probe |= Integer.MIN_VALUE;

        var adjustedTimestamp = (int) (timestamp - startCycleTimestamp);
        buffer.putInt(offset, probe);
        buffer.putInt(offset + PROBE_SIZE, adjustedTimestamp);
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
    public boolean correctProbeOnCurrentPosition(ByteBuffer buffer)
    {
        var previousTimestampIsPresent = buffer.getInt(buffer.limit() - TIMESTAMP_SIZE) != 0;
        var previousProbeIsPresent = buffer.getInt(buffer.limit() - (PROBE_AND_TIMESTAMP_BYTES_SUM)) != 0;
        return previousTimestampIsPresent && previousProbeIsPresent;
    }
}
