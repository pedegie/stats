package net.pedegie.stats.api.queue;

import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;

@Slf4j
class CompressedFileAccess extends FileAccess
{
    private static final int HEADER_SIZE = 8;
    static final int PROBE_SIZE = 4;
    static final int TIMESTAMP_SIZE = 4;
    static final int PROBE_AND_TIMESTAMP_BYTES_SUM = PROBE_SIZE + TIMESTAMP_SIZE;

    private final long startCycleTimestamp;

    CompressedFileAccess(long startCycleTimestamp, Path filePath, int mmapSize)
    {
        super(filePath, mmapSize);
        var timestamp = mappedFileBuffer.getLong(0);
        if (timestamp == 0)
        {
            timestamp = startCycleTimestamp;
            var offset = super.bufferOffset.getAndAdd(HEADER_SIZE);
            if (offset != 0)
            {
                throw new IllegalStateException("Unexpected offset incrementation during initialization, offset is: " + offset + ", should be 0");
            }

            long value = timestamp | Long.MIN_VALUE;
            mappedFileBuffer.putLong(offset, value);

            log.debug("Initialized new compressed file, starting at offset: {} with timestamp: {}", 0, startCycleTimestamp);
        } else
        {
            log.debug("Found dirty header: {}", Long.toHexString(timestamp));

            if ((timestamp & Long.MIN_VALUE) == 0)
            {
                throw new CompressedFileAccessException("First bit not set, it's not CompressedFile");
            }

            timestamp = timestamp & Long.MAX_VALUE;
            if (timestamp != startCycleTimestamp)
            {
                throw new IllegalStateException("Passed timestamp differs from timestamp found in file");
            }
            log.debug("Found compressed file, appending to index: {}", super.fileSize + super.bufferOffset.get());
        }
        this.startCycleTimestamp = startCycleTimestamp;
    }

    @Override
    int recordSize()
    {
        return PROBE_AND_TIMESTAMP_BYTES_SUM;
    }

    @Override
    void writeProbe(int offset, int probe, long timestamp)
    {
        var adjustedTimestamp = (int) (timestamp - startCycleTimestamp);
        super.mappedFileBuffer.putInt(offset, probe);
        super.mappedFileBuffer.putInt(offset + PROBE_SIZE, adjustedTimestamp);
    }
}
