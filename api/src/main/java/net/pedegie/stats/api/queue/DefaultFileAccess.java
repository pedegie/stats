package net.pedegie.stats.api.queue;

import java.nio.file.Path;

class DefaultFileAccess extends FileAccess
{
    static final int PROBE_SIZE = 4;
    static final int TIMESTAMP_SIZE = 8;
    static final int PROBE_AND_TIMESTAMP_BYTES_SUM = PROBE_SIZE + TIMESTAMP_SIZE;

    DefaultFileAccess(Path filePath, int mmapSize)
    {
        super(filePath, mmapSize);
    }

    @Override
    int recordSize()
    {
        return PROBE_AND_TIMESTAMP_BYTES_SUM;
    }

    @Override
    void writeProbe(int offset, int probe, long timestamp)
    {
        super.mappedFileBuffer.putInt(offset, probe);
        super.mappedFileBuffer.putLong(offset + PROBE_SIZE, timestamp);
    }
}
