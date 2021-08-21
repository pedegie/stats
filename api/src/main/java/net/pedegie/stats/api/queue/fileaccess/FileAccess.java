package net.pedegie.stats.api.queue.fileaccess;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

@FieldDefaults(level = AccessLevel.PROTECTED)
public class FileAccess implements Closeable
{
    private static final int INT_SIZE = 4;
    static final int LONG_SIZE = 8;
    private static final int INT_AND_LONG_SIZE_SUM = INT_SIZE + LONG_SIZE;

    RandomAccessFile fileAccess;
    ByteBuffer mappedFileBuffer;
    AtomicInteger fileOffset = new AtomicInteger(0);

    public FileAccess(RandomAccessFile fileAccess, ByteBuffer mappedFileBuffer)
    {
        this.fileAccess = fileAccess;
        this.mappedFileBuffer = mappedFileBuffer;
    }

    public void writeProbe(int probe, long timestamp)
    {
        int offset = nextOffset();
        mappedFileBuffer.putInt(offset, probe);
        mappedFileBuffer.putLong(offset + INT_SIZE, timestamp);
    }

    private int nextOffset()
    {
        return fileOffset.getAndAdd(INT_AND_LONG_SIZE_SUM);
    }

    //todo merging files feature
    public void resetOffset()
    {
        fileOffset.set(0);
    }

    @Override
    public void close() throws IOException
    {
        fileAccess.close();
        boolean unmapOnClose = true; // todo
        if (unmapOnClose)
        {
            fileAccess = null;
            mappedFileBuffer = null;
            System.gc();
        }
    }
}
