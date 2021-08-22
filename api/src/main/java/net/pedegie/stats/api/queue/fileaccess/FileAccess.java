package net.pedegie.stats.api.queue.fileaccess;

import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;

import java.io.Closeable;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

@FieldDefaults(level = AccessLevel.PROTECTED, makeFinal = true)
public class FileAccess implements Closeable
{
    private static final int PROBE_SIZE = 4;
    static final int TIMESTAMP_SIZE = 8;
    private static final int PROBE_AND_TIMESTAMP_BYTES_SUM = PROBE_SIZE + TIMESTAMP_SIZE;

    private static final int MB_500 = 1024 * 1024 * 512;
    private static final int PAGE_SIZE = 4096;

    ReentrantLock resizeLock = new ReentrantLock();
    @NonFinal
    RandomAccessFile fileAccess;
    @NonFinal
    ByteBuffer mappedFileBuffer;
    @NonFinal
    FileChannel channel;

    AtomicInteger bufferOffset = new AtomicInteger(0);

    Path filePath;
    int mmapSize;

    @NonFinal
    long fileSize;

    @SneakyThrows
    public FileAccess(Path filePath, int mmapSize)
    {
        this.fileAccess = new RandomAccessFile(filePath.toFile(), "rw");
        this.filePath = filePath;
        this.mmapSize = mmapSize == 0 ? MB_500 : roundToPageSize(mmapSize);
        this.channel = fileAccess.getChannel();
        this.mappedFileBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, this.mmapSize);
    }

    public void writeProbe(int probe, long timestamp)
    {
        int offset = nextOffset();
        try
        {
            mappedFileBuffer.putInt(offset, probe);
            mappedFileBuffer.putLong(offset + PROBE_SIZE, timestamp);
        } catch (IndexOutOfBoundsException exc)
        {
            int sumBytes = offset + PROBE_AND_TIMESTAMP_BYTES_SUM;
            if (sumBytes >= mappedFileBuffer.limit() || sumBytes < 0)
            {
                if (resizeLock.tryLock())
                {
                    try
                    {
                        resize();
                        offset = nextOffset();
                        mappedFileBuffer.putInt(offset, probe);
                        mappedFileBuffer.putLong(offset + PROBE_SIZE, timestamp);
                    } finally
                    {
                        resizeLock.unlock();
                    }
                }
            } else
            {
                throw exc;
            }
        } catch (NullPointerException e)
        {
            // resizing, ignore
        }
    }

    @SneakyThrows
    private void resize()
    {
        this.fileSize = fileSize + (bufferOffset.get() - PROBE_AND_TIMESTAMP_BYTES_SUM);
        close(fileSize);
        this.fileAccess = new RandomAccessFile(filePath.toFile(), "rw");
        this.channel = fileAccess.getChannel();
        this.mappedFileBuffer = channel.map(FileChannel.MapMode.READ_WRITE, fileSize, mmapSize);
        this.bufferOffset.set(0);

    }

    private int nextOffset()
    {
        return bufferOffset.getAndAdd(PROBE_AND_TIMESTAMP_BYTES_SUM);
    }

    @Override
    @SneakyThrows
    public void close()
    {
        close(fileSize + bufferOffset.get());
    }

    @SneakyThrows
    private void close(long truncate)
    {
        channel.truncate(truncate);
        fileAccess.close();
        boolean unmapOnClose = true; // todo
        if (unmapOnClose)
        {
            fileAccess = null;
            mappedFileBuffer = null;
            channel = null;
            System.gc();
        }
    }

    private static int roundToPageSize(int mmapSize)
    {
        int rounded = mmapSize + PAGE_SIZE - 1 & (-PAGE_SIZE);
        if (rounded < 0)
        {
            return Integer.MAX_VALUE;
        }
        return rounded;
    }

}
