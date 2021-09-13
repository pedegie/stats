package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.Getter;
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
abstract class FileAccess implements Closeable
{
    private static final int MB_500 = 1024 * 1024 * 512;

    ReentrantLock resizeLock = new ReentrantLock();
    @NonFinal
    RandomAccessFile fileAccess;
    @NonFinal
    ByteBuffer mappedFileBuffer;
    @NonFinal
    FileChannel channel;

    AtomicInteger bufferOffset = new AtomicInteger(0);
    @Getter
    Path filePath;
    int mmapSize;
    int bufferLimit;
    @NonFinal
    long fileSize;

    @SneakyThrows
    FileAccess(Path filePath, int mmapSize)
    {
        FileUtils.createFile(filePath);
        this.filePath = filePath;
        this.mmapSize = mmapSize == 0 ? MB_500 : FileUtils.roundToPageSize(mmapSize);
        mmap();
        this.bufferLimit = this.mappedFileBuffer.limit();

        var firstFreeIndex = FileUtils.findFirstFreeIndex(mappedFileBuffer);
        mappedFileBuffer.position(firstFreeIndex);
        bufferOffset.set(firstFreeIndex);
        PreToucher.preTouch(mappedFileBuffer);
    }

    abstract int recordSize();

    abstract void writeProbe(int offset, int probe, long timestamp);

    public void writeProbe(int probe, long timestamp)
    {
        int offset = nextOffset();
        int sumBytes = offset + recordSize();
        if (sumBytes >= bufferLimit || sumBytes < 0)
        {
            if (resizeLock.tryLock())
            {
                try
                {
                    resize();
                    writeProbe(0, probe, timestamp);
                } finally
                {
                    resizeLock.unlock();
                }
            }
            // drop probes during resize
        } else
        {
            writeProbe(offset, probe, timestamp);
        }
    }

    private void resize()
    {
        this.fileSize += bufferLimit - (bufferLimit % recordSize());
        close(fileSize);
        mmap();
        PreToucher.preTouch(mappedFileBuffer);
        this.bufferOffset.set(recordSize());
    }

    @SneakyThrows
    private void mmap()
    {
        this.fileAccess = new RandomAccessFile(filePath.toFile(), "rw");
        this.channel = fileAccess.getChannel();
        this.mappedFileBuffer = channel.map(FileChannel.MapMode.READ_WRITE, fileSize, mmapSize);
    }

    private int nextOffset()
    {
        return bufferOffset.getAndAdd(recordSize());
    }

    @Override
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
}
