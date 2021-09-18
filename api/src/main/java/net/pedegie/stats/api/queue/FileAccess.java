package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import net.openhft.chronicle.core.OS;

import java.io.Closeable;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

@FieldDefaults(level = AccessLevel.PROTECTED, makeFinal = true)
class FileAccess implements Closeable
{
    ReentrantLock resizeLock = new ReentrantLock();
    ProbeWriter probeWriter;
    @Getter
    long startCycleMillis;

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

    FileAccess(Path filePath, int mmapSize, Function<FileAccessContext, ProbeWriter> probeWriterFactory, long startCycleMillis)
    {
        FileUtils.createFile(filePath);
        this.filePath = filePath;
        this.mmapSize = mmapSize;
        this.startCycleMillis = startCycleMillis;
        this.mappedFileBuffer = mmap(this.filePath, this.fileSize, this.mmapSize);
        this.bufferLimit = this.mappedFileBuffer.limit();

        var accessContext = new FileAccessContext(mappedFileBuffer, bufferOffset, filePath);
        this.probeWriter = probeWriterFactory.apply(accessContext);
        PreToucher.preTouch(mappedFileBuffer);
        OS.memory().storeFence();
    }

    public void writeProbe(int probe, long timestamp)
    {
        int nextProbeOffset = nextOffset();
        int offsetAfterWriting = nextProbeOffset + probeWriter.probeSize();
        if (offsetAfterWriting >= bufferLimit || offsetAfterWriting < 0)
        {
            if (resizeLock.tryLock())
            {
                try
                {
                    resize();
                    probeWriter.writeProbe(mappedFileBuffer, 0, probe, timestamp);
                } finally
                {
                    resizeLock.unlock();
                }
            }
            // drop probes during resize
        } else
        {
            probeWriter.writeProbe(mappedFileBuffer, nextProbeOffset, probe, timestamp);
        }
    }

    private void resize()
    {
        this.fileSize += bufferLimit - (bufferLimit % probeWriter.probeSize());
        close(fileSize);
        this.mappedFileBuffer = mmap(filePath, fileSize, mmapSize);
        PreToucher.preTouch(mappedFileBuffer);
        this.bufferOffset.set(probeWriter.probeSize());
    }

    @SneakyThrows
    private ByteBuffer mmap(Path filePath, long offset, int size)
    {
        this.fileAccess = new RandomAccessFile(filePath.toFile(), "rw");
        this.channel = fileAccess.getChannel();
        return channel.map(FileChannel.MapMode.READ_WRITE, offset, size);
    }

    private int nextOffset()
    {
        return bufferOffset.getAndAdd(probeWriter.probeSize());
    }

    @Override
    public void close()
    {
        close(fileSize + bufferOffset.get());
    }

    @SneakyThrows
    private void close(long truncate)
    {
        resizeLock.lock();
        try
        {
            if(closed())
            {
                return;
            }

            channel.truncate(truncate);
            fileAccess.close();

            fileAccess = null;
            mappedFileBuffer = null;
            channel = null;

            boolean unmapOnClose = true; // todo
            if (unmapOnClose)
            {
                System.gc();
            }
        } finally
        {
            resizeLock.unlock();
        }
    }

    private boolean closed()
    {
        return fileAccess == null;
    }
}
