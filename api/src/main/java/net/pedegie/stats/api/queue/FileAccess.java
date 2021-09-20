package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.core.OS;

import java.io.Closeable;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

@FieldDefaults(level = AccessLevel.PROTECTED, makeFinal = true)
@Slf4j
class FileAccess implements Closeable
{
    Lock resizeLock;
    ProbeWriter probeWriter;
    @Getter
    long startCycleMillis;

    @NonFinal
    RandomAccessFile fileAccess;
    @NonFinal
    ByteBuffer mappedFileBuffer;
    @NonFinal
    FileChannel channel;

    Counter bufferOffset;
    @Getter
    Path filePath;
    int mmapSize;
    int bufferLimit;
    @NonFinal
    long fileSize;

    @Builder
    FileAccess(Path filePath, int mmapSize, Function<FileAccessContext, ProbeWriter> probeWriterFactory, long startCycleMillis, Synchronizer synchronizer)
    {
        log.info("Creating {}", filePath.toString());
        FileUtils.createFile(filePath);
        this.filePath = filePath;
        this.mmapSize = mmapSize;
        this.startCycleMillis = startCycleMillis;
        this.mappedFileBuffer = mmap(this.filePath, this.fileSize, this.mmapSize);
        this.bufferLimit = this.mappedFileBuffer.limit();
        this.resizeLock = synchronizer.newLock();
        this.bufferOffset = synchronizer.newCounter();

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
                if(!needResize())
                    return;

                log.debug("Next offset ({}) exceeds current bufferLimit ({}). Resizing mmaped file...", nextProbeOffset + probeWriter.probeSize(), bufferLimit);
                try
                {
                    resize();
                    log.debug("mmaped file resized.");
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

    private boolean needResize()
    {
        int offset = bufferOffset.get() + probeWriter.probeSize();
        return offset >= bufferLimit || offset < 0;
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
        withinResizeLock(() -> close(fileSize + bufferOffset.get()));
    }

    @SneakyThrows
    private void close(long truncate)
    {
        withinResizeLock(() -> close1(truncate));
    }

    @SneakyThrows
    private void close1(long truncate)
    {
        if (closed())
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
    }

    private boolean closed()
    {
        return fileAccess == null;
    }

    private void withinResizeLock(Runnable action)
    {
        resizeLock.lock();
        {
            try
            {
                action.run();
            } finally
            {
                resizeLock.unlock();
            }
        }
    }
}
