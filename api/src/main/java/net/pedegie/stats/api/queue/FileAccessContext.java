package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
class FileAccessContext
{
    private static final int CLOSE_ONLY = 5;
    static final int ALL_PERMITS = 4;
    static final int TERMINATED = 6;

    @Getter
    Semaphore state;
    @Getter
    QueueConfiguration queueConfiguration;
    @Getter
    @NonFinal
    long nextCycleTimestampMillis;
    @Getter
    @NonFinal
    Path fileName;
    @NonFinal
    ProbeWriter probeWriter;
    int mmapSize;
    @Getter
    @NonFinal
    ByteBuffer buffer;
    @NonFinal
    RandomAccessFile fileAccess;
    @NonFinal
    FileChannel channel;
    @NonFinal
    long fileSize;

    @Builder
    private FileAccessContext(Path fileName, Function<FileAccessContext, ProbeWriter> probeWriter, long nextCycleTimestampMillis,
                              QueueConfiguration queueConfiguration, int mmapSize)
    {
        this.queueConfiguration = queueConfiguration;
        this.fileName = fileName;
        this.nextCycleTimestampMillis = nextCycleTimestampMillis;
        this.mmapSize = mmapSize;
        mmapNextSlice();
        this.probeWriter = probeWriter.apply(this);
        state = new Semaphore(0);
    }

    public void writeProbe(Probe probe)
    {
        probeWriter.writeProbe(buffer, probe);
    }

    public void enableAccess()
    {
        state.release(ALL_PERMITS);
    }

    boolean writesEnabled()
    {
        return state.availablePermits() == ALL_PERMITS;
    }

    void acquireWrites()
    {
        assert state.tryAcquire(1);
    }

    @SneakyThrows
    boolean acquireWritesBlocking(long timeout, TimeUnit timeUnit)
    {
        return state.tryAcquire(timeout, timeUnit);
    }

    public void releaseWrites()
    {
        state.release(1);
    }

    boolean acquireClose()
    {
        return state.tryAcquire(3);
    }

    public void closeOnly()
    {
        setState(CLOSE_ONLY);
    }

    public void terminate()
    {
        setState(TERMINATED);
    }

    private void setState(int newState)
    {
        state.release(newState - state.availablePermits());
    }

    public void mmapNextSlice()
    {
        mmapNextSlice(fileName);
    }

    @SneakyThrows
    private void mmapNextSlice(Path path)
    {
        this.fileAccess = new RandomAccessFile(path.toFile(), "rw");
        this.channel = fileAccess.getChannel();
        this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, fileSize, mmapSize);
    }

    public boolean needResize()
    {
        return buffer.limit() - buffer.position() < probeWriter.probeSize();
    }

    @SneakyThrows
    void close() // idempotent
    {
        if (buffer != null)
        {
            fileSize += buffer.position();
            buffer = null;
        }
        if (channel != null)
        {
            channel.truncate(fileSize);
            channel = null;
        }
        if (fileAccess != null)
        {
            fileAccess.close();
            fileAccess = null;
        }
        if (queueConfiguration.isUnmapOnClose())
        {
            System.gc(); // todo use chronicle unmap so we can just unmap file without GC
        }
    }

    public FileAccessContext setFileName(Path fileName)
    {
        this.fileName = fileName;
        return this;
    }

    public FileAccessContext setNextCycleTimestampMillis(long nextCycleTimestampMillis)
    {
        this.nextCycleTimestampMillis = nextCycleTimestampMillis;
        return this;
    }

    public void reinitialize(Function<FileAccessContext, ProbeWriter> probeWriter)
    {
        this.fileSize = 0;
        mmapNextSlice();
        this.probeWriter = probeWriter.apply(this);
    }

}
