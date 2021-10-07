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
import java.util.function.Function;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
class FileAccessContext
{
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
        state = new Semaphore(3);
    }

    public void writeProbe(Probe probe)
    {
        probeWriter.writeProbe(buffer, probe);
    }

    boolean writesEnabled()
    {
        return state.availablePermits() == 3;
    }

    int availablePermits()
    {
        return state.availablePermits();
    }

    void acquireWrites()
    {
        acquireWrites(1);
    }

    boolean acquireClose()
    {
        return acquireWrites(2);
    }

    private boolean acquireWrites(int writes)
    {
        return state.tryAcquire(writes);
    }

    public void releaseWrites()
    {
        state.release(1);
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
    void close()
    {
        fileSize += buffer.position();
        channel.truncate(fileSize);
        fileAccess.close();
        this.channel = null;
        this.fileAccess = null;

        if (queueConfiguration.isUnmapOnClose())
        {
            System.gc(); // todo use chronicle unmap so we can just unmap file without GC
        }
    }

    void terminate()
    {
        state.release(4);
    }

    boolean isTerminated()
    {
        return state.availablePermits() >= 4;
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
