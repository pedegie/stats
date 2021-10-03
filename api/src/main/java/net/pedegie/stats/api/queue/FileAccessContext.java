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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
class FileAccessContext
{
    private static final int BUSY = 1;
    private static final int FREE = 0;
    @NonFinal
    volatile int state = BUSY;

    @Getter
    AtomicBoolean terminated;

    @Getter
    QueueConfiguration queueConfiguration;
    @Getter
    long nextCycleTimestampMillis;
    @Getter
    Path fileName;
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
                              QueueConfiguration queueConfiguration, int mmapSize, AtomicBoolean terminated)
    {
        this.queueConfiguration = queueConfiguration;
        this.fileName = fileName;
        this.nextCycleTimestampMillis = nextCycleTimestampMillis;
        this.mmapSize = mmapSize;
        mmapNextSlice();
        this.probeWriter = probeWriter.apply(this);
        this.terminated = terminated;
    }

    public void writeProbe(Probe probe)
    {
        probeWriter.writeProbe(buffer, probe);
    }

    boolean writesEnabled()
    {
        return state == FREE;
    }

    void disableWrites()
    {
        state = BUSY;
    }

    public void enableWrites()
    {
        state = FREE;
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
            System.gc();
        }
    }

    void terminate()
    {
        terminated.set(true);
    }
}
