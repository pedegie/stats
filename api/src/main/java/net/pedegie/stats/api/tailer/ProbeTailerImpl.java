package net.pedegie.stats.api.tailer;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.pedegie.stats.api.queue.probe.ProbeAccess;
import net.pedegie.stats.api.queue.probe.ProbeHolder;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
class ProbeTailerImpl implements ProbeTailer
{
    @Getter
    Tailer tailer;
    SingleChronicleQueue chronicleQueue;
    ExcerptTailer chronicleTailer;
    ProbeAccess probeAccess;
    private final ProbeHolder probe = new ProbeHolder();

    public ProbeTailerImpl(TailerConfiguration tailerConfiguration)
    {
        this.chronicleQueue = SingleChronicleQueueBuilder
                .binary(tailerConfiguration.getPath())
                .blockSize(tailerConfiguration.getMmapSize())
                .build();

        this.tailer = tailerConfiguration.getTailer();
        this.chronicleTailer = chronicleQueue.createTailer(tailerConfiguration.getPath().toString());
        this.probeAccess = tailerConfiguration.getProbeAccess();
    }

    @Override
    public boolean read(long amount)
    {
        while (amount-- > 0)
        {
            if (!chronicleTailer.readBytes(bytes -> probeAccess.readProbeInto(bytes, probe)))
                break;
            tailer.onProbe(probe);
        }
        return amount == -1;
    }

    @Override
    public void read()
    {
        while (chronicleTailer.readBytes(bytes -> probeAccess.readProbeInto(bytes, probe)))
            tailer.onProbe(probe);
    }

    @Override
    public void readFromStart()
    {
        var fromStartTailer = chronicleTailer.toStart();
        while (fromStartTailer.readBytes(bytes -> probeAccess.readProbeInto(bytes, probe)))
            tailer.onProbe(probe);
    }

    @Override
    public void close()
    {
        chronicleQueue.close();
    }

    @Override
    public long probes()
    {
        var currentIndex = chronicleTailer.index();
        var startIndex = currentIndex == 0 ? chronicleTailer.toStart().index() : currentIndex;
        var endIndex = chronicleTailer.toEnd().index();
        chronicleTailer.moveToIndex(startIndex);
        return chronicleQueue.countExcerpts(startIndex, endIndex);
    }

    @Override
    public boolean isClosed()
    {
        return chronicleQueue.isClosed();
    }
}
