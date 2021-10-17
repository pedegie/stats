package net.pedegie.stats.api.tailer;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.pedegie.stats.api.queue.FileUtils;
import net.pedegie.stats.api.queue.QueueConfiguration;
import net.pedegie.stats.api.queue.probe.Probe;
import net.pedegie.stats.api.queue.probe.ProbeAccess;

import java.io.Closeable;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class ProbeTailer implements Closeable
{
    Tailer tailer;
    SingleChronicleQueue chronicleQueue;
    ExcerptTailer chronicleTailer;
    ProbeAccess probeAccess;
    private final Probe probe = new Probe();

    public ProbeTailer(QueueConfiguration queueConfiguration)
    {
        this.chronicleQueue = SingleChronicleQueueBuilder
                .binary(queueConfiguration.getPath())
                .rollCycle(queueConfiguration.getRollCycle())
                .blockSize(FileUtils.roundToPageSize(queueConfiguration.getMmapSize()))
                .build();
        this.tailer = queueConfiguration.getTailer();
        this.chronicleTailer = chronicleQueue.createTailer(queueConfiguration.getPath().toString());
        this.probeAccess = queueConfiguration.getProbeAccess();
    }

    public long probes() //todo optimize, toEnd and toStart are really expensive methods
    {
        var currentIndex = chronicleTailer.index();
        var probes = chronicleTailer.toEnd().index() - chronicleTailer.toStart().index();
        chronicleTailer.moveToIndex(currentIndex);
        return probes;
    }

    public void readProbes()
    {
        while (true)
        {
            try (DocumentContext dc = chronicleTailer.readingDocument())
            {
                if (dc.isPresent())
                {
                    probeAccess.readProbeInto(dc.wire().bytes(), probe);

                } else
                {
                    break;
                }
            }
            tailer.onProbe(probe);
        }

    }

    public static ProbeTailer from(QueueConfiguration queueConfiguration)
    {
        return new ProbeTailer(queueConfiguration);
    }

    @Override
    public void close()
    {
        chronicleQueue.close();
    }
}
