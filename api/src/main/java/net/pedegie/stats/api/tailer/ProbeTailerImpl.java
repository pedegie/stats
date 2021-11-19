package net.pedegie.stats.api.tailer;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.pedegie.stats.api.queue.probe.ProbeAccess;
import net.pedegie.stats.api.queue.probe.ProbeHolder;

import static net.pedegie.stats.api.queue.probe.ProbeHolder.PROBE_SIZE;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
class ProbeTailerImpl implements ProbeTailer
{
    @Getter
    Tailer tailer;
    SingleChronicleQueue chronicleQueue;
    ExcerptTailer chronicleTailer;
    ProbeAccess probeAccess;
    ProbeHolder probe = new ProbeHolder();
    Bytes<?> batchBytes;

    @NonFinal
    volatile DocumentContext currentBatchContext;
    @NonFinal
    volatile long perBatchProbes;

    public ProbeTailerImpl(TailerConfiguration tailerConfiguration)
    {
        System.setProperty("disable.thread.safety", "true");

        if (tailerConfiguration.getBatchSize() < 1)
            throw new IllegalArgumentException("batchSize: " + tailerConfiguration.getBatchSize() + " cannot be less than 1");

        this.chronicleQueue = SingleChronicleQueueBuilder
                .binary(tailerConfiguration.getPath())
                .blockSize(tailerConfiguration.getMmapSize())
                .build();

        this.tailer = tailerConfiguration.getTailer();
        this.chronicleTailer = chronicleQueue.createTailer(tailerConfiguration.getPath().toString());
        this.currentBatchContext = chronicleTailer.readingDocument();
        this.probeAccess = tailerConfiguration.getProbeAccess();
        this.batchBytes = Bytes.allocateDirect((long) tailerConfiguration.getBatchSize() * PROBE_SIZE);
        tryToFigureOutPerBatchProbes();
    }

    @Override
    public boolean read(long amount)
    {
        while (readRequestNotFulfilledYet(amount) && thereIsSomethingToRead(chronicleTailer))
        {
            amount = batchRead(amount);
        }

        return amount == 0;
    }

    private boolean readRequestNotFulfilledYet(long amount)
    {
        return amount > 0;
    }

    private long batchRead(long amount)
    {
        while (amount > 0 && hasBatchedSomeData())
        {
            probeAccess.readProbeInto(batchBytes, probe);
            tailer.onProbe(probe);
            amount--;
        }
        return amount;
    }

    private boolean hasBatchedSomeData()
    {
        return countProbes(batchBytes) > 0;
    }

    @Override
    public void read()
    {
        while (thereIsSomethingToRead(chronicleTailer))
        {
            while (hasBatchedSomeData())
            {
                probeAccess.readProbeInto(batchBytes, probe);
                tailer.onProbe(probe);
            }
        }
    }

    @Override
    public void readFromStart()
    {
        var fromStartTailer = chronicleTailer.toStart();
        currentBatchContext = fromStartTailer.readingDocument();

        while (thereIsSomethingToRead(fromStartTailer))
        {
            while (hasBatchedSomeData())
            {
                probeAccess.readProbeInto(batchBytes, probe);
                tailer.onProbe(probe);
            }
        }
    }

    private boolean thereIsSomethingToRead(ExcerptTailer chronicleTailer)
    {
        if (batchBytes.readLimit() == 0 || batchBytes.readPosition() == batchBytes.readLimit())
        {
            batchBytes.clear();

            if (contextNotPresent(chronicleTailer))
                return false;

            Bytes<?> bytes = currentBatchContext.wire().bytes();

            if (bytes.readPosition() == bytes.readLimit())
            {
                currentBatchContext.close();
                if (contextNotPresent(chronicleTailer))
                    return false;
            }

            long len = Math.min(batchBytes.writeRemaining(), bytes.readRemaining());
            batchBytes.write(bytes, bytes.readPosition(), len);
            bytes.readSkip(len);
        }
        return batchBytes.readLimit() != 0;
    }

    @Override
    public void close()
    {
        currentBatchContext.close();
        chronicleQueue.close();
        tailer.onClose();
    }

    @Override
    public long probes()
    {
        if (contextNotPresent(chronicleTailer))
            return 0;

        var currentIndex = chronicleTailer.index();
        var lastIndex = chronicleQueue.createTailer().toEnd().index();
        var batches = chronicleQueue.countExcerpts(currentIndex, lastIndex) - 1;
        tryToFigureOutPerBatchProbes();

        return batches * perBatchProbes + countProbes(batchBytes) + countCurrentContextProbes();
    }

    private boolean contextNotPresent(ExcerptTailer chronicleTailer)
    {
        if (!currentBatchContext.isPresent())
        {
            currentBatchContext = chronicleTailer.readingDocument();
            return !currentBatchContext.isPresent();
        }
        return false;
    }

    private void tryToFigureOutPerBatchProbes()
    {
        if (perBatchProbes == 0)
        {
            var wire = chronicleQueue.createTailer().readingDocument().wire();
            if (wire != null)
            {
                perBatchProbes = countProbes(wire.bytes());
            }
        }
    }

    private long countCurrentContextProbes()
    {
        var wire = currentBatchContext.wire();
        if (wire == null)
            return 0;
        return countProbes(wire.bytes());
    }

    private long countProbes(Bytes<?> bytes)
    {
        return (bytes.readLimit() - bytes.readPosition()) / PROBE_SIZE;
    }

    @Override
    public boolean isClosed()
    {
        return chronicleQueue.isClosed();
    }
}
