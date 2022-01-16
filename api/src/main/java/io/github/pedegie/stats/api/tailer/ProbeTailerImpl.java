package io.github.pedegie.stats.api.tailer;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import io.github.pedegie.stats.api.queue.probe.ProbeAccess;
import io.github.pedegie.stats.api.queue.probe.ProbeHolder;

import static io.github.pedegie.stats.api.queue.probe.ProbeHolder.PROBE_SIZE;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Slf4j
class ProbeTailerImpl implements ProbeTailer
{
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

    static
    {
        System.setProperty("disable.thread.safety", "true");
    }

    public ProbeTailerImpl(TailerConfiguration tailerConfiguration)
    {
        this.chronicleQueue = SingleChronicleQueueBuilder
                .binary(tailerConfiguration.getPath())
                .rollCycle(tailerConfiguration.getRollCycle())
                .blockSize(tailerConfiguration.getMmapSize())
                .build();

        this.tailer = tailerConfiguration.getTailer();
        this.chronicleTailer = chronicleQueue.createTailer(tailerConfiguration.getPath().toString());
        this.currentBatchContext = chronicleTailer.readingDocument();
        this.probeAccess = tailerConfiguration.getProbeAccess();
        this.batchBytes = Bytes.allocateElasticDirect(0);
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
            if (readSingleProbe())
                amount--;
        }
        return amount;
    }

    private boolean hasBatchedSomeData()
    {
        return countProbes(batchBytes) > 0 && batchBytes.readLong(batchBytes.readPosition()) != 0;
    }

    @Override
    public void read()
    {
        while (thereIsSomethingToRead(chronicleTailer))
        {
            readProbesFromBatchBytes();
        }
    }

    @Override
    public void readFromStart()
    {
        var fromStartTailer = chronicleTailer.toStart();
        currentBatchContext = fromStartTailer.readingDocument();
        batchBytes.clear();

        while (thereIsSomethingToRead(fromStartTailer))
        {
            readProbesFromBatchBytes();
        }
    }

    private void readProbesFromBatchBytes()
    {
        while (hasBatchedSomeData())
        {
            readSingleProbe();
        }
    }

    private boolean readSingleProbe()
    {
        var readPosition = batchBytes.readPosition();

        try
        {
            probeAccess.readProbeInto(batchBytes, probe);
            tailer.onProbe(probe);
            return true;
        } catch (Exception e)
        {
            batchBytes.readPosition(readPosition);
            log.error("Error during reading probe.", e);
            return false;
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

            long len = bytes.readRemaining();
            batchBytes.write(bytes, bytes.readPosition(), len);
            bytes.readSkip(len);
        }
        return batchBytes.readLimit() != 0;
    }

    @Override
    public void close()
    {
        readProbesFromBatchBytes();
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
        ExcerptTailer excerptTailer = chronicleQueue.createTailer();
        excerptTailer.moveToIndex(currentIndex);

        long bytes = 0;
        long lastBatchBytes = 0;

        // count all written bytes
        while (true)
        {
            try (DocumentContext dc = excerptTailer.readingDocument())
            {
                if (dc.isPresent())
                {
                    lastBatchBytes = dc.wire().bytes().readRemaining();
                    bytes += lastBatchBytes;
                } else
                {
                    break;
                }
            }
        }
        // substract the last batch bytes, because there is no guarantee it was fully written
        long probes = (bytes - lastBatchBytes) / PROBE_SIZE;
        // count last batch probes linearly, checking if its really fully written or not
        excerptTailer.moveToIndex(excerptTailer.index() - 1);
        probes += countProbesLinearly(excerptTailer.readingDocument().wire().bytes());
        // substract already read probes
        probes -= ((batchBytes.readLimit() - batchBytes.readRemaining()) / PROBE_SIZE);

        return probes;

    }

    private long countProbesLinearly(Bytes<?> bytes)
    {
        var probes = 0;
        for (long i = bytes.readPosition(); i < bytes.readLimit(); i += PROBE_SIZE)
        {
            if (bytes.readLong(i) == 0)
                break;

            probes++;
        }
        return probes;
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
