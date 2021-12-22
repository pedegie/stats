package io.github.pedegie.stats.api.queue.probe;

import io.github.pedegie.stats.api.tailer.ProbeTailer;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;

public interface ProbeAccess
{
    /**
     * After generating timestamp and checking Collection size this method is used during writing probe to Bytes
     * representing batched data, later flushed by {@link #batchWrite(Bytes, Bytes)} to file. Serialization logic
     * goes here.
     *
     * @param batchBytes batched {@code Bytes}
     * @param count      current {@link java.util.Collection#size()}
     * @param timestamp  in milliseconds representing time when {@link java.util.Collection#size()} was equal to {@code count}
     */
    void writeProbe(BytesOut<?> batchBytes, int count, long timestamp);

    /**
     * After reading slice of memory mapped file to {@code batchBytes} this method is responsible for deserialization
     * bytes to {@link Probe}
     *
     * @param batchBytes batched slice of {@code Bytes} from file
     * @param probe      {@link ProbeHolder} mutable {@link Probe}
     */
    void readProbeInto(BytesIn<?> batchBytes, ProbeHolder probe);

    /**
     * The only method which directly access memory mapped file. Invoked when writing probes exceeds batch threshold.
     * Makes data visible to {@link ProbeTailer} for reading
     *
     * @param memoryMappedFile represents file with probes
     * @param batchBytes       batched data, aggregated during {@link #writeProbe(BytesOut, int, long)}
     */
    default void batchWrite(Bytes<?> memoryMappedFile, Bytes<?> batchBytes)
    {
        memoryMappedFile.write(batchBytes);
    }

    static ProbeAccess defaultAccess()
    {
        return DefaultProbeAccess.INSTANCE;
    }
}
