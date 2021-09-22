package net.pedegie.stats.api.queue;

import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

@Slf4j
class CrashRecovery
{
    public static int recover(FileAccessContext accessContext, Recoverable recoverable)
    {
        var buffer = accessContext.getBuffer();
        var limitMark = buffer.limit();

        if (needRecovery(buffer, recoverable))
        {
            if (bufferNotContainsEvenSingleProbe(buffer, recoverable))
            {
                log.warn("Found broken probes, there is not even single probe in buffer. Truncating to index 0");
                buffer.limit(0);
            } else
            {
                log.warn("Found broken probe at index: {}, probe: {}. Truncating to first correct probe.", buffer.limit(), fetchProbe(recoverable, buffer));

                buffer.limit(adjustToProbeSize(recoverable, buffer));
                while (buffer.limit() > recoverable.headerSize() && needRecovery(buffer, recoverable)) ;
            }
        }
        var index = buffer.limit();
        buffer.limit(limitMark);
        return index;
    }

    private static String fetchProbe(Recoverable recoverable, ByteBuffer buffer)
    {
        var probe = new StringBuilder(recoverable.probeSize() * Byte.SIZE);
        for (int i = buffer.limit() - recoverable.probeSize(); i < buffer.limit(); i++)
        {
            probe.append(Integer.toBinaryString(buffer.get(i) & 255 | 256).substring(1));
        }

        return probe.toString();
    }

    private static boolean needRecovery(ByteBuffer buffer, Recoverable recoverable)
    {
        var index = FileUtils.findFirstFreeIndex(buffer);
        var bufferLimitTheSameAsIndex = index == buffer.limit();
        buffer.limit(index);

        if (bufferNotContainsEvenSingleProbe(buffer, recoverable) || itsNotDivisibleByProbeSize(buffer, recoverable))
        {
            return true;
        }
        var needRecovery = !recoverable.correctProbeOnLastPosition(buffer);

        if (needRecovery && bufferLimitTheSameAsIndex)
        {
            throw new IllegalStateException("Infinity loop");
        }

        return needRecovery;
    }

    private static boolean bufferNotContainsEvenSingleProbe(ByteBuffer buffer, Recoverable recoverable)
    {
        return buffer.limit() < recoverable.probeSize() + recoverable.headerSize();
    }

    private static boolean itsNotDivisibleByProbeSize(ByteBuffer buffer, Recoverable recoverable)
    {
        return (buffer.limit() - recoverable.headerSize()) % recoverable.probeSize() != 0;
    }

    private static int adjustToProbeSize(Recoverable recoverable, ByteBuffer buffer)
    {
        return buffer.limit() - (buffer.limit() % recoverable.probeSize());
    }
}
