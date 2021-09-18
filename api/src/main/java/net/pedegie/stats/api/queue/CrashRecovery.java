package net.pedegie.stats.api.queue;

import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

@Slf4j
class CrashRecovery
{
    private static final int MAGIC_NUMBER = 4;

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
                log.warn("Found broken probe at index {}. Truncating to first correct probe.", buffer.limit());

                buffer.limit(adjustToProbeSize(recoverable, buffer));
                while (buffer.limit() > recoverable.headerSize() && needRecovery(buffer, recoverable)) ;
            }
        }
        var index = buffer.limit();
        buffer.limit(limitMark);
        return Math.max(index, recoverable.headerSize());
    }

    private static boolean needRecovery(ByteBuffer buffer, Recoverable recoverable)
    {
        var index = FileUtils.findFirstFreeIndex(buffer, MAGIC_NUMBER);
        buffer.limit(index);

        if (bufferNotContainsEvenSingleProbe(buffer, recoverable) || itsNotDivisibleByProbeSize(buffer, recoverable))
        {
            return true;
        }
        return !recoverable.correctProbeOnCurrentPosition(buffer);
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
