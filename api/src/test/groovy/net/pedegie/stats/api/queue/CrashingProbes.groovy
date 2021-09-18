package net.pedegie.stats.api.queue

import java.nio.ByteBuffer
import java.time.ZonedDateTime
import java.util.function.Function

class CrashingProbes
{
    static class CompressedCrashingProbeWriter implements ProbeWriter, Function<FileAccessContext, ProbeWriter>
    {
        private final int crashOnWrite
        int writes
        ZonedDateTime zonedDateTime

        CompressedCrashingProbeWriter(ZonedDateTime zonedDateTime, int crashOnWrite)
        {
            this.zonedDateTime = zonedDateTime
            this.crashOnWrite = crashOnWrite
        }

        CompressedCrashingProbeWriter(ZonedDateTime zonedDateTime, FileAccessContext fileAccessContext, int crashOnWrite)
        {
            fileAccessContext.bufferOffset.getAndAdd(8)
            fileAccessContext.buffer.putLong(zonedDateTime.toInstant().toEpochMilli() | Long.MIN_VALUE)
            this.zonedDateTime = zonedDateTime
            this.crashOnWrite = crashOnWrite
        }

        @Override
        void writeProbe(ByteBuffer buffer, int offset, int probe, long timestamp)
        {
            if (crashOnWrite == -1)
            {
                throw new TestExpectedException("Intentionally! Mocking crashes for testing")
            }

            buffer.putInt(offset, probe)
            if (++writes == crashOnWrite)
            {
                throw new TestExpectedException("Intentionally! Mocking crashes for testing")
            } else
            {
                buffer.putInt(offset + 4, 1)
            }
        }

        @Override
        int probeSize()
        {
            return 8
        }

        @Override
        ProbeWriter apply(FileAccessContext fileAccessContext)
        {
            return new CompressedCrashingProbeWriter(zonedDateTime, fileAccessContext, crashOnWrite)
        }
    }

    static class DefaultCrashingProbeWriter implements ProbeWriter, Function<FileAccessContext, ProbeWriter>
    {
        private final int crashOnWrite
        int writes

        DefaultCrashingProbeWriter(int crashOn)
        {
            this.crashOnWrite = crashOn
        }

        @Override
        void writeProbe(ByteBuffer buffer, int offset, int probe, long timestamp)
        {
            if (crashOnWrite == -1)
            {
                throw new TestExpectedException("Intentionally! Mocking crashes for testing")
            }

            buffer.putInt(offset, probe)
            if (++writes == crashOnWrite)
            {
                throw new TestExpectedException("Intentionally! Mocking crashes for testing")
            } else
            {
                buffer.putLong(offset + 4, 1)
            }
        }

        @Override
        int probeSize()
        {
            return 12
        }

        @Override
        ProbeWriter apply(FileAccessContext fileAccessContext)
        {
            return new DefaultCrashingProbeWriter(crashOnWrite)
        }
    }
}
