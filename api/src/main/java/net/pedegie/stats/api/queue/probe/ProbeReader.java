package net.pedegie.stats.api.queue.probe;

import net.pedegie.stats.api.queue.Probe;

import java.nio.ByteBuffer;

public interface ProbeReader
{
    Probe readProbe(ByteBuffer buffer);
}
