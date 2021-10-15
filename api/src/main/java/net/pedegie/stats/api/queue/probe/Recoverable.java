package net.pedegie.stats.api.queue.probe;

import java.nio.ByteBuffer;

interface Recoverable
{
    int probeSize();

    int headerSize();

    boolean correctProbeOnLastPosition(ByteBuffer buffer);
}
