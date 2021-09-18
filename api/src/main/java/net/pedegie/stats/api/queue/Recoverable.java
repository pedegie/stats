package net.pedegie.stats.api.queue;

import java.nio.ByteBuffer;

interface Recoverable
{
    int probeSize();

    int headerSize();

    boolean correctProbeOnCurrentPosition(ByteBuffer buffer);
}
