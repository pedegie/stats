package io.github.pedegie.stats.api.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;

class InternalFileAccess
{
    static InternalFileAccess INSTANCE = new InternalFileAccess();

    void close(SingleChronicleQueue queue)
    {
        queue.close();
    }
}
