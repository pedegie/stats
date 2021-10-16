package net.pedegie.stats.api.queue

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue

import java.util.function.Consumer

class InternalFileAccessMock extends InternalFileAccess
{
    Iterator<Consumer<SingleChronicleQueue>> onClose

    @Override
    void close(SingleChronicleQueue queue)
    {
        if (onClose != null && onClose.hasNext())
            onClose.next()(queue)

        super.close(queue)
    }

}
