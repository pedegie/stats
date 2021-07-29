package net.pedegie.stats.api.reader;

import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.threads.MediumEventLoop;
import net.openhft.chronicle.threads.Pauser;

public class Tailers
{
    private static final MediumEventLoop readers = new MediumEventLoop(null, "readers_loop", Pauser.busy(), false, "any");

    static
    {
        readers.start();
    }

    public static void addTailer(ExcerptTailer tailer, Tailer<Long, Integer> reader)
    {
        readers.addHandler(new TailerHandler(tailer, reader));
    }


    private static class TailerHandler implements EventHandler
    {
        private final ExcerptTailer tailer;
        private final Tailer<Long,Integer> reader;

        public TailerHandler(ExcerptTailer tailer, Tailer<Long,Integer> reader)
        {
            this.tailer = tailer;
            this.reader = reader;
        }

        @Override
        public boolean action()
        {
            //TODO loop here and create HUB to not allow starvation by single queue
            tailer.readBytes(bytes -> {
                long a = bytes.readLong();
                var b = bytes.readInt();
                reader.read(a, b);
            });

            return false;
        }

        @Override
        public void loopFinished()
        {
            tailer.close();
        }
    }
}
