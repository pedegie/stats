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

    public static void addTailer(ExcerptTailer tailer)
    {
        readers.addHandler(new TailerHandler(tailer));
    }


    private static class TailerHandler implements EventHandler
    {
        private final ExcerptTailer tailer;

        public TailerHandler(ExcerptTailer tailer)
        {
            this.tailer = tailer;
        }

        @Override
        public boolean action()
        {
            tailer.readBytes(bytes -> {
                long a = bytes.readLong();
                var b = bytes.readInt();
                System.out.println("Received: " + a + " " + b);
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
