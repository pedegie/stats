package io.github.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.threads.MediumEventLoop;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.threads.VanillaEventLoop;

import java.util.EnumSet;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
class PretoucherThread
{
    MediumEventLoop eventLoop;

    static
    {
        System.setProperty("disable.thread.safety", "true");
        System.setProperty("SingleChronicleQueueExcerpts.earlyAcquireNextCycle", "true");
        System.setProperty("SingleChronicleQueueExcerpts.pretoucherPrerollTimeMs", "100");
    }

    public PretoucherThread()
    {
        this.eventLoop = new VanillaEventLoop(null,
                "pretoucher",
                Pauser.balanced(),
                2000,
                true,
                "none",
                EnumSet.of(HandlerPriority.MEDIUM));
    }

    public void startPretouching(PretouchHandler pretouchHandler)
    {
        eventLoop.addHandler(pretouchHandler);
    }

    public void start()
    {
        eventLoop.start();
    }
}
