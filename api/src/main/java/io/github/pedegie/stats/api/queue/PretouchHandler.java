package io.github.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.queue.internal.InternalPretouchHandler;
import org.jetbrains.annotations.NotNull;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
class PretouchHandler implements EventHandler
{
    InternalPretouchHandler internalPretouchHandler;

    @Override
    public boolean action() throws InvalidEventHandlerException
    {
        return internalPretouchHandler.action();
    }

    @Override
    public void eventLoop(EventLoop eventLoop)
    {
        internalPretouchHandler.eventLoop(eventLoop);
    }

    @Override
    public void loopStarted()
    {
        internalPretouchHandler.loopStarted();
    }

    @Override
    public void loopFinished()
    {
        internalPretouchHandler.loopFinished();
    }

    @Override
    public @NotNull HandlerPriority priority()
    {
        return HandlerPriority.MEDIUM;
    }
}
