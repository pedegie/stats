package net.pedegie.stats.api.queue

import java.util.function.Consumer
import java.util.function.Function

class InternalFileAccessMock implements InternalFileAccess
{
    Consumer<FileAccessContext> onClose
    Function<QueueConfiguration, FileAccessContext> onAccessContext
    Consumer<FileAccessContext> onRecycle
    Consumer<FileAccessContext> onResize

    @Override
    void closeAccess(FileAccessContext accessContext)
    {
        if (onClose == null)
            new InternalFileAccess() {}.closeAccess(accessContext)
        else
            onClose.accept(accessContext)
    }

    @Override
    FileAccessContext accessContext(QueueConfiguration configuration)
    {
        if (onAccessContext == null)
            return new InternalFileAccess() {}.accessContext(configuration)
        else
            return onAccessContext.apply(configuration)
    }

    @Override
    void recycle(FileAccessContext accessContext)
    {
        if (onRecycle == null)
            new InternalFileAccess() {}.closeAccess(accessContext)
        else
            onRecycle.accept(accessContext)
    }

    @Override
    void resize(FileAccessContext fileAccess)
    {
        if (onResize == null)
            new InternalFileAccess() {}.closeAccess(fileAccess)
        else
            onResize.accept(fileAccess)
    }
}
