package net.pedegie.stats.api.queue


import java.util.function.Consumer

class InternalFileAccessMock implements InternalFileAccess
{
    Iterator<Consumer<FileAccessContext>> onClose
    Iterator<Consumer<QueueConfiguration>> onAccessContext
    Iterator<Consumer<QueueConfiguration>> onWriteProbe
    Iterator<Consumer<FileAccessContext>> onRecycle
    Iterator<Consumer<FileAccessContext>> onResize

    private final InternalFileAccess fileAccess

    InternalFileAccessMock()
    {
        this.fileAccess = new InternalFileAccess() {}
    }

    @Override
    void writeProbe(FileAccessContext fileAccess, Probe probe)
    {
        if (onWriteProbe != null && onWriteProbe.hasNext())
            onWriteProbe.next()(fileAccess)

        fileAccess.writeProbe(probe)
    }

    @Override
    void closeAccess(FileAccessContext accessContext)
    {
        if (onClose != null && onClose.hasNext())
            onClose.next()(accessContext)

        fileAccess.closeAccess(accessContext)
    }


    @Override
    FileAccessContext accessContext(QueueConfiguration configuration)
    {
        if (onAccessContext != null && onAccessContext.hasNext())
        {
            onAccessContext.next()(configuration)
        }
        return fileAccess.accessContext(configuration)
    }

    @Override
    void recycle(FileAccessContext accessContext)
    {
        if (onRecycle != null && onRecycle.hasNext())
            onRecycle.next()(accessContext)

        fileAccess.recycle(accessContext)
    }

    @Override
    void resize(FileAccessContext accessContext)
    {
        if (onResize != null && onResize.hasNext())
            onResize.next()(accessContext)

        fileAccess.resize(accessContext)
    }
}
