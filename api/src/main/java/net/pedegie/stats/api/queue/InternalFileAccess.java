package net.pedegie.stats.api.queue;

interface InternalFileAccess
{
    default void closeAccess(FileAccessContext accessContext)
    {
        accessContext.close();
    }

    default FileAccessContext accessContext(QueueConfiguration configuration)
    {
        FileAccessContext accessContext = null;
        try
        {
            accessContext = FileAccessStrategy.fileAccess(configuration);
            if (configuration.isPreTouch())
                PreToucher.preTouch(accessContext.getBuffer());
            return accessContext;
        } catch (Exception e)
        {
            if (accessContext != null)
                accessContext.close();
            throw e;
        }
    }

    default void recycle(FileAccessContext accessContext)
    {
        accessContext.close();
        FileAccessStrategy.recycle(accessContext);
    }

    default void resize(FileAccessContext accessContext)
    {
        accessContext.close();
        accessContext.mmapNextSlice();
    }

    default void writeProbe(FileAccessContext fileAccess, Probe probe)
    {
        fileAccess.writeProbe(probe);
    }
}
