package net.pedegie.stats.api.queue;

interface InternalFileAccess
{
    default void closeAccess(FileAccessContext accessContext)
    {
        accessContext.close();
    }

    default FileAccessContext accessContext(QueueConfiguration configuration)
    {
        return FileAccessStrategy.fileAccess(configuration);
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
}
