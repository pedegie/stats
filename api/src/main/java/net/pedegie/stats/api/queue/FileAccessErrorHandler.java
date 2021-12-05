package net.pedegie.stats.api.queue;

public interface FileAccessErrorHandler
{
    /**
     * @param throwable t
     * @return true if queue should be closed, false otherwise. Default is false
     */
    default boolean onError(Throwable throwable)
    {
        return false;
    }

    static FileAccessErrorHandler logAndIgnore()
    {
        return LogAndIgnoreFileAccessErrorHandler.INSTANCE;
    }
}
