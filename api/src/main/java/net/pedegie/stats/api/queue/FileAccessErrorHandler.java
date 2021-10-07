package net.pedegie.stats.api.queue;

public interface FileAccessErrorHandler
{
    static FileAccessErrorHandler logAndClose()
    {
        return throwable ->
        {
            Logger.error(throwable);
            return true;
        };
    }

    static FileAccessErrorHandler logAndIgnore()
    {
        return throwable ->
        {
            Logger.error(throwable);
            return false;
        };
    }


    /**
     * @param throwable t
     * @return true if file should be closed and all resources released, false otherwise
     */
    boolean handle(Throwable throwable);
}
