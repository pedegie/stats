package net.pedegie.stats.api.queue;

public interface FileAccessErrorHandler
{

    default void errorOnCreatingFile(Throwable throwable)
    {
    }

    /**
     * @param throwable t
     * @return true if file should be closed and all resources released, false otherwise, default is false
     */

    default boolean errorOnProbeWrite(Throwable throwable)
    {
        return false;
    }

    /**
     * @param throwable t
     * @return true if file should be closed and all resources released, false otherwise, default is false
     */
    default boolean errorOnResize(Throwable throwable)
    {
        return false;
    }

    /**
     * @param throwable t
     * @return true if file should be closed and all resources released, false otherwise, default is false
     */
    default boolean errorOnRecycle(Throwable throwable)
    {
        return false;
    }


    /**
     * File goes into CLOSE_ONLY state if error throws during closing, what means it will accept only CLOSE_FILE probe messages and
     * ignore all the rest
     *
     * @param throwable t
     */
    default void errorOnClosingFile(Throwable throwable)
    {

    }


    static FileAccessErrorHandler logAndIgnore()
    {
        return LogAndIgnoreFileAccessErrorHandler.INSTANCE;
    }
}
