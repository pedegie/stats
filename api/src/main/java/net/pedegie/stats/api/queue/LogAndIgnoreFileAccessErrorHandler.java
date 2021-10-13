package net.pedegie.stats.api.queue;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class LogAndIgnoreFileAccessErrorHandler implements FileAccessErrorHandler
{
    public static final FileAccessErrorHandler INSTANCE = new LogAndIgnoreFileAccessErrorHandler();

    @Override
    public void errorOnCreatingFile(Throwable throwable)
    {
        log.error("Error during creating file", throwable);
    }

    @Override
    public boolean errorOnProbeWrite(Throwable throwable)
    {
        log.error("Error during probe write", throwable);
        return false;
    }

    @Override
    public boolean errorOnResize(Throwable throwable)
    {
        log.error("Error during resize", throwable);
        return false;
    }

    @Override
    public boolean errorOnRecycle(Throwable throwable)
    {
        log.error("Error during recycle", throwable);
        return false;
    }

    @Override
    public void errorOnClosingFile(Throwable throwable)
    {
        log.error("Error during close", throwable);
    }
}
