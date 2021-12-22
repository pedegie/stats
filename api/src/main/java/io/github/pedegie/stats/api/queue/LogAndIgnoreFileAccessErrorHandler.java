package io.github.pedegie.stats.api.queue;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class LogAndIgnoreFileAccessErrorHandler implements FileAccessErrorHandler
{
    public static final FileAccessErrorHandler INSTANCE = new LogAndIgnoreFileAccessErrorHandler();

    @Override
    public boolean onError(Throwable throwable)
    {
        log.error("Error during probe write", throwable);
        return false;
    }

    @Override
    public String toString()
    {
        return this.getClass().getName();
    }
}
