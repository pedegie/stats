package net.pedegie.stats.api.queue;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class Logger
{
    public static void error(Throwable throwable)
    {
        log.error("", throwable);
    }
}
