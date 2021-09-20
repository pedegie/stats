package net.pedegie.stats.api.queue;

import lombok.experimental.Delegate;

import java.util.concurrent.atomic.AtomicInteger;

class ConcurrentCounter implements Counter
{
    @Delegate
    private final AtomicInteger counter = new AtomicInteger();
}
