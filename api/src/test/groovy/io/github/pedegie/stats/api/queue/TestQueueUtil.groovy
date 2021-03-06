package io.github.pedegie.stats.api.queue

import io.github.pedegie.stats.api.queue.QueueConfiguration
import io.github.pedegie.stats.api.queue.StatsQueue

import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.ConcurrentLinkedQueue

class TestQueueUtil
{
    public static Path PATH = Paths.get(System.getProperty("java.io.tmpdir").toString(), "stats_queue", "stats_queue.log").toAbsolutePath()

    static StatsQueue<Integer> createQueue(QueueConfiguration queueConfiguration)
    {
        return StatsQueue.queue(new ConcurrentLinkedQueue<Integer>(), queueConfiguration)
    }
}
