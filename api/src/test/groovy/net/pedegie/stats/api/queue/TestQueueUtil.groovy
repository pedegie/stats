package net.pedegie.stats.api.queue

import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.ConcurrentLinkedQueue

class TestQueueUtil
{
    public static Path PATH = Paths.get(System.getProperty("java.io.tmpdir").toString(), "stats_queue", "stats_queue.log").toAbsolutePath()

    static StatsQueue<Integer> createQueue(QueueConfiguration queueConfiguration)
    {
        StatsQueue.<Integer> builder()
                .queue(new ConcurrentLinkedQueue<Integer>())
                .queueConfiguration(queueConfiguration)
                .build()
    }
}
