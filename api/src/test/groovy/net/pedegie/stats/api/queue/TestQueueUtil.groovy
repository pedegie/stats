package net.pedegie.stats.api.queue

import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.ConcurrentLinkedQueue

class TestQueueUtil
{
    public static Path PATH = Paths.get(System.getProperty("java.io.tmpdir").toString(), "stats_queue", "stats_queue.log").toAbsolutePath()

    static Path findExactlyOneOrThrow(Path path)
    {
        List<Path> paths = path.getParent().toFile().listFiles(new FileFilter() {
            @Override
            boolean accept(File pathname)
            {
                return pathname.toString().startsWith(path.toString())
            }
        }).collect { it.toPath() }

        if (paths.size() != 1)
            throw new IllegalStateException("Found " + paths.size() + " paths " + paths)

        return paths.get(0)
    }

    static List<Path> findMany(Path path)
    {
        return path.getParent().toFile().listFiles(new FileFilter() {
            @Override
            boolean accept(File pathname)
            {
                return pathname.toString().startsWith(path.toString())
            }
        }).collect { it.toPath() }
    }

    static StatsQueue<Integer> createQueue(QueueConfiguration queueConfiguration)
    {
        StatsQueue.<Integer> builder()
                .queue(new ConcurrentLinkedQueue<Integer>())
                .queueConfiguration(queueConfiguration)
                .build()
    }
}
