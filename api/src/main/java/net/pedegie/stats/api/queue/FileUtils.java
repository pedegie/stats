package net.pedegie.stats.api.queue;

import lombok.SneakyThrows;
import net.openhft.chronicle.core.OS;

import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;

class FileUtils
{
    @SneakyThrows
    public static void createFile(Path path)
    {
        if (Files.exists(path))
        {
            return;
        }
        if (!Files.exists(path.getParent()))
        {
            Files.createDirectories(path.getParent());
        }
        Files.createFile(path);
    }

    @SneakyThrows
    public static void cleanDirectory(Path path)
    {
        if (!Files.exists(path))
        {
            return;
        }

        Deque<Path> dirs = new LinkedList<>();
        dirs.push(path);
        do
        {
            var dir = dirs.pop();
            if (Files.isDirectory(dir, LinkOption.NOFOLLOW_LINKS))
            {
                try (DirectoryStream<Path> entries = Files.newDirectoryStream(dir))
                {
                    var iter = entries.iterator();
                    if (directoryIsEmpty(iter))
                    {
                        Files.delete(dir);
                    } else
                    {
                        dirs.push(dir);
                        while (iter.hasNext())
                        {
                            dirs.push(iter.next());
                        }
                    }
                }
            } else
            {
                Files.delete(dir);
            }
        } while (!dirs.isEmpty());
    }

    private static boolean directoryIsEmpty(Iterator<Path> entries)
    {
        return !entries.hasNext();
    }

    public static int roundToPageSize(int mmapSize)
    {
        int rounded = mmapSize + OS.pageSize() - 1 & (-OS.pageSize());
        return rounded < 0 ? Integer.MAX_VALUE : rounded;
    }

    public static int findFirstFreeIndex(ByteBuffer buffer, int probeSize)
    {
        if (isEmpty(buffer))
            return 0;

        if (isFull(buffer, probeSize))
            return buffer.limit();

        var index = index(buffer, probeSize);
        var offset = index % probeSize;
        if (offset == 0)
            return index;
        return index - offset + probeSize;
    }

    private static int index(ByteBuffer buffer, int probeSize)
    {
        var low = 0;
        var high = buffer.limit() - probeSize;

        while (low < high)
        {
            int mid = (low + high) / 2;

            if (buffer.getInt(mid) != 0)
            {
                low = mid + 1;
                continue;
            }

            if (buffer.get(mid - 1) != 0)
            {
                return mid;
            }

            high = mid;
        }
        return high;
    }

    private static boolean isEmpty(ByteBuffer buffer)
    {
        return buffer.getLong(0) == 0;
    }

    private static boolean isFull(ByteBuffer buffer, int probeSize)
    {
        return buffer.getInt(buffer.limit() - probeSize) != 0;
    }
}
