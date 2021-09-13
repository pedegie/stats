package net.pedegie.stats.api.queue;

import lombok.SneakyThrows;
import net.openhft.chronicle.core.OS;

import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;

import static net.pedegie.stats.api.queue.DefaultFileAccess.PROBE_SIZE;

class FileUtils
{
    private static final int PAGE_SIZE = OS.pageSize();

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
        if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS))
        {
            try (DirectoryStream<Path> entries = Files.newDirectoryStream(path))
            {
                for (Path entry : entries)
                {
                    cleanDirectory(entry);
                }
            }
        }
        Files.deleteIfExists(path);
    }

    public static int roundToPageSize(int mmapSize)
    {
        int rounded = mmapSize + PAGE_SIZE - 1 & (-PAGE_SIZE);
        if (rounded < 0)
        {
            return Integer.MAX_VALUE;
        }
        return rounded;
    }

    public static int findFirstFreeIndex(ByteBuffer buffer)
    {
        if (isEmpty(buffer))
            return 0;

        if (isFull(buffer))
            return buffer.limit();

        var index = index(buffer);
        var offset = index % PROBE_SIZE;
        if (offset == 0)
            return index;
        return index - offset + PROBE_SIZE;
    }

    private static int index(ByteBuffer buffer)
    {
        var low = 0;
        var high = buffer.limit() - PROBE_SIZE;

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

    private static boolean isFull(ByteBuffer buffer)
    {
        return buffer.getInt(buffer.limit() - PROBE_SIZE) != 0;
    }
}
