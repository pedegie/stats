package net.pedegie.stats.api.queue.fileaccess;

import lombok.SneakyThrows;
import net.openhft.chronicle.core.OS;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;

public class FileUtils
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
}
