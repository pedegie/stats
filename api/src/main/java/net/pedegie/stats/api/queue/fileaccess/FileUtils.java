package net.pedegie.stats.api.queue.fileaccess;

import lombok.SneakyThrows;
import net.openhft.chronicle.core.OS;

import java.nio.file.Files;
import java.nio.file.Path;

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
