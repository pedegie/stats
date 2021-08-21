package net.pedegie.stats.api.queue.fileaccess;

import lombok.SneakyThrows;
import net.pedegie.stats.api.queue.LogFileConfiguration;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static net.pedegie.stats.api.queue.fileaccess.FileAccess.LONG_SIZE;

public class FileAccessStrategy
{
    private static final int BUFFER_FULL = -1;
    private static final int MB_500 = 1024 * 1024 * 512;

    @SneakyThrows
    public static FileAccess accept(LogFileConfiguration logFileConfiguration)
    {
        LogFileConfigurationValidator.validate(logFileConfiguration);
        Path path = logFileConfiguration.getPath();

        if (logFileConfiguration.isOverride())
        {
            Files.deleteIfExists(path);
            return fileAccess(path);
        } else if (logFileConfiguration.isNewFileWithDate())
        {
            var fileName = PathDateFormatter.appendDate(path);
            return fileAccess(fileName);
        } else
        {
            boolean exists = Files.exists(path);
            var fileAccess = fileAccess(path);
            if (exists)
            {
                var firstFreeIndex = findFirstFreeIndex(fileAccess.mappedFileBuffer);
                fileAccess.mappedFileBuffer.position(firstFreeIndex);
                fileAccess.fileOffset.set(firstFreeIndex);
            }

            return fileAccess;
        }
    }

    private static int findFirstFreeIndex(ByteBuffer buffer)
    {
        long low = 0;
        long high = buffer.limit();

        label:
        while (low < high)
        {
            int mid = (int) ((low + high) / 2);
            for (int i = mid; i < mid + LONG_SIZE; i++)
            {
                if (buffer.get(i) != 0)
                {
                    low = mid + 1;
                    continue label;
                }
            }

            if (buffer.get(mid - 1) != 0)
            {
                return mid;
            }

            high = mid;
        }
        return BUFFER_FULL;
    }

    @SneakyThrows
    private static FileAccess fileAccess(Path filePath)
    {
        createFile(filePath);
        var logFileAccess = new RandomAccessFile(filePath.toFile(), "rw");
        var statsLogFile = logFileAccess.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, MB_500);
        return new FileAccess(logFileAccess, statsLogFile);
    }

    @SneakyThrows
    private static void createFile(Path path)
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
}
