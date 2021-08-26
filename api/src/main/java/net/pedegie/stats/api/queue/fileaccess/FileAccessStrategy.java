package net.pedegie.stats.api.queue.fileaccess;

import lombok.SneakyThrows;
import net.pedegie.stats.api.queue.LogFileConfiguration;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import static net.pedegie.stats.api.queue.fileaccess.FileAccess.TIMESTAMP_SIZE;

public class FileAccessStrategy
{
    private static final int BUFFER_FULL = -1;

    @SneakyThrows
    public static FileAccess accept(LogFileConfiguration logFileConfiguration)
    {
        LogFileConfigurationValidator.validate(logFileConfiguration);
        Path path = logFileConfiguration.getPath();
        var mmapSize = logFileConfiguration.getMmapSize();

        if (logFileConfiguration.isOverride())
        {
            Files.deleteIfExists(path);
            return fileAccess(path, mmapSize);
        } else if (logFileConfiguration.isAppend())
        {
            boolean exists = Files.exists(path);
            var fileAccess = fileAccess(path, mmapSize);
            if (exists)
            {
                var firstFreeIndex = findFirstFreeIndex(fileAccess.mappedFileBuffer);
                fileAccess.mappedFileBuffer.position(firstFreeIndex);
                fileAccess.bufferOffset.set(firstFreeIndex);
            }

            return fileAccess;
        } else
        {
            var fileName = PathDateFormatter.appendDate(path);
            return fileAccess(fileName, mmapSize);
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
            for (int i = mid; i < mid + TIMESTAMP_SIZE; i++)
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
    private static FileAccess fileAccess(Path filePath, int mmapSize)
    {
        return new FileAccess(filePath, mmapSize);
    }
}
