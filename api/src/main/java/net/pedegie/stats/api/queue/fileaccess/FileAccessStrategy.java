package net.pedegie.stats.api.queue.fileaccess;

import lombok.SneakyThrows;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import static net.pedegie.stats.api.queue.fileaccess.FileAccess.PROBE_SIZE;

public class FileAccessStrategy
{
    @SneakyThrows
    public static FileAccess accept(int mmapSize, Path fileName)
    {
        boolean exists = Files.exists(fileName);
        var fileAccess = fileAccess(fileName, mmapSize);
        if (exists)
        {
            var firstFreeIndex = findFirstFreeIndex(fileAccess.mappedFileBuffer);
            fileAccess.mappedFileBuffer.position(firstFreeIndex);
            fileAccess.bufferOffset.set(firstFreeIndex);
        }
        return fileAccess;
    }

    private static int findFirstFreeIndex(ByteBuffer buffer)
    {
        if (isFull(buffer))
            return buffer.limit();

        if (isEmpty(buffer))
            return 0;

        var index = index(buffer);
        int offset = index % PROBE_SIZE;
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

    private static FileAccess fileAccess(Path filePath, int mmapSize)
    {
        return new FileAccess(filePath, mmapSize);
    }
}
