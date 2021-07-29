package net.pedegie.stats.api.util;

import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class BlockSize
{
    public static long blockSize(Path path)
    {
        try (FileChannel fc = FileChannel.open(path))
        {
            return fc.size();
        } catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
