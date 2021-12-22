package io.github.pedegie.stats.api.queue;

import lombok.SneakyThrows;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;

public class FileUtils
{

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
}
