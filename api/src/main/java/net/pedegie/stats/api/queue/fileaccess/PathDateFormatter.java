package net.pedegie.stats.api.queue.fileaccess;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

class PathDateFormatter
{
    private static final DateTimeFormatter DATE_PATTERN = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    static Path appendDate(Path path)
    {
        return Paths.get(path.toString() + "_" + DATE_PATTERN.format(LocalDateTime.now()));
    }

    static LocalDateTime takeDate(Path path)
    {
        return LocalDateTime.parse(path.toString().split("_")[2]);
    }
}
