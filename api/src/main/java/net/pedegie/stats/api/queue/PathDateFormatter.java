package net.pedegie.stats.api.queue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

class PathDateFormatter
{
    public static final DateTimeFormatter DATE_PATTERN = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    public static Path appendDate(Path path, ZonedDateTime offsetDateTime)
    {
        return Paths.get(path.toString() + "_" + DATE_PATTERN.format(offsetDateTime));
    }
}
