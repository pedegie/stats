package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.FieldDefaults;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
@Getter
@ToString
public class Probe
{
    private static final Probe CLOSE_ALL_FILES_MESSAGE = new Probe(0, FileAccess.CLOSE_ALL_FILES_ID, 0);

    int accessId;
    int probe;
    long timestamp;

    public static Probe closeFileMessage(int accessId)
    {
        return new Probe(accessId, FileAccess.CLOSE_FILE_ID, 0);
    }

    public static Probe closeAllFilesMessage()
    {
        return CLOSE_ALL_FILES_MESSAGE;
    }
}
