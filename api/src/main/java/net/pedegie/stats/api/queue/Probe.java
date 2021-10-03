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
    int accessId;
    int probe;
    long timestamp;

    public static Probe closeFileMessage(int accessId)
    {
        return new Probe(accessId, FileAccess.CLOSE_FILE_MESSAGE_ID, 0);
    }
}
