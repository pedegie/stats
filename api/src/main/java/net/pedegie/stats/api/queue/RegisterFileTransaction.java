package net.pedegie.stats.api.queue;

import gnu.trove.map.TIntObjectMap;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Slf4j
class RegisterFileTransaction implements Transaction<RegisterFileResponse>
{
    QueueConfiguration conf;
    InternalFileAccess internalFileAccess;
    TIntObjectMap<FileAccessContext> files;

    @NonFinal
    FileAccessContext accessContext;
    @NonFinal
    int fileAccessId;

    @Override
    public void withinTimeout()
    {
        var id = conf.getPath().hashCode();
        if (files.containsKey(id))
        {
            this.fileAccessId = -1;
            return;
        }
        this.fileAccessId = id;
        log.debug("Registering file {}", fileAccessId);

        this.accessContext = internalFileAccess.accessContext(conf);
    }

    @Override
    public RegisterFileResponse commit()
    {
        if (this.fileAccessId == -1)
            return RegisterFileResponse.fileAlreadyExists();

        files.put(fileAccessId, accessContext);
        accessContext.enableAccess();
        log.debug("File registered {}", fileAccessId);
        return new RegisterFileResponse(fileAccessId, accessContext.getState());
    }
}
