package net.pedegie.stats.api.queue;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;

import java.util.concurrent.Semaphore;

import static net.pedegie.stats.api.queue.FileAccess.ERROR;
import static net.pedegie.stats.api.queue.FileAccess.FILE_ALREADY_EXISTS;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
class RegisterFileResponse
{
    private static final RegisterFileResponse FILE_ALREADY_EXISTS_RESPONSE = new RegisterFileResponse(FILE_ALREADY_EXISTS, null);
    private static final RegisterFileResponse ERROR_DURING_INITIALIZATION_RESPONSE = new RegisterFileResponse(ERROR, null);
    @Getter
    int fileAccessId;
    Semaphore semaphore;


    public static RegisterFileResponse fileAlreadyExists()
    {
        return FILE_ALREADY_EXISTS_RESPONSE;
    }

    public static RegisterFileResponse errorDuringInit()
    {
        return ERROR_DURING_INITIALIZATION_RESPONSE;
    }

    public boolean isFileAlreadyExists()
    {
        return fileAccessId == FILE_ALREADY_EXISTS;
    }

    public boolean isErrorDuringInit()
    {
        return fileAccessId == ERROR;
    }

    public boolean notClosed()
    {
        return semaphore.availablePermits() <= FileAccessContext.ALL_PERMITS;
    }

    public boolean isTerminated()
    {
        return semaphore.availablePermits() == FileAccessContext.TERMINATED;
    }
}
