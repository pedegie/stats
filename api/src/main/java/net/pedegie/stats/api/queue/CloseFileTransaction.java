package net.pedegie.stats.api.queue;

import gnu.trove.map.TIntObjectMap;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Slf4j
class CloseFileTransaction implements Transaction<Void>
{
	FileAccessContext context;
	long timeoutMillis;
	int accessId;
	InternalFileAccess internalFileAccess;
	TIntObjectMap<FileAccessContext> files;

	@Override
	public void setup()
	{
		log.debug("Closing {}", accessId);
		if (!context.acquireWritesBlocking(timeoutMillis, TimeUnit.MILLISECONDS))
			throw new IllegalStateException("recycle / resize prevents from closing file");
	}

	@Override
	public void withinTimeout()
	{
		internalFileAccess.closeAccess(context);
	}

	@Override
	public Void commit()
	{
		var accessContext = files.remove(accessId);
		assert accessContext != null;
		accessContext.terminate();
		log.debug("Closed {}", accessId);
		return null;
	}
}
