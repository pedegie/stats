package io.github.pedegie.stats.api.queue;

class WriteFilterFactory
{
    private static final WriteFilter ACCEPT_ALL_FILTER = acceptAll();
    private static final AcceptWhenSizeHigherThanWriteFilter[] WRITE_FILTERS = new AcceptWhenSizeHigherThanWriteFilter[6];

    public static WriteFilter acceptAllFilter()
    {
        return ACCEPT_ALL_FILTER;
    }

    public static WriteFilter acceptWhenSizeHigherThan(int size)
    {
        int freeIndex;
        for (freeIndex = 0; freeIndex < WRITE_FILTERS.length; freeIndex++)
        {
            var maybeWriteFilter = WRITE_FILTERS[freeIndex];
            if (maybeWriteFilter == null)
                break;
            else if (maybeWriteFilter.acceptSize == size)
                return maybeWriteFilter;
        }

        var writeFilter = new AcceptWhenSizeHigherThanWriteFilter(size);

        if (freeIndex < WRITE_FILTERS.length)
        {
            WRITE_FILTERS[freeIndex] = writeFilter;
        }

        return writeFilter;
    }

    private static WriteFilter acceptAll()
    {
        return new WriteFilter()
        {
            @Override
            public boolean shouldWrite(int size, long timestamp)
            {
                return true;
            }

            @Override
            public String toString()
            {
                return "acceptAllFilter";
            }
        };
    }

    private static class AcceptWhenSizeHigherThanWriteFilter implements WriteFilter
    {
        private final int acceptSize;

        public AcceptWhenSizeHigherThanWriteFilter(int acceptSize)
        {
            this.acceptSize = acceptSize;
        }

        @Override
        public boolean shouldWrite(int size, long timestamp)
        {
            return size > acceptSize;
        }

        @Override
        public String toString()
        {
            return "acceptWhenSizeHigherThan " + acceptSize;
        }
    }
}
