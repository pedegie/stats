package net.pedegie.stats.api.queue;

@FunctionalInterface
public interface WriteFilter
{
    WriteFilter acceptAllFilter = acceptAll();

    boolean shouldWrite(int size, long timestamp);

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

    static WriteFilter acceptWhenSizeHigherThan(int size)
    {
        return new WriteFilter()
        {
            private final int acceptSize = size;

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
        };
    }
}
