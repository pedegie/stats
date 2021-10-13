package net.pedegie.stats.api.queue;

@FunctionalInterface
interface Transaction<T>
{
    default void setup()
    {

    }

    void withinTimeout();

    default T commit()
    {
        return null;
    }
}
