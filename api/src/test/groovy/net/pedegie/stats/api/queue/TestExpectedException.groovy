package net.pedegie.stats.api.queue

class TestExpectedException extends RuntimeException
{
    TestExpectedException()
    {
        super("Expected", null, true, false)
    }
}
