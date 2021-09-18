package net.pedegie.stats.api.queue

class TestExpectedException extends RuntimeException
{
    TestExpectedException(String message)
    {
        super(message, null, true, false);
    }
}
