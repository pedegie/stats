package io.github.pedegie.stats.api.queue

class TestExpectedException extends RuntimeException
{
    TestExpectedException()
    {
        super("Expected", null, true, false)
    }
}
