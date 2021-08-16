package net.pedegie.stats.api.overflow;

import net.pedegie.stats.api.queue.Tuple;

public class DropOnOverflow
{
    private static final DropOnOverflow DROP_ON_OVERFLOW_STRATEGY = new DropOnOverflow();

    public void onOverflow(Tuple<Integer, Long> probe) {

    }

    public static DropOnOverflow strategy() {
        return DROP_ON_OVERFLOW_STRATEGY;
    }
}
