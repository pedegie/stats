package io.github.pedegie.stats.api.queue;

interface StateUpdater
{
    boolean intoBusy();

    void intoFree();

    boolean intoClosing();

    void intoClosed();
}
