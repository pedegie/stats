package io.github.pedegie.stats.api.tailer

import io.github.pedegie.stats.api.queue.TestTailer

import java.nio.file.Path

class TailerFactory
{
    static ProbeTailer tailerFor(Path path)
    {
        return tailerFor(path, new TestTailer())
    }

    static ProbeTailer tailerFor(Path path, Tailer from)
    {
        TailerConfiguration configuration = TailerConfiguration.builder()
                .tailer(from)
                .path(path)
                .build()
        return ProbeTailer.from(configuration)
    }

}
