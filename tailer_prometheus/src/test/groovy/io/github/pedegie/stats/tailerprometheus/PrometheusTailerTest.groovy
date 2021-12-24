package io.github.pedegie.stats.tailerprometheus

import io.github.pedegie.stats.api.tailer.ProbeTailer
import io.github.pedegie.stats.api.tailer.TailerConfiguration
import spock.lang.Specification

import java.nio.file.Path
import java.nio.file.Paths

class PrometheusTailerTest extends Specification
{
    private static final Path PATH = Paths.get(System.getProperty("java.io.tmpdir").toString(), "stats_queue", "stats_queue.log")

    def "should release memory when tailer is closed"()
    {
        given:
            String source = "source"
            TailerConfiguration tailerConfiguration = TailerConfiguration.builder()
                    .tailer(PrometheusTailer.COLLECTOR.newTailer(source))
                    .path(PATH)
                    .build()
            ProbeTailer probeTailer = ProbeTailer.from(tailerConfiguration)
        when:
            probeTailer.close()
            boolean removed = PrometheusTailer.COLLECTOR.removeTailer(source)
        then: "tailer was removed during close, so it wasn't removed"
            !removed
    }

    def "should throw an exception if source already exists"()
    {
        given:
            String source = "source"
            PrometheusTailer.COLLECTOR.newTailer(source)
        when:
            PrometheusTailer.COLLECTOR.newTailer(source)
        then:
            thrown(IllegalStateException)
    }
}
