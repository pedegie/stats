package io.github.pedegie.stats.integrationtests

import io.github.pedegie.stats.api.queue.StatsQueue
import net.openhft.chronicle.core.OS
import io.github.pedegie.stats.api.queue.BusyWaiter
import io.github.pedegie.stats.api.queue.FileUtils
import io.github.pedegie.stats.api.queue.QueueConfiguration
import io.github.pedegie.stats.api.tailer.ProbeTailer
import io.github.pedegie.stats.api.tailer.ProbeTailerScheduler
import io.github.pedegie.stats.api.tailer.TailerConfiguration
import io.github.pedegie.stats.integrationtests.prometheus.PrometheusHttpExporter
import io.github.pedegie.stats.tailerprometheus.PrometheusTailer
import spock.lang.Requires
import spock.lang.Specification

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.ConcurrentLinkedQueue

@Requires({ env.INTEGRATION_TESTS == 'true' })
class PrometheusTest extends Specification
{
    private static final String PROMETHEUS_API = "http://localhost:9090/api/v1"
    private static final Path PATH = Paths.get(System.getProperty("java.io.tmpdir").toString(), "stats_queue", "stats_queue.log")

    def setup()
    {
        FileUtils.cleanDirectory(PATH.getParent())
    }

    def "should feed probes to Prometheus"()
    {
        given:
            PrometheusTailer prometheusTailer = new WaitUntilCollectedPrometheusTailer()
            PrometheusHttpExporter exporter = new PrometheusHttpExporter(prometheusTailer)
            exporter.startExportingStatistics(9091)

            Path pathQueue1 = Paths.get(PATH.toString() + "_1")
            Path pathQueue2 = Paths.get(PATH.toString() + "_2")
            writeElementsTo(5, pathQueue1)
            writeElementsTo(10, pathQueue2)

            String tailer_name_1 = "test_tailer_1"
            String tailer_name_2 = "test_tailer_2"

            TailerConfiguration configuration1 = TailerConfiguration.builder()
                    .tailer(prometheusTailer.newTailer(tailer_name_1))
                    .path(pathQueue1)
                    .build()

            TailerConfiguration configuration2 = TailerConfiguration.builder()
                    .tailer(prometheusTailer.newTailer(tailer_name_2))
                    .path(pathQueue2)
                    .build()

            ProbeTailer probeTailer1 = ProbeTailer.from(configuration1)
            ProbeTailer probeTailer2 = ProbeTailer.from(configuration2)
            ProbeTailerScheduler scheduler = ProbeTailerScheduler.create(1)
        when:
            scheduler.addTailer(probeTailer1)
            scheduler.addTailer(probeTailer2)
            BusyWaiter.busyWaitMillis({ prometheusTailer.collected }, 15000, "waiting for incoming prometheus request")

            HttpClient httpClient = HttpClient.newHttpClient()

            HttpRequest request_1 = HttpRequest.newBuilder()
                    .uri(new URI(PROMETHEUS_API + "/query?query=" + tailer_name_1))
                    .GET()
                    .build()

            HttpRequest request_2 = HttpRequest.newBuilder()
                    .uri(new URI(PROMETHEUS_API + "/query?query=" + tailer_name_2))
                    .GET()
                    .build()

            HttpResponse<String> response_1 = httpClient.send(request_1, HttpResponse.BodyHandlers.ofString())
            HttpResponse<String> response_2 = httpClient.send(request_2, HttpResponse.BodyHandlers.ofString())
        then:
            response_1.statusCode() == 200
            response_1.body().contains("value")

            response_2.statusCode() == 200
            response_2.body().contains("value")
        cleanup:
            scheduler.close()
            exporter.stopExportingStatistics()
    }

    private static class WaitUntilCollectedPrometheusTailer extends PrometheusTailer
    {
        private volatile boolean collected

        @Override
        List<MetricFamilySamples> collect()
        {
            List<MetricFamilySamples> samples = super.collect()
            if (!samples.isEmpty())
                collected = true

            return samples
        }

    }

    static void writeElementsTo(int elements, Path path)
    {
        QueueConfiguration queueConfiguration = QueueConfiguration.builder()
                .path(path)
                .mmapSize(OS.pageSize())
                .build()

        StatsQueue<Integer> queue = StatsQueue.queue(new ConcurrentLinkedQueue<>(), queueConfiguration)

        (1..elements).forEach({ queue.add(it) })
        queue.close()
    }
}
