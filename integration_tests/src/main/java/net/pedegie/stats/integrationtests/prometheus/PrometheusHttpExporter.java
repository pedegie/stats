package net.pedegie.stats.integrationtests.prometheus;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class PrometheusHttpExporter
{
    private final Collector collector;
    private HTTPServer httpServer;

    public PrometheusHttpExporter(Collector collector)
    {
        this.collector = collector;
    }

    @SneakyThrows
    public void startExportingStatistics(int port)
    {
        httpServer = CompletableFuture.supplyAsync(() ->
        {
            CollectorRegistry registry = new CollectorRegistry(true);
            registry.register(collector);
            try
            {
                return new HTTPServer.Builder()
                        .withPort(port)
                        .withRegistry(registry)
                        .build();
            } catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }).get(5, TimeUnit.SECONDS);

    }

    public void stopExportingStatistics()
    {
        httpServer.close();
    }

}
