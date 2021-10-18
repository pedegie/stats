package net.pedegie.stats.tailerprometheus;

import io.prometheus.client.Collector;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import net.pedegie.stats.api.tailer.Tailer;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PrometheusTailer extends Collector
{
    public static final PrometheusTailer INSTANCE = new PrometheusTailer();

    private final ConcurrentHashMap<String, SinglePrometheusTailer> tailers = new ConcurrentHashMap<>();

    @Override
    public List<MetricFamilySamples> collect()
    {
        return tailers.values().stream()
                .flatMap(tailer -> tailer.collect().stream())
                .collect(Collectors.toList());
    }

    public Tailer newTailer(String source)
    {
        SinglePrometheusTailer tailer = new SinglePrometheusTailer(source);
        if (tailers.putIfAbsent(source, tailer) != null)
            throw new IllegalStateException("Source '" + source + "' already exists.");

        return tailer;
    }

    public void removeTailer(String source)
    {
        tailers.remove(source);
    }
}
