package io.github.pedegie.stats.tailerprometheus;

import io.github.pedegie.stats.api.tailer.Tailer;
import io.prometheus.client.Collector;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@NoArgsConstructor(access = AccessLevel.PROTECTED)
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
        return newTailer(source, false);
    }

    public Tailer newTailer(String source, boolean generateTimestampOnRequestReceive)
    {
        SinglePrometheusTailer tailer = new SinglePrometheusTailer(source, () -> tailers.remove(source), generateTimestampOnRequestReceive);
        if (tailers.putIfAbsent(source, tailer) != null)
            throw new IllegalStateException("Source '" + source + "' already exists.");

        return tailer;
    }

    public boolean removeTailer(String source)
    {
        return tailers.remove(source) != null;
    }
}
