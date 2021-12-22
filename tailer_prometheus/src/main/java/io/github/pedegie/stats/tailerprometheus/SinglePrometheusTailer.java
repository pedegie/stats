package io.github.pedegie.stats.tailerprometheus;

import io.github.pedegie.stats.api.queue.probe.Probe;
import io.github.pedegie.stats.api.tailer.Tailer;
import io.prometheus.client.Collector;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
class SinglePrometheusTailer extends Collector implements Tailer
{
    @NonFinal
    volatile MetricFamilySamples.Sample sample;
    String source;
    Runnable onClose;
    boolean generateTimestampOnRequestReceive;

    @Override
    public void onProbe(Probe probe)
    {
        var timestamp = generateTimestampOnRequestReceive ? System.currentTimeMillis() : probe.getTimestamp();
        sample = new MetricFamilySamples.Sample(source, emptyList(), emptyList(), probe.getCount(), timestamp);
    }

    @Override
    public List<MetricFamilySamples> collect()
    {
        return Collections.singletonList(new Collector.MetricFamilySamples(source, Type.GAUGE, "collection size", singletonList(sample)));
    }

    @Override
    public void onClose()
    {
        onClose.run();
    }
}
