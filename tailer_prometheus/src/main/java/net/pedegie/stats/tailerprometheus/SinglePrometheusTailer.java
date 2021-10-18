package net.pedegie.stats.tailerprometheus;

import io.prometheus.client.Collector;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import net.pedegie.stats.api.queue.probe.Probe;
import net.pedegie.stats.api.tailer.Tailer;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
class SinglePrometheusTailer extends Collector implements Tailer
{
    SPSCConcurrentArrayList<MetricFamilySamples.Sample> samples = new SPSCConcurrentArrayList<>(1024);
    String source;

    @Override
    public void onProbe(Probe probe)
    {
        samples.add(new MetricFamilySamples.Sample(source, emptyList(), emptyList(), probe.getCount(), probe.getTimestamp()));
    }

    @Override
    public List<MetricFamilySamples> collect()
    {
        return Collections.singletonList(new ProbeSamples(source, Type.UNKNOWN, "collection size", samples.drain()));
    }

    private static class ProbeSamples extends Collector.MetricFamilySamples
    {
        public ProbeSamples(String name, Collector.Type type, String help, List<Sample> samples)
        {
            super(name, type, help, samples);
        }
    }
}
