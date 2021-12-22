package io.github.pedegie.stats.api.queue.probe;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class ProbeHolder implements Probe
{
    public static final int PROBE_SIZE = 12;

    int count;
    long timestamp;

    @Override
    public Probe copyForStore()
    {
        return new ProbeHolder(count, timestamp);
    }
}
