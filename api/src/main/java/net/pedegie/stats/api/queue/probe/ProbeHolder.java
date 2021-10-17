package net.pedegie.stats.api.queue.probe;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class ProbeHolder implements Probe
{
    int count;
    long timestamp;

    @Override
    public Probe copyForStore()
    {
        return new ProbeHolder(count, timestamp);
    }
}
