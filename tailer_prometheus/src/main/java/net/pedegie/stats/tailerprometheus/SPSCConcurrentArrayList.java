package net.pedegie.stats.tailerprometheus;

import lombok.experimental.NonFinal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class SPSCConcurrentArrayList<T>
{
    volatile List<T> elements;
    @NonFinal
    private volatile boolean piggyback;

    public SPSCConcurrentArrayList(int size)
    {
        this.elements = new ArrayList<>(size);
    }

    public void add(T element)
    {
        elements.add(element);
        piggyback = true;
    }

    public List<T> drain()
    {
        if (piggyback)
        {
            var currentSizeIncreasedByFivePercent = elements.size() * 1.05;
            var view = Collections.unmodifiableList(elements);
            elements = new ArrayList<>((int) currentSizeIncreasedByFivePercent);
            return view;
        }
        return Collections.emptyList();
    }
}
