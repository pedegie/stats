package net.pedegie.stats.api.queue;

import java.util.HashMap;
import java.util.Map;

public class Properties
{
    private static final Map<String, Object> properties = new HashMap<>();

    public static void add(String key, Object value)
    {
        properties.put(key, value);
    }

    public static int get(String key, int defaultValue)
    {
        var property = properties.get(key);
        if (property == null)
        {
            return defaultValue;
        }

        return (int) property;
    }
}
