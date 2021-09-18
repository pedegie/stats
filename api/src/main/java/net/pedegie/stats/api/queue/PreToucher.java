package net.pedegie.stats.api.queue;

import net.openhft.chronicle.core.OS;

import java.nio.ByteBuffer;

class PreToucher
{
    public static void preTouch(ByteBuffer byteBuffer)
    {
        for (int i = byteBuffer.position(); i < byteBuffer.limit() && i >= 0; i += OS.pageSize())
        {
            byteBuffer.put(i, (byte) 0);
        }
    }
}
