package net.pedegie.stats.api.queue;

import net.openhft.chronicle.core.OS;

import java.nio.ByteBuffer;

class PreToucher
{
    private static final int PAGE_SIZE = OS.pageSize();

    public static void preTouch(ByteBuffer byteBuffer)
    {
        for (int i = byteBuffer.position(); i < byteBuffer.limit() && i >= 0; i += PAGE_SIZE)
        {
            byteBuffer.put(i, (byte) 0);
        }
    }
}
