package org.omnaest.repository.nitrite;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.omnaest.utils.FileUtils;
import org.omnaest.utils.cache.Cache;

public class NitriteCacheTest
{

    @Test
    public void testGet() throws Exception
    {
        Cache cache = new NitriteCache(FileUtils.createRandomTempFile());

        String value = cache.computeIfAbsent("key1", () -> "value1", String.class);
        assertEquals("value1", value);
        assertEquals("value1", cache.get("key1", String.class));
    }

}
