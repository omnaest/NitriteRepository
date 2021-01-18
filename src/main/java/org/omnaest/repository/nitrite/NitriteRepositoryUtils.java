package org.omnaest.repository.nitrite;

import java.io.File;

import org.omnaest.utils.CacheUtils;
import org.omnaest.utils.cache.Cache;
import org.omnaest.utils.repository.MapElementRepository;
import org.omnaest.utils.supplier.SupplierConsumer;

/**
 * Helper to create {@link NitriteElementRepository} and {@link Cache} instances
 * 
 * @author omnaest
 */
public class NitriteRepositoryUtils
{
    private NitriteRepositoryUtils()
    {
    }

    public static <I extends Comparable<I>, D> NitriteElementRepository<I, D> newElementRepository(Class<D> type, File file, SupplierConsumer<I> idSupplier)
    {
        return new NitriteElementRepository<>(type, file, () -> idSupplier);
    }

    public static <I extends Comparable<I>, D> MapElementRepository<I, D> newMapElementRepository(Class<D> type, File file)
    {
        return newElementRepository(type, file, new SupplierConsumer<I>()
        {
            @Override
            public I get()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void accept(I t)
            {
                // do nothing
            }
        });
    }

    public static <D> NitriteIndexElementRepository<D> newIndexElementRepository(Class<D> type, File file)
    {
        return new NitriteIndexElementRepository<>(type, file);
    }

    public static Cache newLocalCache(String name)
    {
        return newCache(new File(CacheUtils.DEFAULT_CACHE_FOLDER, name + ".dat"));
    }

    public static Cache newCache(File file)
    {
        return new NitriteCache(file);
    }
}
