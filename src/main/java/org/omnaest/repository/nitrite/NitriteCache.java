package org.omnaest.repository.nitrite;

import java.io.File;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.omnaest.utils.cache.AbstractCache;
import org.omnaest.utils.cache.Cache;
import org.omnaest.utils.supplier.SupplierConsumer;

public class NitriteCache extends AbstractCache implements Cache
{
    private NitriteElementRepository<String, ElementAndType> repository;

    private static class ElementAndType
    {
        private Object   value;
        private Class<?> type;

        public ElementAndType(Object value, Class<?> type)
        {
            super();
            this.value = value;
            this.type = type;
        }

        @SuppressWarnings("unused")
        ElementAndType()
        {
            super();
        }

        public Object getValue()
        {
            return this.value;
        }

        public Class<?> getType()
        {
            return this.type;
        }

    }

    public NitriteCache(File file)
    {
        super();
        Supplier<SupplierConsumer<String>> idSupplier = () -> new SupplierConsumer<String>()
        {
            @Override
            public String get()
            {
                return null;
            }

            @Override
            public void accept(String t)
            {
                //do nothing
            }
        };
        this.repository = new NitriteElementRepository<String, ElementAndType>(ElementAndType.class, file, idSupplier);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> V get(String key, Class<V> type)
    {
        return (V) this.repository.get(key)
                                  .getValue();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> Class<V> getType(String key)
    {
        return (Class<V>) this.repository.get(key)
                                         .getType();
    }

    @Override
    public void put(String key, Object value)
    {
        this.repository.update(key, new ElementAndType(value, value != null ? value.getClass() : null));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> V computeIfAbsent(String key, Supplier<V> supplier, Class<V> type)
    {
        V retval = null;

        ElementAndType entry = this.repository.get(key);
        if (entry != null)
        {
            retval = (V) entry.getValue();
        }
        else
        {
            retval = supplier.get();
            this.put(key, retval);
        }

        return retval;
    }

    @Override
    public void remove(String key)
    {
        this.repository.delete(key);
    }

    @Override
    public Set<String> keySet()
    {
        return this.repository.ids()
                              .collect(Collectors.toSet());
    }

}