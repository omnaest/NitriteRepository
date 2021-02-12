package org.omnaest.repository.nitrite;

import java.io.File;
import java.util.Date;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.omnaest.utils.JSONHelper;
import org.omnaest.utils.cache.internal.AbstractCache;
import org.omnaest.utils.duration.TimeDuration;
import org.omnaest.utils.supplier.SupplierConsumer;

public class NitriteCache extends AbstractCache
{
    private NitriteElementRepository<String, ElementAndType> repository;

    private static class ElementAndType
    {
        private Object   value;
        private Class<?> type;
        private Date     modificationDate;

        public ElementAndType(Object value, Class<?> type)
        {
            super();
            this.value = value;
            this.type = type;
            this.modificationDate = new Date();
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

        public Date getModificationDate()
        {
            return this.modificationDate;
        }

    }

    @SuppressWarnings("resource")
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
        this.repository = new NitriteElementRepository<String, ElementAndType>(ElementAndType.class, file, idSupplier).withIgnoreMappingExceptions();
    }

    @Override
    public <V> V get(String key, Class<V> type)
    {
        return Optional.ofNullable(this.repository.getValue(key))
                       .map(elementAndType -> JSONHelper.toObjectWithType(elementAndType.getValue(), type))
                       .orElse(null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> Class<V> getType(String key)
    {
        return (Class<V>) this.repository.getValue(key)
                                         .getType();
    }

    @Override
    public void put(String key, Object value)
    {
        this.repository.put(key, new ElementAndType(value, value != null ? value.getClass() : null));
    }

    @Override
    public <V> V computeIfAbsent(String key, Supplier<V> supplier, Class<V> type)
    {
        V retval = this.get(key, type);
        if (retval == null)
        {
            retval = supplier.get();
            this.put(key, retval);
        }

        return retval;
    }

    @Override
    public void remove(String key)
    {
        this.repository.remove(key);
    }

    @Override
    public Set<String> keySet()
    {
        return this.repository.ids()
                              .collect(Collectors.toSet());
    }

    @Override
    public TimeDuration getAge(String key)
    {
        return Optional.ofNullable(this.repository.getValue(key))
                       .map(elementAndType -> elementAndType.getModificationDate())
                       .map(modificationDate -> TimeDuration.between(new Date(), modificationDate))
                       .orElse(null);
    }

}
