package org.omnaest.repository.nitrite;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.omnaest.utils.MapperUtils;
import org.omnaest.utils.repository.IndexElementRepository;
import org.omnaest.utils.supplier.SupplierConsumer;

public class NitriteIndexElementRepository<D> extends NitriteElementRepository<Long, D> implements IndexElementRepository<D>
{

    public NitriteIndexElementRepository(Class<D> type, File file)
    {
        super(type, file, null);

        this.idSupplier = (Supplier<SupplierConsumer<Long>>) () ->
        {
            AtomicLong id = new AtomicLong(this.ids()
                                               .mapToLong(MapperUtils.identitiyForLongAsUnboxed())
                                               .max()
                                               .orElse(-1));
            return new SupplierConsumer<Long>()
            {
                @Override
                public Long get()
                {
                    return id.incrementAndGet();
                }

                @Override
                public void accept(Long value)
                {
                    id.updateAndGet(v -> Math.max(v, value));
                }
            };
        };
    }

    @Override
    public NitriteIndexElementRepository<D> clear()
    {
        super.clear();
        return this;
    }

    @Override
    public NitriteIndexElementRepository<D> withCredentials(String username, String password)
    {
        super.withCredentials(username, password);
        return this;
    }

    @Override
    public NitriteIndexElementRepository<D> usingAutoCommit(AutoCommitMode autoCommitMode)
    {
        super.usingAutoCommit(autoCommitMode);
        return this;
    }

}
