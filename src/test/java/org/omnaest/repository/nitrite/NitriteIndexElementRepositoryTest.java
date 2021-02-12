package org.omnaest.repository.nitrite;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Test;
import org.omnaest.repository.nitrite.NitriteElementRepository.AutoCommitMode;
import org.omnaest.utils.FileUtils;
import org.omnaest.utils.MapUtils;
import org.omnaest.utils.repository.IndexElementRepository;

/**
 * @see NitriteIndexElementRepository
 * @author omnaest
 */
public class NitriteIndexElementRepositoryTest
{
    @SuppressWarnings("unused")
    private static class Domain
    {
        private long   id;
        private String field;

        public long getId()
        {
            return this.id;
        }

        public Domain setId(long id)
        {
            this.id = id;
            return this;
        }

        public String getField()
        {
            return this.field;
        }

        public Domain setField(String field)
        {
            this.field = field;
            return this;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((this.field == null) ? 0 : this.field.hashCode());
            result = prime * result + (int) (this.id ^ (this.id >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (obj == null)
            {
                return false;
            }
            if (this.getClass() != obj.getClass())
            {
                return false;
            }
            Domain other = (Domain) obj;
            if (this.field == null)
            {
                if (other.field != null)
                {
                    return false;
                }
            }
            else if (!this.field.equals(other.field))
            {
                return false;
            }
            if (this.id != other.id)
            {
                return false;
            }
            return true;
        }

    }

    @Test
    public void testAddAndGet() throws Exception
    {
        try (NitriteIndexElementRepository<Domain> repositoryRoot = new NitriteIndexElementRepository<>(Domain.class, FileUtils.createRandomTempFile());
                IndexElementRepository<Domain> repository = repositoryRoot.usingAutoCommit(AutoCommitMode.COMMIT_AFTER_EACH_WRITE_OPERATION)
                                                                          .clear())
        {

            assertEquals("value1", repository.getValue(repository.add(new Domain().setField("value1")))
                                             .getField());

            assertEquals("value2", repository.getValue(repository.add(new Domain().setField("value2")))
                                             .getField());

            assertEquals("value3", repository.getValue(repository.add(new Domain().setField("value3")))
                                             .getField());

            assertEquals("value4value5", repository.addAll(new Domain().setField("value4"), new Domain().setField("value5"))
                                                   .map(id -> repository.getValue(id)
                                                                        .getField())
                                                   .collect(Collectors.joining()));
        }
    }

    @Test
    public void testPutAndGetAll() throws Exception
    {
        try (NitriteIndexElementRepository<Domain> repositoryRoot = new NitriteIndexElementRepository<>(Domain.class, FileUtils.createRandomTempFile());
                IndexElementRepository<Domain> repository = repositoryRoot.usingAutoCommit(AutoCommitMode.COMMIT_AFTER_EACH_WRITE_OPERATION)
                                                                          .clear())
        {
            Map<Long, Domain> entries1 = MapUtils.builder()
                                                 .put(1l, new Domain().setField("value1"))
                                                 .put(2l, new Domain().setField("value2"))
                                                 .build();
            Map<Long, Domain> entries2 = MapUtils.builder()
                                                 .put(2l, new Domain().setField("value2"))
                                                 .put(3l, new Domain().setField("value3"))
                                                 .build();
            repository.putAll(entries1);
            repository.putAll(entries2);

            assertEquals(MapUtils.merge(entries1, entries2), repository.getAll(1l, 2l, 3l));
        }

    }

    private static class SetDomain extends HashSet<String>
    {
        private static final long serialVersionUID = 6926794468284034605L;

        @SuppressWarnings("unused")
        SetDomain()
        {
            super();
        }

        public SetDomain(Collection<? extends String> c)
        {
            super(c);
        }

        public SetDomain(String element)
        {
            this(Arrays.asList(element));
        }
    }

    @Test
    public void testUpdateWithId() throws IOException
    {
        try (NitriteIndexElementRepository<SetDomain> repositoryRoot = new NitriteIndexElementRepository<>(SetDomain.class, FileUtils.createRandomTempFile());
                IndexElementRepository<SetDomain> repository = repositoryRoot.usingAutoCommit(AutoCommitMode.COMMIT_AFTER_EACH_WRITE_OPERATION)
                                                                             .clear())
        {
            repository.put(100l, new SetDomain("123"));
            SetDomain value = repository.getValue(100l);
            assertEquals("123", value.iterator()
                                     .next());
        }
    }

    @Test
    public void testRemoveWithId() throws IOException
    {
        try (NitriteIndexElementRepository<SetDomain> repositoryRoot = new NitriteIndexElementRepository<>(SetDomain.class, FileUtils.createRandomTempFile());
                IndexElementRepository<SetDomain> repository = repositoryRoot.usingAutoCommit(AutoCommitMode.COMMIT_AFTER_EACH_WRITE_OPERATION)
                                                                             .clear())
        {
            repository.put(100l, new SetDomain("123"));
            repository.remove(100l);
            assertEquals(null, repository.getValue(100l));
        }
    }

}
