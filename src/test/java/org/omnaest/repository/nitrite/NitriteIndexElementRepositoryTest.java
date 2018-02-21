package org.omnaest.repository.nitrite;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Collectors;

import org.junit.Test;
import org.omnaest.repository.nitrite.NitriteElementRepository.AutoCommitMode;
import org.omnaest.utils.FileUtils;
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

    }

    @Test
    public void testAddAndGet() throws Exception
    {
        try (NitriteIndexElementRepository<Domain> repositoryRoot = new NitriteIndexElementRepository<>(Domain.class, FileUtils.createRandomTempFile());
                IndexElementRepository<Domain> repository = repositoryRoot.usingAutoCommit(AutoCommitMode.COMMIT_AFTER_EACH_WRITE_OPERATION)
                                                                          .clear())
        {

            assertEquals("value1", repository.get(repository.add(new Domain().setField("value1")))
                                             .getField());

            assertEquals("value2", repository.get(repository.add(new Domain().setField("value2")))
                                             .getField());

            assertEquals("value3", repository.get(repository.add(new Domain().setField("value3")))
                                             .getField());

            assertEquals("value4value5", repository.add(new Domain().setField("value4"), new Domain().setField("value5"))
                                                   .map(id -> repository.get(id)
                                                                        .getField())
                                                   .collect(Collectors.joining()));
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
            repository.update(100l, new SetDomain("123"));
            SetDomain value = repository.get(100l);
            assertEquals("123", value.iterator()
                                     .next());
        }
    }

}
