package org.omnaest.repository.nitrite;

import static org.junit.Assert.assertEquals;

import java.util.stream.Collectors;

import org.junit.Test;
import org.omnaest.utils.FileUtils;
import org.omnaest.utils.repository.IndexElementRepository;

/**
 * @see NitriteElementRepository
 * @author omnaest
 */
public class NitriteElementRepositoryTest
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
        IndexElementRepository<Domain> repository = new NitriteElementRepository<>(Domain.class, FileUtils.createRandomTempFile()).clear();

        assertEquals("value1", repository.get(repository.add(new Domain().setField("value1")))
                                         .getField());

        assertEquals("value2", repository.get(repository.add(new Domain().setField("value2")))
                                         .getField());

        assertEquals("value3", repository.get(repository.add(new Domain().setField("value3")))
                                         .getField());

        assertEquals("value4value5", repository.add(new Domain().setField("value4"), new Domain().setField("value5"))
                                               .mapToObj(id -> repository.get(id)
                                                                         .getField())
                                               .collect(Collectors.joining()));

    }

}
