package org.omnaest.repository.nitrite;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.dizitart.no2.Document;
import org.dizitart.no2.Nitrite;
import org.dizitart.no2.NitriteBuilder;
import org.dizitart.no2.mapper.JacksonMapper;
import org.dizitart.no2.mapper.NitriteMapper;
import org.dizitart.no2.objects.Cursor;
import org.dizitart.no2.objects.Id;
import org.dizitart.no2.objects.ObjectRepository;
import org.dizitart.no2.objects.filters.ObjectFilters;
import org.omnaest.utils.ObjectUtils;
import org.omnaest.utils.StreamUtils;
import org.omnaest.utils.element.cached.CachedElement;
import org.omnaest.utils.repository.IndexElementRepository;

/**
 * {@link IndexElementRepository} based on the {@link Nitrite} database
 * 
 * @author omnaest
 * @param <D>
 */
public class NitriteElementRepository<D> implements IndexElementRepository<D>
{
    private CachedElement<DatabaseAndRepository<D>> repository = CachedElement.of(() -> this.createDatabase());
    private Class<D>                                type;
    private File                                    file;
    private String                                  username;
    private String                                  password;
    private Supplier<Supplier<Long>>                idSupplier;

    private static class DatabaseAndRepository<D>
    {
        private Supplier<ObjectRepository<Element>> repository;
        private Nitrite                             database;

        public DatabaseAndRepository(Supplier<ObjectRepository<Element>> repository, Nitrite database)
        {
            super();
            this.repository = repository;
            this.database = database;
        }

        public <R> R executeWriteOnRepositoryAndGet(Function<ObjectRepository<Element>, R> operation)
        {
            R retval = operation.apply(this.repository.get());
            this.database.commit();
            return retval;
        }

        public <R> R executeReadOnRepositoryAndGet(Function<ObjectRepository<Element>, R> operation)
        {
            R retval = operation.apply(this.repository.get());
            return retval;
        }

        public void executeWriteOnRepository(Consumer<ObjectRepository<Element>> operation)
        {
            operation.accept(this.repository.get());
            this.database.commit();
        }

        public void closeDatabase()
        {
            this.database.close();
        }
    }

    private static class Element
    {
        @Id
        private long   id;
        private Object element;

        public long getId()
        {
            return this.id;
        }

        public Element setId(long id)
        {
            this.id = id;
            return this;
        }

        @SuppressWarnings("unchecked")
        public <D> D getElement()
        {
            return (D) this.element;
        }

        public Element setElement(Object element)
        {
            this.element = element;
            return this;
        }

        public static Element of(long id, Object element)
        {
            return new Element().setId(id)
                                .setElement(element);
        }

    }

    public NitriteElementRepository(Class<D> type, File file)
    {
        super();
        this.type = type;
        this.file = file;

        this.idSupplier = CachedElement.of((Supplier<Supplier<Long>>) () ->
        {
            AtomicLong id = new AtomicLong(this.ids()
                                               .max()
                                               .orElse(0));
            return () -> id.getAndIncrement();
        });
    }

    public IndexElementRepository<D> withCredentials(String username, String password)
    {
        this.username = username;
        this.password = password;
        return this;
    }

    private DatabaseAndRepository<D> createDatabase()
    {
        NitriteBuilder builder = Nitrite.builder()
                                        .compressed()
                                        .nitriteMapper(this.createMapper(this.type))
                                        .filePath(this.file);
        Nitrite db = this.username != null ? builder.openOrCreate(this.username, this.password) : builder.openOrCreate();

        return new DatabaseAndRepository<D>(() -> db.getRepository(Element.class), db);
    }

    private NitriteMapper createMapper(Class<D> elementType)
    {
        return new NitriteMapper()
        {
            private NitriteMapper nitriteMapper = new JacksonMapper();

            @Override
            public <T> Document asDocument(T object)
            {
                return this.nitriteMapper.asDocument(object);
            }

            @SuppressWarnings("unchecked")
            @Override
            public <T> T asObject(Document document, Class<T> type)
            {
                Object object = this.nitriteMapper.asObject((Document) document.get("element"), (Class<Object>) elementType);
                long id = (Long) document.get("id");
                T element = (T) Element.of(id, object);
                return element;
            }

            @Override
            public boolean isValueType(Object object)
            {
                return this.nitriteMapper.isValueType(object);
            }

            @Override
            public Object asValue(Object object)
            {
                return this.nitriteMapper.asValue(object);
            }

            @Override
            public Document parse(String json)
            {
                return this.nitriteMapper.parse(json);
            }

            @Override
            public String toJson(Object object)
            {
                return this.nitriteMapper.toJson(object);
            }

        };
    }

    @Override
    public Long add(D element)
    {
        return this.repository.get()
                              .executeWriteOnRepositoryAndGet(repository ->
                              {
                                  long id = this.idSupplier.get()
                                                           .get();
                                  repository.insert(Element.of(id, element));
                                  return id;
                              });
    }

    @Override
    public LongStream add(Stream<D> elements)
    {
        return this.repository.get()
                              .executeWriteOnRepositoryAndGet(repository -> elements.mapToLong(element ->
                              {
                                  long id = this.idSupplier.get()
                                                           .get();
                                  repository.insert(Element.of(id, element));
                                  return id;
                              })
                                                                                    .boxed()
                                                                                    .collect(Collectors.toList())
                                                                                    .stream()
                                                                                    .mapToLong(v -> v));
    }

    @Override
    public void update(Long id, D element)
    {
        this.repository.get()
                       .executeWriteOnRepository(repository -> repository.update(Element.of(id, element)));

    }

    @Override
    public void delete(Long id)
    {
        this.repository.get()
                       .executeWriteOnRepository(repository -> repository.remove(Element.of(id, this.get(id))));

    }

    @Override
    public D get(Long id)
    {
        return this.repository.get()
                              .executeReadOnRepositoryAndGet(repository -> ObjectUtils.getIfNotNull(repository.find(ObjectFilters.eq("id", id))
                                                                                                              .firstOrDefault(),
                                                                                                    element -> element.getElement()));
    }

    @Override
    public long size()
    {
        return this.repository.get()
                              .executeReadOnRepositoryAndGet(repository -> repository.size());
    }

    @Override
    public IndexElementRepository<D> clear()
    {
        this.repository.get()
                       .executeWriteOnRepository(repository -> repository.drop());
        return this;
    }

    @Override
    public LongStream ids()
    {
        Cursor<Element> cursor = this.repository.get()
                                                .executeReadOnRepositoryAndGet(repository -> repository.find());
        return StreamUtils.fromIterator(cursor.iterator())
                          .mapToLong(element -> element.getId());
    }

    @Override
    public IndexElementRepository<D> close()
    {
        this.repository.get()
                       .closeDatabase();
        return this;
    }
}
