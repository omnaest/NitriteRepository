package org.omnaest.repository.nitrite;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.dizitart.no2.Document;
import org.dizitart.no2.Nitrite;
import org.dizitart.no2.NitriteBuilder;
import org.dizitart.no2.mapper.JacksonMapper;
import org.dizitart.no2.mapper.NitriteMapper;
import org.dizitart.no2.objects.Cursor;
import org.dizitart.no2.objects.Id;
import org.dizitart.no2.objects.ObjectRepository;
import org.dizitart.no2.objects.filters.ObjectFilters;
import org.omnaest.utils.ExceptionUtils;
import org.omnaest.utils.MapperUtils;
import org.omnaest.utils.ObjectUtils;
import org.omnaest.utils.StreamUtils;
import org.omnaest.utils.ThreadUtils;
import org.omnaest.utils.element.cached.CachedElement;
import org.omnaest.utils.lock.SynchronizedAtLeastOneTimeExecutor;
import org.omnaest.utils.repository.IndexElementRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link IndexElementRepository} based on the {@link Nitrite} database
 * 
 * @author omnaest
 * @param <D>
 */
public class NitriteElementRepository<D> implements IndexElementRepository<D>
{
    private static final Logger LOG = LoggerFactory.getLogger(NitriteElementRepository.class);

    private CachedElement<DatabaseAndRepository<D>> repository = CachedElement.of(() -> this.createDatabase());
    private Class<D>                                type;
    private File                                    file;
    private String                                  username;
    private String                                  password;
    private Supplier<Supplier<Long>>                idSupplier;
    private CommitExecutor<D>                       commitExecutor;

    private static class CommitExecutor<D>
    {
        private AutoCommitMode                     autoCommitMode = AutoCommitMode.COMMIT_AFTER_EACH_WRITE_OPERATION;
        private Supplier<DatabaseAndRepository<D>> repository;
        private SynchronizedAtLeastOneTimeExecutor onlyOneTimeExecutor;

        public CommitExecutor(Supplier<DatabaseAndRepository<D>> repository)
        {
            super();
            this.repository = repository;

            int numberOfThreads = 10 * Runtime.getRuntime()
                                              .availableProcessors();
            this.onlyOneTimeExecutor = new SynchronizedAtLeastOneTimeExecutor(Executors.newFixedThreadPool(numberOfThreads), () ->
            {
                ThreadUtils.sleepSilently(1, TimeUnit.SECONDS);
                LOG.info("Autocommit...");
                this.commitImmediate();
                LOG.info("...done");
            });
        }

        public void commit()
        {
            if (AutoCommitMode.COMMIT_AFTER_EACH_WRITE_OPERATION.equals(this.autoCommitMode))
            {
                this.commitImmediate();
            }
            else if (AutoCommitMode.COMMIT_AFTER_1_SECOND.equals(this.autoCommitMode))
            {
                this.onlyOneTimeExecutor.fire();
            }
        }

        public void setAutoCommitMode(AutoCommitMode autoCommitMode)
        {
            this.autoCommitMode = autoCommitMode;
        }

        public void close()
        {
            this.onlyOneTimeExecutor.shutdown()
                                    .awaitTermination(1, TimeUnit.MINUTES);
        }

        public void commitImmediate()
        {
            this.repository.get()
                           .getDatabase()
                           .commit();
        }

    }

    private static class DatabaseAndRepository<D>
    {
        private Supplier<ObjectRepository<Element>> repository;
        private Nitrite                             database;
        private CommitExecutor<D>                   commitExecutor;

        public DatabaseAndRepository(Supplier<ObjectRepository<Element>> repository, Nitrite database, CommitExecutor<D> commitExecutor)
        {
            super();
            this.repository = repository;
            this.database = database;
            this.commitExecutor = commitExecutor;
        }

        public Nitrite getDatabase()
        {
            return this.database;
        }

        public <R> R executeWriteOnRepositoryAndGet(Function<ObjectRepository<Element>, R> operation)
        {
            R retval = operation.apply(this.repository.get());
            this.executeCommitByAutoCommitMode();
            return retval;
        }

        private void executeCommitByAutoCommitMode()
        {
            this.commitExecutor.commit();
        }

        public <R> R executeReadOnRepositoryAndGet(Function<ObjectRepository<Element>, R> operation)
        {
            R retval = operation.apply(this.repository.get());
            return retval;
        }

        public void executeWriteOnRepository(Consumer<ObjectRepository<Element>> operation)
        {
            operation.accept(this.repository.get());
            this.executeCommitByAutoCommitMode();
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
                                               .mapToLong(MapperUtils.identitiyForLongAsUnboxed())
                                               .max()
                                               .orElse(0));
            return () -> id.getAndIncrement();
        });

        this.commitExecutor = new CommitExecutor<>(this.repository);
    }

    public IndexElementRepository<D> withCredentials(String username, String password)
    {
        this.username = username;
        this.password = password;
        return this;
    }

    public enum AutoCommitMode
    {
        COMMIT_AFTER_EACH_WRITE_OPERATION, COMMIT_AFTER_1_SECOND, AUTOCOMMIT_DISABLED
    }

    /**
     * Sets the {@link AutoCommitMode}, default is {@link AutoCommitMode#COMMIT_AFTER_EACH_WRITE_OPERATION}
     * 
     * @param autoCommitMode
     * @return
     */
    public NitriteElementRepository<D> usingAutoCommit(AutoCommitMode autoCommitMode)
    {
        this.commitExecutor.setAutoCommitMode(autoCommitMode);
        return this;
    }

    private DatabaseAndRepository<D> createDatabase()
    {
        ExceptionUtils.executeSilentVoid(() -> FileUtils.forceMkdirParent(this.file));
        NitriteBuilder builder = Nitrite.builder()
                                        .compressed()
                                        .nitriteMapper(this.createMapper(this.type))
                                        .filePath(this.file);
        Nitrite db = this.username != null ? builder.openOrCreate(this.username, this.password) : builder.openOrCreate();

        return new DatabaseAndRepository<D>(() -> db.getRepository(Element.class), db, this.commitExecutor);
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
                Object rawElement = document.get("element");
                Object object;
                if (rawElement instanceof Document)
                {
                    object = this.nitriteMapper.asObject((Document) rawElement, (Class<Object>) elementType);
                }
                else
                {
                    object = rawElement;
                }

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
        return this.getRepository()
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
        return this.getRepository()
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
        this.getRepository()
            .executeWriteOnRepository(repository -> repository.update(Element.of(id, element)));

    }

    @Override
    public void delete(Long id)
    {
        this.getRepository()
            .executeWriteOnRepository(repository -> repository.remove(Element.of(id, this.get(id))));

    }

    @Override
    public D get(Long id)
    {
        return this.getRepository()
                   .executeReadOnRepositoryAndGet(repository ->
                   {
                       Element element = repository.find(ObjectFilters.eq("id", id))
                                                   .firstOrDefault();
                       return ObjectUtils.getIfNotNull(element, e -> e.getElement());
                   });
    }

    @Override
    public long size()
    {
        return this.getRepository()
                   .executeReadOnRepositoryAndGet(repository -> repository.size());
    }

    @Override
    public IndexElementRepository<D> clear()
    {
        this.getRepository()
            .executeWriteOnRepository(repository -> repository.drop());
        this.commitExecutor.commitImmediate();
        return this;
    }

    private DatabaseAndRepository<D> getRepository()
    {
        return this.repository.get();
    }

    @Override
    public Stream<Long> ids()
    {
        Cursor<Element> cursor = this.getRepository()
                                     .executeReadOnRepositoryAndGet(repository -> repository.find());
        return StreamUtils.fromIterator(cursor.iterator())
                          .map(element -> element.getId());
    }

    @Override
    public void close()
    {
        LOG.info("Shutdown...");
        LOG.info("  ...executor...");
        this.commitExecutor.close();
        LOG.info("  ...repository...");
        this.getRepository()
            .closeDatabase();
        LOG.info("...done");
    }

    @Override
    public String toString()
    {
        return "NitriteElementRepository [" + this.file + "]";
    }

}
