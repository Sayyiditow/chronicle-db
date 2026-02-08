package chronicle.db.service;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.tinylog.Logger;

import chronicle.db.dao.ChronicleDao;
import net.openhft.chronicle.hash.locks.InterProcessDeadLockException;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

/**
 * Service for managing ChronicleMap instances with reference counting and
 * caching.
 * <p>
 * This singleton provides shared access to ChronicleMap instances, ensuring
 * that
 * multiple callers can safely share the same map file without conflicts. It
 * implements
 * reference counting to track active users and automatically closes maps when
 * no longer needed.
 * </p>
 * <p>
 * <b>Key Features:</b>
 * <ul>
 * <li>Reference-counted shared map instances</li>
 * <li>Automatic map recovery from abnormal terminations</li>
 * <li>Thread-safe concurrent access</li>
 * <li>Support for ChronicleDao reflective operations</li>
 * </ul>
 * </p>
 * <p>
 * <b>Important:</b> Using ChronicleMap with complex value types requires
 * implementing
 * Value interfaces from Chronicle-Values. See:
 * <a href="https://github.com/OpenHFT/Chronicle-Values">Chronicle-Values
 * Documentation</a>
 * </p>
 * <p>
 * Usage example:
 * 
 * <pre>{@code
 * SharedChronicleMap<String, MyEntity> shared = CHRONICLE_DB.open(
 *         "mydb", 10000, "key", new MyEntity(), "/path/to/file.dat", 1.0);
 * try {
 *     shared.map.put("key1", entity);
 * } finally {
 *     shared.close(); // Decrements ref count
 * }
 * }</pre>
 * </p>
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public final class ChronicleDb {
    public static final ChronicleDb CHRONICLE_DB = new ChronicleDb();
    private static final ConcurrentMap<String, SharedChronicleMap> mapCache = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, MethodHandle> constructors = new ConcurrentHashMap<>();

    private ChronicleDb() {
    }

    /**
     * Wrapper for a shared ChronicleMap instance with reference counting.
     * <p>
     * This class ensures that a ChronicleMap file can be safely shared across
     * multiple callers. The underlying map is only closed when all references
     * have been released via {@link #close()}.
     * </p>
     * <p>
     * <b>Important:</b> Do not use try-with-resources when obtaining this from
     * {@link ChronicleDb#open}, as it will prematurely close the shared instance.
     * Instead, manually call {@link #close()} when done.
     * </p>
     * 
     * @param <K> The key type
     * @param <V> The value type
     */
    public static class SharedChronicleMap<K, V> implements AutoCloseable {
        /** The underlying ChronicleMap instance */
        public final ChronicleMap<K, V> map;

        private final AtomicInteger refCount;
        private final String filePath; // Track file path for cleanup

        SharedChronicleMap(final ChronicleMap<K, V> map, final String filePath) {
            this.map = map;
            this.filePath = filePath;
            this.refCount = new AtomicInteger(1);
        }

        /**
         * Increments the reference count when sharing this map instance.
         * <p>
         * Call this method when passing the shared map to another component
         * that will independently manage its lifecycle.
         * </p>
         * 
         * @return This SharedChronicleMap instance for chaining
         */
        SharedChronicleMap retain() {
            refCount.incrementAndGet();
            return this;
        }

        /**
         * Decrements the reference count and closes the map if no references remain.
         * <p>
         * This method is thread-safe and ensures the underlying ChronicleMap is
         * only closed when the last reference is released.
         * </p>
         */
        @Override
        public void close() {
            mapCache.computeIfPresent(filePath, (k, entry) -> {
                if (entry.refCount.decrementAndGet() == 0) {
                    entry.map.close();
                    return null;
                }
                return entry; // Keep the entry
            });
        }
    }

    /**
     * Opens a shared ChronicleMap instance. Call close(filePath) to release it.
     * Do not use try-with-resources as it will prematurely close the shared
     * instance.
     * 
     * @param entries    the number of entries of the db as a starter
     * @param averageKey the average key
     * @param filePath   the path to the file to create
     * @param keyClass   the class of the key
     * @param valueClass the class of the value (best to implement Value interface
     *                   for complex structures)
     * @return ChronicleMap or null, if null do not run close()
     */
    public <K, V> SharedChronicleMap open(final String name, final long entries,
            final K averageKey, final V averageValue, final String filePath, final double maxBloatFactor) {
        final SharedChronicleMap entry = mapCache.compute(filePath, (k, existingEntry) -> {
            if (existingEntry != null) {
                return existingEntry.retain();
            }

            // Create a new entry
            try {
                final File file = new File(filePath);
                final Class<K> keyClass = (Class<K>) averageKey.getClass();
                final Class<V> valueClass = (Class<V>) averageValue.getClass();
                final ChronicleMapBuilder<K, V> builder = ChronicleMapBuilder.of(keyClass, valueClass)
                        .maxBloatFactor(maxBloatFactor);
                if (!file.exists()) {
                    builder.name(name).entries(entries).averageKey(averageKey).averageValue(averageValue);
                }
                final ChronicleMap<K, V> map = builder.createPersistedTo(file);
                // check for locks early
                map.size();
                return new SharedChronicleMap(map, filePath);
            } catch (final InterProcessDeadLockException deadlockEx) {
                Logger.warn("Deadlock detected when opening ChronicleMap [{}], attempting recovery.", filePath);
                // Backup the ChronicleMap file
                final var filePathPath = Path.of(filePath);
                final var backupFolder = filePathPath.getParent().resolveSibling("backup");
                final var backupFile = backupFolder.resolve(filePathPath.getFileName());

                try {
                    Files.createDirectories(backupFolder); // ensure backup folder exists
                    Files.copy(filePathPath, backupFile, StandardCopyOption.REPLACE_EXISTING,
                            StandardCopyOption.COPY_ATTRIBUTES);
                    Logger.warn("Backed up ChronicleMap file to [{}] before recovery.", backupFile);
                    final ChronicleMap<K, V> recovered = recoverDb(name, entries, averageKey, averageValue, filePath,
                            maxBloatFactor);
                    return new SharedChronicleMap(recovered, filePath);
                } catch (final IOException recoveryEx) {
                    Logger.error("Failed to recover ChronicleMap at [{}]", filePath);
                    throw new UncheckedIOException(recoveryEx);
                }
            } catch (final IOException e) {
                Logger.error("Failed to open ChronicleMap at [{}]", filePath);
                throw new UncheckedIOException(e);
            }
        });

        return entry;
    }

    /**
     * Use this only when jvm hangs on shutdown
     */
    public void close(final String filePath) {
        mapCache.computeIfPresent(filePath, (k, mapEntry) -> {
            mapEntry.map.close();
            Logger.debug("Closed ChronicleMap at [{}]", filePath);
            return null;
        });
    }

    /**
     * Use this only when jvm hangs on shutdown
     */
    public void closeAll() {
        for (final var entry : mapCache.entrySet()) {
            final String filePath = entry.getKey();
            final SharedChronicleMap mapEntry = entry.getValue();
            mapEntry.map.close();
            Logger.debug("Closed ChronicleMap at [{}]", filePath);
        }
        mapCache.clear(); // Clear all cached entries
        Logger.debug("All ChronicleMaps have been closed and mapCache cleared.");
    }

    /**
     * Run this on app startup to check and fix if there were any abnormal
     * terminations
     * 
     * @param filePath   the path to the file to with the data
     * @param keyClass   the class of the key
     * @param valueClass the class of the value (best to implement Value interface
     *                   for complex structures)
     */
    public <K, V> ChronicleMap<K, V> recoverDb(final String name, final long entries,
            final K averageKey, final V averageValue, final String filePath, final double maxBloatFactor)
            throws IOException {
        Logger.info("Restoring ChronicleMap {} at: {}", name, filePath);
        final File file = new File(filePath);
        final Class<K> keyClass = (Class<K>) averageKey.getClass();
        final Class<V> valueClass = (Class<V>) averageValue.getClass();

        return ChronicleMap.of(keyClass, valueClass).name(name).entries(entries).averageKey(averageKey)
                .averageValue(averageValue).maxBloatFactor(maxBloatFactor).recoverPersistedTo(file, true);
    }

    /**
     * Gets the Chronicle dao object to run different methods such as CRUD
     * reflectively
     * 
     * @param daoClassName       the full package class name for the dao
     * @param daoClassObjectName the static object name
     * 
     * @return ChronicleDao
     * @throws Throwable
     */
    public ChronicleDao getChronicleDao(final String daoClassName, final String dataPath) throws Throwable {
        final MethodHandle constructor = constructors.computeIfAbsent(daoClassName, className -> {
            try {
                final var objClass = Class.forName(className);
                for (final var con : objClass.getDeclaredConstructors()) {
                    if (con.getParameterCount() > 0) {
                        return MethodHandles.lookup().unreflectConstructor(con);
                    }
                }
                throw new RuntimeException("No constructor with parameters found for " + daoClassName);
            } catch (final ClassNotFoundException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        });

        return (ChronicleDao) constructor.invoke(dataPath);
    }

    public <V> Map<String, V> getMapForMultiInserts(final ChronicleDao<V> dao) {
        return new HashMap<String, V>(1000);
    }

    public <V> ConcurrentMap<String, V> getConcurrentMapForMultiInserts(final ChronicleDao<V> dao) {
        return new ConcurrentHashMap<String, V>(1000);
    }
}
