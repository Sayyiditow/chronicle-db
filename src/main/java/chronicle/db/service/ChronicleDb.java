package chronicle.db.service;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
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
 * Using this DB requires to use Value interfaces from Chronical Map:
 * https://github.com/OpenHFT/Chronicle-Values
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public final class ChronicleDb {
    public static final ChronicleDb CHRONICLE_DB = new ChronicleDb();
    public static final int CHRONICLE_SEGMENTS = 16;
    private static final ConcurrentMap<String, MapEntry> mapCache = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, MethodHandle> constructors = new ConcurrentHashMap<>();

    private static class MapEntry {
        final ChronicleMap<?, ?> map;
        final AtomicInteger refCount;

        MapEntry(final ChronicleMap<?, ?> map) {
            this.map = map;
            this.refCount = new AtomicInteger(1);// Start with a reference count of 1
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
     * @throws IOException
     * @return ChronicleMap or null, if null do not run close()
     */
    public <K, V> ChronicleMap<K, V> open(final String name, final long entries,
            final K averageKey, final V averageValue, final String filePath, final double maxBloatFactor)
            throws IOException {
        final MapEntry entry = mapCache.compute(filePath, (k, existingEntry) -> {
            if (existingEntry != null) {
                // Increment reference count for existing entry
                existingEntry.refCount.incrementAndGet();
                return existingEntry;
            }

            // Create a new entry
            try {
                final File file = new File(filePath);
                final Class<K> keyClass = (Class<K>) averageKey.getClass();
                final Class<V> valueClass = (Class<V>) averageValue.getClass();
                final ChronicleMapBuilder<K, V> builder = ChronicleMapBuilder.of(keyClass, valueClass)
                        .maxBloatFactor(maxBloatFactor).actualSegments(CHRONICLE_SEGMENTS);
                if (!file.exists()) {
                    builder.name(name).entries(entries).averageKey(averageKey).averageValue(averageValue);
                }
                final ChronicleMap<K, V> map = builder.createPersistedTo(file);
                return new MapEntry(map);
            } catch (final InterProcessDeadLockException deadlockEx) {
                Logger.warn("Deadlock detected when opening ChronicleMap [{}], attempting recovery.", filePath);
                try {
                    final ChronicleMap<K, V> recovered = recoverDb(name, entries, averageKey, averageValue, filePath,
                            maxBloatFactor);
                    return new MapEntry(recovered);
                } catch (final IOException recoveryEx) {
                    Logger.error("Failed to recover ChronicleMap [{}]. {}", filePath, recoveryEx);
                }
            } catch (final IOException e) {
                Logger.error("IOException for ChronicleMap initialization at [{}]. {}", filePath, e);
            }

            return null; // Failed to open or recover
        });

        if (entry != null) {
            return (ChronicleMap<K, V>) entry.map;
        }

        return null;
    }

    /**
     * Releases a reference to the ChronicleMap for the given filePath.
     * Closes the map and removes it from the cache when the last reference is
     * released.
     * 
     * @param filePath the path to the file to close
     */
    public void close(final String filePath) {
        mapCache.computeIfPresent(filePath, (k, entry) -> {
            if (entry.refCount.decrementAndGet() == 0) {
                entry.map.close();
                return null;
            }
            return entry; // Keep the entry
        });
    }

    /**
     * Use this only when jvm hangs on shutdown
     */
    public void closeAll() {
        for (final var entry : mapCache.entrySet()) {
            final String filePath = entry.getKey();
            final MapEntry mapEntry = entry.getValue();
            mapEntry.map.close();
            Logger.info("Closed ChronicleMap for [{}]", filePath);
        }
        mapCache.clear(); // Clear all cached entries
        Logger.info("All ChronicleMaps have been closed and mapCache cleared.");
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
