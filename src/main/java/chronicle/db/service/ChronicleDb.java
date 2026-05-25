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
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.tinylog.Logger;

import chronicle.db.dao.ChronicleDao;
import net.openhft.chronicle.hash.locks.InterProcessDeadLockException;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.MapEntry;

/**
 * Service for managing ChronicleMap instances with reference counting,
 * caching, and automatic deadlock recovery.
 * <p>
 * This singleton provides shared access to ChronicleMap instances, ensuring
 * that multiple callers can safely share the same map file without conflicts.
 * It implements reference counting to track active users and automatically
 * closes maps when no longer needed.
 * </p>
 * <p>
 * <b>Key Features:</b>
 * <ul>
 * <li>Reference-counted shared map instances</li>
 * <li>Automatic deadlock detection and recovery via
 * {@link SharedChronicleMap}</li>
 * <li>Thread-safe concurrent access</li>
 * <li>Support for ChronicleDao reflective operations</li>
 * </ul>
 * </p>
 * <p>
 * <b>Important:</b> Using ChronicleMap with complex value types requires
 * implementing Value interfaces from Chronicle-Values. See:
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
 *     shared.put("key1", entity);
 *     MyEntity value = shared.get("key1");
 * } finally {
 *     shared.close(); // Decrements ref count
 * }
 * }</pre>
 * </p>
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public final class ChronicleDb {
    public static final ChronicleDb CHRONICLE_DB = new ChronicleDb();
    // Initial bin count for the cache — see MapDb for rationale. Override via
    // -Dchronicle.cache.initialCapacity=N to match deployment file count.
    private static final ConcurrentMap<String, SharedChronicleMap> mapCache = new ConcurrentHashMap<>(
            Integer.getInteger("chronicle.cache.initialCapacity", 1024));
    private static final ConcurrentMap<String, MethodHandle> constructors = new ConcurrentHashMap<>(
            Integer.getInteger("chronicle.constructorCache.initialCapacity", 256));

    private ChronicleDb() {
    }

    /**
     * Wrapper for a shared ChronicleMap instance with reference counting and
     * automatic deadlock recovery.
     * <p>
     * This class ensures that a ChronicleMap file can be safely shared across
     * multiple callers. The underlying map is only closed when all references
     * have been released via {@link #close()}.
     * </p>
     * <p>
     * <b>Deadlock Protection:</b> All map operations ({@link #put}, {@link #get},
     * {@link #remove}, {@link #forEachEntry}, etc.) are wrapped to catch
     * {@link InterProcessDeadLockException}. When a deadlock is detected, the map
     * is automatically recovered in place and the operation is retried once.
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
        private volatile ChronicleMap<K, V> map;
        private final AtomicInteger refCount;
        private final String filePath;

        // Builder params for recovery
        private final String name;
        private final long entries;
        private final int averageKeySize;
        private final V averageValue;
        private final double maxBloatFactor;

        SharedChronicleMap(final ChronicleMap<K, V> map, final String filePath,
                final String name, final long entries, final int averageKeySize,
                final V averageValue, final double maxBloatFactor) {
            this.map = map;
            this.filePath = filePath;
            this.refCount = new AtomicInteger(1);
            this.name = name;
            this.entries = entries;
            this.averageKeySize = averageKeySize;
            this.averageValue = averageValue;
            this.maxBloatFactor = maxBloatFactor;
        }

        public String getFilePath() {
            return filePath;
        }

        /**
         * Recovers the map by closing the broken instance, backing up the file,
         * and re-opening with recoverPersistedTo. Synchronized so only one thread
         * performs recovery if multiple threads hit the deadlock concurrently.
         */
        private synchronized void recoverAndSwap(final ChronicleMap<K, V> brokenMap) {
            if (map != brokenMap) {
                Logger.debug("Map [{}] already recovered by another thread. Skipping.", filePath);
                return;
            }
            Logger.warn("Deadlock on [{}]. Recovering and swapping map...", filePath);
            try {
                map.close();
                CHRONICLE_DB.backupCorruptedFile(filePath);
                map = (ChronicleMap<K, V>) CHRONICLE_DB.recoverDb(name, entries, averageKeySize, averageValue,
                        filePath, maxBloatFactor);
                Logger.info("Successfully recovered and swapped map [{}]", filePath);
            } catch (final IOException e) {
                Logger.error("Failed to recover map [{}]: {}", filePath, e.getMessage());
                throw new UncheckedIOException(e);
            }
        }

        /**
         * Safely performs a put operation, recovering and retrying if deadlock is
         * detected.
         */
        public V put(final K key, final V value) {
            final var current = map;
            try {
                return current.put(key, value);
            } catch (final InterProcessDeadLockException e) {
                recoverAndSwap(current);
                return map.put(key, value);
            } catch (final IllegalStateException e) {
                if (map != current) {
                    return map.put(key, value);
                }
                throw e;
            }
        }

        /**
         * Safely performs a get operation, recovering and retrying if deadlock or
         * closed map is detected.
         */
        public V get(final K key) {
            final var current = map;
            try {
                return current.get(key);
            } catch (final InterProcessDeadLockException e) {
                recoverAndSwap(current);
                return map.get(key);
            } catch (final IllegalStateException e) {
                if (map != current) {
                    return map.get(key);
                }
                throw e;
            }
        }

        /**
         * Safely performs a getUsing operation, recovering and retrying if deadlock is
         * detected.
         */
        public V getUsing(final K key, final V usingValue) {
            final var current = map;
            try {
                return current.getUsing(key, usingValue);
            } catch (final InterProcessDeadLockException e) {
                recoverAndSwap(current);
                return map.getUsing(key, usingValue);
            } catch (final IllegalStateException e) {
                if (map != current) {
                    return map.getUsing(key, usingValue);
                }
                throw e;
            }
        }

        /**
         * Safely checks if key exists, recovering and retrying if deadlock is detected.
         */
        public boolean containsKey(final K key) {
            final var current = map;
            try {
                return current.containsKey(key);
            } catch (final InterProcessDeadLockException e) {
                recoverAndSwap(current);
                return map.containsKey(key);
            } catch (final IllegalStateException e) {
                if (map != current) {
                    return map.containsKey(key);
                }
                throw e;
            }
        }

        /**
         * Safely performs a remove operation, recovering and retrying if deadlock is
         * detected.
         */
        public V remove(final K key) {
            final var current = map;
            try {
                return current.remove(key);
            } catch (final InterProcessDeadLockException e) {
                recoverAndSwap(current);
                return map.remove(key);
            } catch (final IllegalStateException e) {
                if (map != current) {
                    return map.remove(key);
                }
                throw e;
            }
        }

        /**
         * Safely performs a putAll operation from another SharedChronicleMap.
         */
        public void putAll(final SharedChronicleMap<K, V> source) {
            final var current = map;
            try {
                current.putAll(source.map);
            } catch (final InterProcessDeadLockException e) {
                recoverAndSwap(current);
                map.putAll(source.map);
            } catch (final IllegalStateException e) {
                if (map != current) {
                    map.putAll(source.map);
                    return;
                }
                throw e;
            }
        }

        /**
         * Copies the map contents to a new HashMap.
         */
        public Map<K, V> toHashMap() {
            return new HashMap<>(map);
        }

        /**
         * Safely iterates over map entries, recovering and retrying if deadlock is
         * detected.
         */
        public void forEachEntry(final Consumer<MapEntry<K, V>> action) {
            final var current = map;
            try {
                current.forEachEntry(action);
            } catch (final InterProcessDeadLockException e) {
                recoverAndSwap(current);
                map.forEachEntry(action);
            } catch (final IllegalStateException e) {
                if (map != current) {
                    map.forEachEntry(action);
                    return;
                }
                throw e;
            }
        }

        /**
         * Safely iterates over map entries with early termination, recovering and
         * retrying if deadlock is detected.
         */
        public boolean forEachEntryWhile(final Predicate<MapEntry<K, V>> predicate) {
            final var current = map;
            try {
                return current.forEachEntryWhile(predicate);
            } catch (final InterProcessDeadLockException e) {
                recoverAndSwap(current);
                return map.forEachEntryWhile(predicate);
            } catch (final IllegalStateException e) {
                if (map != current) {
                    return map.forEachEntryWhile(predicate);
                }
                throw e;
            }
        }

        public boolean isEmpty() {
            final var current = map;
            try {
                return current.isEmpty();
            } catch (final InterProcessDeadLockException e) {
                recoverAndSwap(current);
                return map.isEmpty();
            } catch (final IllegalStateException e) {
                if (map != current) {
                    return map.isEmpty();
                }
                throw e;
            }
        }

        public int size() {
            final var current = map;
            try {
                return current.size();
            } catch (final InterProcessDeadLockException e) {
                recoverAndSwap(current);
                return map.size();
            } catch (final IllegalStateException e) {
                if (map != current) {
                    return map.size();
                }
                throw e;
            }
        }

        public long longSize() {
            final var current = map;
            try {
                return current.longSize();
            } catch (final InterProcessDeadLockException e) {
                recoverAndSwap(current);
                return map.longSize();
            } catch (final IllegalStateException e) {
                if (map != current) {
                    return map.longSize();
                }
                throw e;
            }
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
         * <p>
         * Operates on the instance's own {@code refCount} so it works correctly
         * even after this entry has been soft-evicted from {@link ChronicleDb#mapCache}.
         * After closing, best-effort removes the cache entry if it still refers
         * to this instance.
         * </p>
         */
        @Override
        public void close() {
            if (refCount.decrementAndGet() == 0) {
                map.close();
                mapCache.computeIfPresent(filePath, (k, e) -> e == this ? null : e);
            }
        }
    }

    /**
     * Opens a shared ChronicleMap instance. Call close(filePath) to release it.
     * Do not use try-with-resources as it will prematurely close the shared
     * instance.
     *
     * @param entries        the number of entries of the db as a starter
     * @param averageKeySize the average key size in bytes
     * @param filePath       the path to the file to create
     * @param valueClass     the class of the value (best to implement Value
     *                       interface
     *                       for complex structures)
     * @return ChronicleMap or null, if null do not run close()
     */
    public <V> SharedChronicleMap<String, V> open(final String name, final long entries,
            final int averageKeySize, final V averageValue, final String filePath, final double maxBloatFactor) {
        final SharedChronicleMap<String, V> entry = mapCache.compute(filePath, (k, existingEntry) -> {
            if (existingEntry != null) {
                return existingEntry.retain();
            }

            // Create a new entry
            try {
                final File file = new File(filePath);
                final Class<V> valueClass = (Class<V>) averageValue.getClass();
                final ChronicleMapBuilder<String, V> builder = ChronicleMapBuilder.of(String.class, valueClass)
                        .maxBloatFactor(maxBloatFactor);
                if (!file.exists()) {
                    builder.name(name).entries(entries).averageKeySize(averageKeySize).averageValue(averageValue);
                }
                final ChronicleMap<String, V> map = builder.createPersistedTo(file);
                return new SharedChronicleMap<>(map, filePath, name, entries, averageKeySize, averageValue,
                        maxBloatFactor);
            } catch (final InterProcessDeadLockException e) {
                Logger.warn("InterProcessDeadLockException detected for [{}]. Attempting recovery...", filePath);
                return recoverFromDeadlock(name, entries, averageKeySize, averageValue, filePath, maxBloatFactor);
            } catch (final IOException e) {
                Logger.error("Failed to open ChronicleMap at [{}]", filePath);
                throw new UncheckedIOException(e);
            } catch (final RuntimeException e) {
                // Check if wrapped exception is InterProcessDeadLockException
                if (hasDeadlockCause(e)) {
                    Logger.warn("InterProcessDeadLockException detected for [{}]. Attempting recovery...", filePath);
                    return recoverFromDeadlock(name, entries, averageKeySize, averageValue, filePath, maxBloatFactor);
                }
                throw e;
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
     * Soft-evict: removes the cache entry without closing the underlying map.
     * <p>
     * Concurrent holders of {@link SharedChronicleMap} references continue to
     * operate against the existing map. When the last holder calls
     * {@link SharedChronicleMap#close()} the map closes naturally via reference
     * counting. Subsequent {@link #open} calls on this path miss the cache and
     * load the file fresh from disk.
     * </p>
     * <p>
     * Use when the file at this path is about to be replaced (e.g. via an atomic
     * file rename during vacuum) and new openers must see the new file. A
     * destructive {@link #close(String)} would break any concurrent reader
     * mid-op.
     * </p>
     */
    public void evict(final String filePath) {
        mapCache.remove(filePath);
        Logger.debug("Evicted ChronicleMap cache for [{}]", filePath);
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
     * Checks if the exception cause chain contains an
     * InterProcessDeadLockException.
     */
    private boolean hasDeadlockCause(final Throwable e) {
        Throwable cause = e;
        while (cause != null) {
            if (cause instanceof InterProcessDeadLockException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    /**
     * Backs up a corrupted ChronicleMap file before recovery.
     */
    private void backupCorruptedFile(final String filePath) throws IOException {
        final Path path = Path.of(filePath);
        final Path backupDir = path.getParent().getParent().resolve("backup");
        Files.createDirectories(backupDir);
        final Path backupPath = backupDir.resolve("corrupted-" + path.getFileName());
        Files.copy(path, backupPath, StandardCopyOption.REPLACE_EXISTING);
        Logger.info("Created backup at [{}]", backupPath);
    }

    /**
     * Recovers from a deadlock by backing up and restoring the ChronicleMap file.
     */
    private <V> SharedChronicleMap<String, V> recoverFromDeadlock(final String name, final long entries,
            final int averageKeySize, final V averageValue, final String filePath, final double maxBloatFactor) {
        try {
            backupCorruptedFile(filePath);
            final ChronicleMap<String, V> map = recoverDb(name, entries, averageKeySize, averageValue, filePath,
                    maxBloatFactor);
            Logger.info("Successfully recovered ChronicleMap at [{}]", filePath);
            return new SharedChronicleMap<>(map, filePath, name, entries, averageKeySize, averageValue, maxBloatFactor);
        } catch (final IOException ex) {
            Logger.error("Failed to recover ChronicleMap at [{}]", filePath);
            throw new UncheckedIOException(ex);
        }
    }

    /**
     * Run this on app startup to check and fix if there were any abnormal
     * terminations
     *
     * @param filePath       the path to the file with the data
     * @param averageKeySize the average key size in bytes
     * @param valueClass     the class of the value (best to implement Value
     *                       interface
     *                       for complex structures)
     */
    public <V> ChronicleMap<String, V> recoverDb(final String name, final long entries,
            final int averageKeySize, final V averageValue, final String filePath, final double maxBloatFactor)
            throws IOException {
        Logger.info("Restoring ChronicleMap {} at: {}", name, filePath);
        final File file = new File(filePath);
        final Class<V> valueClass = (Class<V>) averageValue.getClass();

        return ChronicleMap.of(String.class, valueClass).name(name).entries(entries).averageKeySize(averageKeySize)
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
