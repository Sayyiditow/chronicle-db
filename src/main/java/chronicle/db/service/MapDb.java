package chronicle.db.service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

public final class MapDb {
    private MapDb() {
    }

    public static final MapDb MAP_DB = new MapDb();
    private final ConcurrentHashMap<String, ReentrantReadWriteLock> fileLocks = new ConcurrentHashMap<>(); // Lock per
                                                                                                           // file
    private final ConcurrentHashMap<String, Integer> openMaps = new ConcurrentHashMap<>(); // Reference count per file
    private final ConcurrentHashMap<String, HTreeMap<?, ?>> mapCache = new ConcurrentHashMap<>(); // Cached maps

    /**
     * Opens a MapDB for reads and writes (or reads only if readOnly is true).
     * Returns an HTreeMap—call close(filePath) to release resources and unlock.
     * Uses mmap if supported for maximum performance.
     */
    @SuppressWarnings("unchecked")
    public <K, V> HTreeMap<K, V> getDb(final String filePath, final boolean readOnly) {
        final ReentrantReadWriteLock lock = fileLocks.computeIfAbsent(filePath, k -> new ReentrantReadWriteLock());
        final Lock usedLock = readOnly ? lock.readLock() : lock.writeLock();

        usedLock.lock();
        try {
            final var map = (HTreeMap<K, V>) mapCache.computeIfAbsent(filePath, k -> {
                final var dbMaker = DBMaker.fileDB(filePath)
                        .closeOnJvmShutdown()
                        .fileMmapEnableIfSupported()
                        .fileMmapPreclearDisable()
                        .cleanerHackEnable()
                        .fileLockDisable()
                        .make();
                return (HTreeMap<K, V>) dbMaker.hashMap("map").createOrOpen();
            });

            // Increment reference count for the map
            openMaps.compute(filePath, (k, existing) -> existing == null ? 1 : existing + 1);
            return map; // Return cached or newly created map
        } catch (final Exception e) {
            usedLock.unlock();
            throw e; // Re-throw to let caller handle
        }
    }

    /**
     * Convenience method for default read-write access.
     */
    public <K, V> HTreeMap<K, V> getDb(final String filePath) {
        return getDb(filePath, false); // Default to read-write
    }

    /**
     * Closes the MapDB and releases the lock for the given filePath.
     * Throws IllegalMonitorStateException if the current thread doesn’t own the
     * lock.
     */
    public void close(final String filePath) {
        openMaps.computeIfPresent(filePath, (k, refCount) -> {
            if (refCount <= 1) {
                final var map = mapCache.remove(filePath);
                if (map != null) {
                    map.close(); // Close map silently—no try-catch for speed
                    final var lock = fileLocks.get(filePath);
                    if (lock != null) {
                        if (lock.getWriteHoldCount() > 0) {
                            lock.writeLock().unlock();
                        } else if (lock.getReadHoldCount() > 0) {
                            lock.readLock().unlock();
                        }
                        fileLocks.remove(filePath); // Clean up lock after unlocking
                    }
                }
                return null; // Remove entry
            }
            return refCount - 1; // Decrement refCount
        });
    }
}