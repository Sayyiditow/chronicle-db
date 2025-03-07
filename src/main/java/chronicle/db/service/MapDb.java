package chronicle.db.service;

import java.util.concurrent.ConcurrentHashMap;

import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.tinylog.Logger;

public final class MapDb {
    private MapDb() {
    }

    public static final MapDb MAP_DB = new MapDb();
    private final ConcurrentHashMap<String, Integer> openMaps = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, HTreeMap<?, ?>> mapCache = new ConcurrentHashMap<>();

    /**
     * Opens a MapDB for reads and writes (or reads only if readOnly is true).
     * Returns an HTreeMap—call close(filePath) to release resources and unlock.
     * Uses mmap if supported for maximum performance.
     */
    @SuppressWarnings("unchecked")
    public <K, V> HTreeMap<K, V> getDb(final String filePath, final boolean readOnly) {
        return (HTreeMap<K, V>) mapCache.computeIfAbsent(filePath, k -> {
            try {
                final var dbMaker = DBMaker.fileDB(filePath)
                        .closeOnJvmShutdown() // MapDB handles shutdown
                        .fileMmapEnableIfSupported()
                        .fileMmapPreclearDisable()
                        .cleanerHackEnable();
                if (readOnly) {
                    dbMaker.readOnly();
                }
                return (HTreeMap<K, V>) dbMaker.make().hashMap("map").createOrOpen();
            } catch (final Exception e) {
                Logger.error("Failed to open MapDB for {}: {}", filePath, e.getMessage());
                throw new RuntimeException("MapDB initialization failed", e);
            }
        });
    }

    /**
     * Convenience method for default read-write access.
     */
    public <K, V> HTreeMap<K, V> getDb(final String filePath) {
        return getDb(filePath, false);
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
                    try {
                        map.close();
                    } catch (final Exception e) {
                        Logger.error("Error closing MapDB for {}: {}", filePath, e.getMessage());
                    }
                }
                return null;
            }
            return refCount - 1;
        });
    }
}