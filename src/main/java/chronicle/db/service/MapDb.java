package chronicle.db.service;

import java.util.concurrent.ConcurrentHashMap;

import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.tinylog.Logger;

public final class MapDb {
    private MapDb() {
    }

    public static final MapDb MAP_DB = new MapDb();
    private static final ConcurrentHashMap<String, Integer> openMaps = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, HTreeMap<?, ?>> mapCache = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Object> closeLocks = new ConcurrentHashMap<>();

    /**
     * Opens a MapDB for reads and writes (or reads only if readOnly is true).
     * Returns an HTreeMap—call close(filePath) to release resources and unlock.
     * Uses mmap if supported for maximum performance.
     */
    @SuppressWarnings("unchecked")
    public <K, V> HTreeMap<K, V> getDb(final String filePath) {
        // Track that we are opening this file path
        openMaps.compute(filePath, (k, v) -> v == null ? 1 : v + 1);

        // Get or create the map
        return (HTreeMap<K, V>) mapCache.computeIfAbsent(filePath, k -> {
            try {
                final var db = DBMaker.fileDB(filePath)
                        .closeOnJvmShutdown()
                        .fileLockDisable()
                        .fileMmapEnableIfSupported()
                        .fileMmapPreclearDisable()
                        .cleanerHackEnable().make();
                return db.hashMap("map").createOrOpen();
            } catch (final Exception e) {
                // Roll back openMaps increment on failure
                openMaps.compute(filePath, (k2, v2) -> v2 <= 1 ? null : v2 - 1);
                Logger.error(e);
                throw new RuntimeException("MapDB initialization failed for " + filePath, e);
            }
        });
    }

    /**
     * Closes the MapDB instance for the given filePath when no longer in use.
     */
    public void close(final String filePath) {
        openMaps.compute(filePath, (k, v) -> {
            if (v == null || v <= 1) {
                final var lock = closeLocks.computeIfAbsent(filePath, p -> new Object());
                // Ensure no other thread is opening the DB at the same time
                synchronized (lock) {
                    if (openMaps.getOrDefault(filePath, 0) <= 1) {
                        final var map = mapCache.remove(filePath);
                        if (map != null && !map.isClosed()) {
                            map.close();
                        }
                        return null;
                    }
                }
            }
            return v - 1;
        });
    }
}