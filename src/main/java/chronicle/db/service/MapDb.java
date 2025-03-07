package chronicle.db.service;

import java.util.concurrent.ConcurrentHashMap;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.tinylog.Logger;

public final class MapDb {
    private MapDb() {
    }

    public static final MapDb MAP_DB = new MapDb();
    private final ConcurrentHashMap<String, Integer> openMaps = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, HTreeMap<?, ?>> mapCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, DB> dbCache = new ConcurrentHashMap<>(); // Track DB instances

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
                        .closeOnJvmShutdown() // Ensure shutdown hook
                        .fileMmapEnableIfSupported()
                        .fileMmapPreclearDisable()
                        .cleanerHackEnable();
                if (readOnly) {
                    dbMaker.readOnly();
                }
                final DB db = dbMaker.make();
                dbCache.put(filePath, db); // Cache the DB instance
                final var map = (HTreeMap<K, V>) db.hashMap("map").createOrOpen();
                openMaps.put(filePath, 1); // Initialize count to 1
                return map;
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
     * Closes the MapDB instance for the given filePath when no longer in use.
     */
    public void close(final String filePath) {
        final Integer count = openMaps.compute(filePath, (k, v) -> v == null || v <= 1 ? null : v - 1);
        if (count == null) { // No more references
            final HTreeMap<?, ?> map = mapCache.remove(filePath);
            final DB db = dbCache.remove(filePath);
            if (db != null && !db.isClosed()) {
                db.commit(); // Ensure changes are saved
                db.close(); // Clean shutdown
            }
            if (map != null) {
                map.close();
            }
        }
    }

    /**
     * Returns the number of open references to the map at filePath.
     */
    public int getOpenCount(final String filePath) {
        return openMaps.getOrDefault(filePath, 0);
    }
}