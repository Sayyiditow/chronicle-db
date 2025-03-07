package chronicle.db.service;

import java.util.concurrent.ConcurrentHashMap;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

public final class MapDb {
    private MapDb() {
    }

    public static final MapDb MAP_DB = new MapDb();
    private final ConcurrentHashMap<String, Integer> openMaps = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, HTreeMap<?, ?>> mapCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, DB> dbCache = new ConcurrentHashMap<>();

    /**
     * Opens a MapDB for reads and writes (or reads only if readOnly is true).
     * Returns an HTreeMap—call close(filePath) to release resources and unlock.
     * Uses mmap if supported for maximum performance.
     */
    @SuppressWarnings("unchecked")
    public <K, V> HTreeMap<K, V> getDb(final String filePath) {
        // Increment open count for this filePath every time getDb is called
        openMaps.compute(filePath, (k, v) -> v == null ? 1 : v + 1);

        // Get or create the map
        final HTreeMap<?, ?> map = mapCache.computeIfAbsent(filePath, k -> {
            try {
                final var dbMaker = DBMaker.fileDB(filePath)
                        .closeOnJvmShutdown()
                        .fileMmapEnableIfSupported()
                        .fileMmapPreclearDisable()
                        .cleanerHackEnable();
                final var db = dbMaker.make();
                dbCache.put(filePath, db); // Store DB instance
                return db.hashMap("map").createOrOpen();
            } catch (final Exception e) {
                // Roll back openMaps increment on failure
                openMaps.compute(filePath, (k2, v2) -> v2 <= 1 ? null : v2 - 1);
                throw new RuntimeException("MapDB initialization failed for " + filePath, e);
            }
        });

        return (HTreeMap<K, V>) map;
    }

    /**
     * Closes the MapDB instance for the given filePath when no longer in use.
     */
    public void close(final String filePath) {
        openMaps.compute(filePath, (k, v) -> {
            if (v == null || v <= 1) {
                final var map = mapCache.remove(filePath);
                final var db = dbCache.remove(filePath);
                if (db != null && !db.isClosed()) {
                    db.close();
                }
                if (map != null) {
                    map.close();
                }
                return null;
            }
            return v - 1;
        });
    }
}