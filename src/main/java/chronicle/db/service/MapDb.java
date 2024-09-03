package chronicle.db.service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.tinylog.Logger;

@SuppressWarnings("unchecked")
public final class MapDb {
    private static final ConcurrentMap<String, DB> INSTANCES = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, Integer> REF_COUNTS = new ConcurrentHashMap<>();

    private MapDb() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Logger.info("JVM shutting down. Closing all MapDb instances.");
            closeAllDbs();
        }));
    }

    public static final MapDb MAP_DB = new MapDb();

    /**
     * User is in charge of calling close() to prevent map corruption.
     */
    public synchronized <K, V> HTreeMap<K, V> getDb(final String filePath) {
        Logger.info("Opening index file at: {}", filePath);
        var db = INSTANCES.get(filePath);
        if (db == null) {
            db = DBMaker
                    .fileDB(filePath)
                    .fileMmapEnable() // Always enable mmap
                    .fileMmapEnableIfSupported() // Only enable mmap on supported platforms
                    .fileMmapPreclearDisable() // Make mmap file faster
                    .cleanerHackEnable()
                    .closeOnJvmShutdown()
                    .fileLockDisable()
                    .make();
            INSTANCES.put(filePath, db);
            REF_COUNTS.put(filePath, 0);
        }
        REF_COUNTS.put(filePath, REF_COUNTS.get(filePath) + 1);
        return (HTreeMap<K, V>) db.hashMap("map").createOrOpen();
    }

    public synchronized void closeDb(final String filePath) {
        Logger.info("Closing index file at: {}", filePath);
        var refCount = REF_COUNTS.get(filePath);
        if (refCount != null) {
            refCount--;
            REF_COUNTS.put(filePath, refCount);

            if (refCount == 0) {
                final var db = INSTANCES.get(filePath);
                if (db != null) {
                    db.close();
                    REF_COUNTS.remove(filePath);
                    INSTANCES.remove(filePath);
                }
            }
        }
    }

    private synchronized void closeAllDbs() {
        for (final String filePath : INSTANCES.keySet()) {
            closeDb(filePath);
        }
        INSTANCES.clear();
        REF_COUNTS.clear();
    }
}