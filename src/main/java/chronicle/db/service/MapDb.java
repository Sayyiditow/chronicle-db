package chronicle.db.service;

import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.tinylog.Logger;

@SuppressWarnings("unchecked")
public final class MapDb {
    public static final MapDb MAP_DB = new MapDb();

    /**
     * Use for reads and writes
     * User is in charge of calling close() to prevent map corruption.
     */
    public <K, V> HTreeMap<K, V> getDb(final String filePath) {
        Logger.info("Opening MapDB at: {}", filePath);
        final var db = DBMaker
                .fileDB(filePath)
                .fileMmapEnableIfSupported() // Only enable mmap on supported platforms
                .fileMmapPreclearDisable() // Make mmap file faster
                .cleanerHackEnable()
                .closeOnJvmShutdown()
                .make();
        return (HTreeMap<K, V>) db.hashMap("map").createOrOpen();
    }
}