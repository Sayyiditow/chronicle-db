package chronicle.db.service;

import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.tinylog.Logger;

@SuppressWarnings("unchecked")
public final class MapDb {
    public static final MapDb MAP_DB = new MapDb();

    /**
     * User is in charge of calling close() to prevent map corruption.
     */
    public <K, V> HTreeMap<K, V> getDb(final String filePath) {
        Logger.info("Opening MapDB at: {}", filePath);
        final var db = DBMaker
                .fileDB(filePath)
                .fileMmapEnable() // Always enable mmap
                .fileMmapEnableIfSupported() // Only enable mmap on supported platforms
                .fileMmapPreclearDisable() // Make mmap file faster
                .cleanerHackEnable()
                .closeOnJvmShutdown()
                .fileLockDisable()
                .make();
        return (HTreeMap<K, V>) db.hashMap("map").createOrOpen();
    }
}