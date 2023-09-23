package chronicle.db.service;

import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

@SuppressWarnings("unchecked")
public final class MapDb {
    private MapDb() {
    }

    public static final MapDb MAP_DB = new MapDb();

    /**
     * User is in charge of calling close() to prevent map corruption.
     */
    public <K, V> HTreeMap<K, V> getDb(final String filePath) {
        final var db = DBMaker
                .fileDB(filePath)
                .fileMmapEnable()
                .fileLockDisable()
                .closeOnJvmShutdown()
                .make();
        return (HTreeMap<K, V>) db.hashMap("map").createOrOpen();
    }
}
