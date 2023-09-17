package chronicle.db.service;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

@SuppressWarnings("unchecked")
public final class MapDb {
    private MapDb() {
    }

    public static final MapDb MAP_DB = new MapDb();

    public DB db(final String filePath) {
        return DBMaker
                .fileDB(filePath)
                .fileMmapEnable()
                .fileLockDisable()
                .make();
    }

    public <K, V> HTreeMap<K, V> getMapDb(final DB db) {
        return (HTreeMap<K, V>) db.hashMap("map").createOrOpen();
    }
}
