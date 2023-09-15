package chronicle.db.service;

import java.util.concurrent.ConcurrentMap;

import org.mapdb.DB;
import org.mapdb.DBMaker;

@SuppressWarnings("unchecked")
public final class MapDb {
    private MapDb() {
    }

    public static final MapDb MAP_DB = new MapDb();

    public DB db(final String filePath) {
        return DBMaker
                .fileDB(filePath)
                .fileMmapEnable()
                .make();
    }

    public <K, V> ConcurrentMap<K, V> getMapDb(final DB db) {
        return (ConcurrentMap<K, V>) db.hashMap("map").createOrOpen();
    }
}
