package chronicle.db.service;

import org.mapdb.DB;
import org.mapdb.DBMaker;

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
}
