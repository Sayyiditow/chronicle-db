package chronicle.db.service;

import static chronicle.db.service.ChronicleDb.CHRONICLE_DB;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import chronicle.db.entity.Fetch;

/**
 * This class is useful to run crud functionality and return
 */
public final class ChronicleQueryService {
    private ChronicleQueryService() {
    }

    public static final ChronicleQueryService CRUD_DB_SERVICE = new ChronicleQueryService();

    private ConcurrentMap<?, ?> searchSingleDb(final Fetch fetch, final boolean isIndexed, final int limit)
            throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException, NoSuchFieldException,
            SecurityException, InstantiationException, InvocationTargetException, IOException {
        final var dao = CHRONICLE_DB.getSingleChronicleDao(fetch.objectName(), fetch.dataPath());
        ConcurrentMap<?, ?> map = new ConcurrentHashMap<>();

        if (isIndexed) {
            if (limit != 0)
                map = dao.indexedSearch(fetch.search(), limit);
            else
                map = dao.indexedSearch(fetch.search());
        } else {
            if (limit != 0)
                map = dao.search(fetch.search(), limit);
            else
                map = dao.search(fetch.search());
        }

        return map;
    }

    public ConcurrentMap<?, ?> searchSingleChronicleDb(final Fetch fetch)
            throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException, NoSuchFieldException,
            SecurityException, InstantiationException, InvocationTargetException, IOException {
        return searchSingleDb(fetch, false, 0);
    }

    public ConcurrentMap<?, ?> searchSingleChronicleDb(final Fetch fetch, final boolean isIndexed)
            throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException, NoSuchFieldException,
            SecurityException, InstantiationException, InvocationTargetException, IOException {
        return searchSingleDb(fetch, isIndexed, 0);
    }

    public ConcurrentMap<?, ?> searchSingleChronicleDb(final Fetch fetch, final boolean isIndexed, final int limit)
            throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException, NoSuchFieldException,
            SecurityException, InstantiationException, InvocationTargetException, IOException {
        return searchSingleDb(fetch, isIndexed, limit);
    }
}