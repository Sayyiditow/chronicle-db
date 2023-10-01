package chronicle.db.dao;

import static chronicle.db.dao.ChronicleUtils.CHRONICLE_UTILS;
import static chronicle.db.service.MapDb.MAP_DB;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.mapdb.HTreeMap;
import org.tinylog.Logger;

import chronicle.db.entity.Search;
import chronicle.db.service.ChronicleDb;
import net.openhft.chronicle.map.ChronicleMap;

/**
 *
 * @param <K> Type of the unique identifier
 * @param <V> Type of the single element
 */
public interface SingleChronicleDao<K, V> extends BaseDao<K, V> {
    /**
     * Get the db object, you must close the object manually
     * 
     * @return ChronicleMap<K, V>
     * @throws IOException
     */
    default ChronicleMap<K, V> db() throws IOException {
        return ChronicleDb.CHRONICLE_DB.createOrGet(name(), entries(), averageKey(), averageValue(),
                dataPath() + "/data/data", bloatFactor());
    }

    /**
     * Fetches all records in the db
     * 
     * @return ConcurrentMap<K, V>
     * @throws IOException
     */
    default ConcurrentMap<K, V> fetch() throws IOException {
        final ChronicleMap<K, V> db = db();
        final ConcurrentMap<K, V> map = new ConcurrentHashMap<>(db);
        db.close();
        return map;
    }

    /**
     * Get a value using key
     * 
     * @param key the key to search in the db
     * @return V value
     * @throws IOException
     */
    default V get(final K key) throws IOException {
        final var db = db();
        CHRONICLE_UTILS.getLog(name(), key);
        final var value = db.getUsing(key, using());
        db.close();
        return value;
    }

    /**
     * Get all the values for a list of keys
     * 
     * @param keys list of keys
     * @return ConcurrentMap<K, V> values
     * @throws IOException
     */
    default ConcurrentMap<K, V> get(final Set<K> keys) throws IOException {
        final var map = new ConcurrentHashMap<K, V>();
        final var db = db();
        for (final K key : keys) {
            CHRONICLE_UTILS.getLog(name(), key);
            final var value = db.getUsing(key, using());
            if (Objects.nonNull(value))
                map.put(key, value);
        }
        db.close();
        return map;
    }

    /**
     * Remove a value using key
     * 
     * @param key the key to remove
     * @return true if updated else false
     * @throws IOException
     */
    default boolean delete(final K key) throws IOException {
        final var db = db();
        CHRONICLE_UTILS.deleteLog(name(), key);
        final var value = db.getUsing(key, using());
        final var updated = Objects.nonNull(db.remove(key));

        if (updated) {
            CHRONICLE_UTILS.successDeleteLog(name(), key);
            if (containsIndexes()) {
                CHRONICLE_UTILS.removeFromIndex("data", name(), dataPath(), indexFileNames(), Map.of(key, value));
            }
        }
        db.close();

        return updated;
    }

    /**
     * Remove a value using a list of keys
     * 
     * @param keys the keys to remove
     * @return true if updated else false
     * @throws IOException
     */
    default boolean delete(final Set<K> keys) throws IOException {
        final var db = db();
        CHRONICLE_UTILS.deleteAllLog(name());
        final Map<K, V> updatedMap = new HashMap<>();

        if (containsIndexes()) {
            for (final K key : keys) {
                final var value = db.getUsing(key, using());
                if (Objects.nonNull(value)) {
                    updatedMap.put(key, value);
                }
            }
        }

        final var updated = db.keySet().removeAll(keys);

        if (updated) {
            CHRONICLE_UTILS.successDeleteLog(name(), keys);
        }

        if (containsIndexes()) {
            CHRONICLE_UTILS.removeFromIndex("data", name(), dataPath(), indexFileNames(), updatedMap);
        }
        db.close();

        return updated;
    }

    private ChronicleMap<K, V> createNewDb(ChronicleMap<K, V> db) throws IOException {
        if (db.size() != 0 && db.size() % entries() == 0) {
            final var currentValues = new ConcurrentHashMap<>(db);
            db.close();
            Logger.info("Creating a bigger file, max limit of records reached on {}", name());
            final var dataFile = dataPath() + "/data/data";
            CHRONICLE_UTILS.deleteFileIfExists(dataFile);
            db = ChronicleDb.CHRONICLE_DB.createOrGet(name(),
                    entries() * ((currentValues.size() / entries()) + 1) + entries(),
                    averageKey(), averageValue(), dataPath() + "/data/data.tmp", bloatFactor());
            db.putAll(currentValues);
            Files.move(Paths.get(dataPath() + "/data/data.tmp"), Paths.get(dataFile),
                    StandardCopyOption.REPLACE_EXISTING);
        }
        return db;
    }

    /**
     * Add a value
     * 
     * @param key   the key
     * @param value the value
     * @return true if updated else false
     * @throws IOException
     */
    default boolean put(final K key, final V value) throws IOException {
        // create a bigger file if records in db are equal to multiple of entries()
        final var db = createNewDb(db());
        Logger.info("Inserting into {} using key {}.", name(), key);
        final var updated = Objects.nonNull(db.put(key, value));

        if (updated && containsIndexes()) {
            CHRONICLE_UTILS.addToIndex("data", name(), dataPath(), indexFileNames(), Map.of(key, value));
        }

        db.close();
        return updated;
    }

    /**
     * Add multiple keys and values into the db
     * 
     * @param map the map to add
     * @throws IOException
     */
    default void put(final Map<K, V> map) throws IOException {
        if (map.size() > entries()) {
            Logger.error("Insert size bigger than entry size.");
            return;
        }

        Logger.info("Inserting multiple values into {}.", name());
        var db = db();

        for (final var entry : map.entrySet()) {
            db = createNewDb(db);
            db.put(entry.getKey(), entry.getValue());
        }
        if (containsIndexes()) {
            CHRONICLE_UTILS.addToIndex("data", name(), dataPath(), indexFileNames(), map);
        }

        db.close();
    }

    /**
     * Current size of the data
     * 
     * @return int size
     * @throws IOException
     */
    default int size() throws IOException {
        final var db = db();
        final var size = db.size();
        db.close();
        return size;
    }

    /**
     * Refer to BaseDao.super.search
     */
    default ConcurrentMap<K, V> search(final Search search) throws IOException {
        final var db = db();
        final var result = BaseDao.super.search(db, search);
        db.close();
        return result;
    }

    /**
     * Refer to BaseDao.super.search
     */
    default ConcurrentMap<K, V> search(final Search search, final int limit) throws IOException {
        final var db = db();
        final var result = BaseDao.super.search(db, search, limit);
        db.close();
        return result;
    }

    /**
     * Only runs to initialize an index on the field first time
     * 
     * @param field the field of the V value object
     * @throws IOException
     * 
     */
    default void initIndex(final String[] fields) throws IOException {
        final var db = db();
        for (final var field : fields) {
            final String path = getIndexPath(field);
            CHRONICLE_UTILS.deleteFileIfExists(path);
            final HTreeMap<String, Map<Object, List<K>>> indexDb = MAP_DB.getDb(path);
            CHRONICLE_UTILS.index(db, name(), field, indexDb, "data");
            indexDb.close();
        }
        db.close();
    }

    /**
     * Refer to @BaseDao.super.indexedSearch
     */
    default ConcurrentMap<K, V> indexedSearch(final Search search) throws IOException {
        final HTreeMap<String, Map<Object, List<K>>> indexDb = MAP_DB.getDb(getIndexPath(search.field()));
        final var result = BaseDao.super.indexedSearch(search, db(), indexDb.get("data"));
        indexDb.close();
        return result;
    }

    /**
     * Refer to @BaseDao.super.indexedSearch
     */
    default ConcurrentMap<K, V> indexedSearch(final Search search, final int limit) throws IOException {
        final HTreeMap<String, Map<Object, List<K>>> indexDb = MAP_DB.getDb(getIndexPath(search.field()));
        final var result = BaseDao.super.indexedSearch(search, db(), indexDb.get("data"), limit);
        indexDb.close();
        return result;
    }
}
