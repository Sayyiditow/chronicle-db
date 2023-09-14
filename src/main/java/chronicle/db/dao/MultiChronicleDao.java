package chronicle.db.dao;

import static chronicle.db.dao.ChronicleUtils.CHRONICLE_UTILS;
import static chronicle.db.service.ChronicleDb.CHRONICLE_DB;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.mapdb.DB;
import org.tinylog.Logger;

import chronicle.db.entity.Search;
import chronicle.db.service.HandleConsumer;
import chronicle.db.service.MapDb;
import net.openhft.chronicle.map.ChronicleMap;

@SuppressWarnings({ "unchecked" })
/**
 *
 * @param <K> Type of the unique identifier
 * @param <V> Type of the single element
 */
public interface MultiChronicleDao<K, V> extends BaseDao<K, V> {
    private List<String> getFiles() {
        return ChronicleUtils.getFileList(dataPath() + "/data");
    }

    private boolean multiThreaded(final List<String> files) {
        return files.size() > 2;
    }

    private String getInsertFile(final int records) throws IOException {
        final var files = getFiles();

        if (files.size() == 0)
            return "1";

        for (final String file : files) {
            if (db(file).size() + records <= entries()) {
                return file;
            }
        }
        return String.valueOf(files.size() + 1);
    }

    /**
     * Get the db object
     * 
     * @param path the file name
     * @throws IOException
     */
    private ChronicleMap<K, V> db(final String path) throws IOException {
        return CHRONICLE_DB.createOrGet(name(), entries(), averageKey(), averageValue(), dataPath() + "/data/" + path);
    }

    /**
     * Fetches all records in the db
     * 
     * @return ConcurrentMap<String, ConcurrentMap<K, V>> map of maps with each file
     *         as the key
     * @throws IOException
     */
    default ConcurrentMap<String, ConcurrentMap<K, V>> fetch() throws IOException {
        final var map = new ConcurrentHashMap<String, ConcurrentMap<K, V>>();
        final var files = getFiles();

        if (multiThreaded(files)) {
            files.parallelStream().forEach(HandleConsumer.handleConsumerBuilder(file -> {
                final var db = db(file);
                map.put(file, new ConcurrentHashMap<>(db));
                db.close();
            }));
            return map;
        }

        for (final String file : files) {
            final var db = db(file);
            map.put(file, new ConcurrentHashMap<>(db));
            db.close();
        }

        return map;
    }

    /**
     * Get a value using key and file name if you know where the value is
     * 
     * @param key      the key to search in the db
     * @param file     the file
     * @param valueMap map to set the value
     * @throws IOException
     */
    private void get(final K key, final String file, final ConcurrentMap<String, V> valueMap) throws IOException {
        final var db = db(file);

        if (db.size() != 0) {
            CHRONICLE_UTILS.getLog(name() + " at file " + file, key);
            final var value = db.getUsing(key, using());

            if (Objects.nonNull(value)) {
                valueMap.put(file, value);
            }
        }
        db.close();
    }

    /**
     * Get a value using key
     * 
     * @param key the key to search in the db
     * @return map of file as key and value.
     * @throws IOException
     */
    default ConcurrentMap<String, V> get(final K key) throws IOException {
        final var valueMap = new ConcurrentHashMap<String, V>();
        final var files = getFiles();

        if (multiThreaded(files)) {
            files.parallelStream().allMatch(file -> {
                try {
                    get(key, file, valueMap);
                } catch (final IOException e) {
                    CHRONICLE_UTILS.dbFetchError(name(), file);
                }
                return Objects.nonNull(valueMap.get(key));
            });
            return valueMap;
        }

        for (final String file : files) {
            get(key, file, valueMap);
            if (Objects.nonNull(valueMap.get(key))) {
                break;
            }
        }
        return valueMap;
    }

    /**
     * Get a value using key and file name if you know where the value is
     * 
     * @param key  the key to search in the db
     * @param file the file
     * @return ConcurrentMap<String, V> of the value and file key
     * @throws IOException
     */
    default ConcurrentMap<String, V> get(final K key, final String file) throws IOException {
        final var valueMap = new ConcurrentHashMap<String, V>();
        get(key, file, valueMap);
        return valueMap;
    }

    /**
     * get records from a set of keys if the file is known
     * 
     * @param keys set of keys
     * @param file the file name
     * @param map  the map to file the results in, whose key is the file name
     */
    default void get(final Set<K> keys, final String file, final ConcurrentMap<String, ConcurrentMap<K, V>> map)
            throws IOException {
        final var db = db(file);

        if (db.size() != 0) {
            for (final K key : keys) {
                CHRONICLE_UTILS.getLog(name() + " at file " + file, key);
                final var value = db.getUsing(key, using());
                if (Objects.nonNull(value)) {
                    ConcurrentMap<K, V> valueMap = map.get(file);
                    if (valueMap == null)
                        valueMap = new ConcurrentHashMap<>();
                    valueMap.put(key, value);
                    map.put(file, valueMap);
                }
            }
        }
        db.close();
    }

    /**
     * Get all the values for a list of keys
     * 
     * @param keys list of keys
     * @return ConcurrentMap<String, ConcurrentMap<K, V>> map of maps with each file
     *         as the key
     * @throws IOException
     */
    default ConcurrentMap<String, ConcurrentMap<K, V>> get(final Set<K> keys) throws IOException {
        final var map = new ConcurrentHashMap<String, ConcurrentMap<K, V>>();
        final var files = getFiles();

        if (multiThreaded(files)) {
            files.parallelStream().allMatch(file -> {
                try {
                    get(keys, file, map);
                } catch (final IOException e) {
                    CHRONICLE_UTILS.dbFetchError(name(), file);
                }
                return map.entrySet().size() == keys.size();
            });
            return map;
        }

        for (final String file : files) {
            get(keys, file, map);
            if (map.entrySet().size() == keys.size())
                break;
        }

        return map;
    }

    /**
     * Remove a value using key and file name if you know the file
     * 
     * @param key  the key to remove
     * @param file the file
     * @return true if updated else false
     * @throws IOException
     */
    default boolean delete(final K key, final String file) throws IOException {
        var updated = false;
        final var db = db(file);

        if (db.size() != 0) {
            CHRONICLE_UTILS.deleteLog(name() + " at file " + file, key);
            final var value = db.getUsing(key, using());
            updated = Objects.nonNull(db.remove(key));

            if (updated) {
                CHRONICLE_UTILS.successDeleteLog(name(), key);
                if (containsIndexes()) {
                    CHRONICLE_UTILS.removeFromIndex(name(), dataPath(), indexFileNames(), key, value);
                }
            }
        }
        db.close();

        return updated;
    }

    /**
     * Remove a value using key
     * 
     * @param key the key to remove
     * @return true if updated else false
     * @throws IOException
     */
    default boolean delete(final K key) throws IOException {
        final var updated = new AtomicBoolean(false);
        final var files = getFiles();

        if (multiThreaded(files)) {
            files.parallelStream().allMatch(file -> {
                try {
                    updated.set(delete(key, file));
                } catch (final IOException e) {
                    CHRONICLE_UTILS.dbFetchError(name(), file);
                }

                return updated.get() == true;
            });
        } else {
            for (final String file : files) {
                if (delete(key, file)) {
                    updated.set(true);
                    break;
                }
            }
        }

        return updated.get();
    }

    /**
     * Delete multiple values using keys when the file is known
     * 
     * @param keys a set of keys
     * @param file the file name.
     * @return no of records updated
     */
    default int delete(final Set<K> keys, final String file)
            throws IOException {
        var deleted = 0;
        final var db = db(file);
        final int size = db.size();

        if (size != 0) {
            CHRONICLE_UTILS.deleteAllLog(name() + " at file " + file);
            for (final K key : keys) {
                final var value = db.getUsing(key, using());
                if (Objects.nonNull(value)) {
                    deleted += 1;
                    if (containsIndexes()) {
                        CHRONICLE_UTILS.removeFromIndex(name(), dataPath(), indexFileNames(), key, value);
                    }
                }
            }
            if (deleted != 0) {
                db.keySet().removeAll(keys);
            }
        }
        db.close();

        return deleted;
    }

    /**
     * Remove a value using a list of keys
     * 
     * @param keys the keys to remove
     * @return true if updated else false
     * @throws IOException
     */
    default int delete(final Set<K> keys) throws IOException {
        final var updated = new AtomicInteger(0);
        final var files = getFiles();

        if (multiThreaded(files)) {
            files.parallelStream().forEach(HandleConsumer.handleConsumerBuilder(file -> {
                updated.set(updated.get() + delete(keys, file));
            }));
        } else {
            for (final String file : files) {
                updated.set(updated.get() + delete(keys, file));
            }
        }

        if (updated.get() != 0) {
            CHRONICLE_UTILS.successDeleteLog(name(), keys);
        }

        return updated.get();
    }

    /**
     * insert/update a record if the file is known
     * 
     * @param key
     * @param value
     * @param file
     * @throws IOException
     * @returns true/false
     */
    default boolean put(final K key, final V value, final String file) throws IOException {
        Logger.info("Inserting into {} at file {} using key {}.", name(), file, key);

        final var db = db(file);
        final var updated = Objects.nonNull(db.put(key, value));
        db.close();

        if (updated && containsIndexes()) {
            CHRONICLE_UTILS.addToIndex(name(), dataPath(), indexFileNames(), key, value);
        }
        return updated;
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
        final var getMap = get(key);

        if (getMap.size() != 0) {
            final var file = getMap.keySet().iterator().next();
            return put(key, value, file);
        }

        return put(key, value, getInsertFile(1));
    }

    /**
     * Add multiple keys and values into the db if you know the file
     * 
     * @param map  the map to add
     * @param file the file name
     * @throws IOException
     */
    default void put(final Map<K, V> map, final String file) throws IOException {
        if (map.size() > entries()) {
            Logger.error("Insert size bigger than entry size.");
            return;
        }

        final var db = db(file);

        Logger.info("Inserting multiple values into {}.", name());
        db.putAll(map);
        db.close();
        if (containsIndexes()) {
            for (final var entry : map.entrySet()) {
                CHRONICLE_UTILS.addToIndex(name(), dataPath(), indexFileNames(), entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * Add multiple keys and values into the db
     * this method will update existing records and insert new ones
     * 
     * @param map            the map to add
     * @param updateExisting if we should update or just insert.
     * @throws IOException
     */
    default void put(final Map<K, V> map, final boolean updateExisting) throws IOException {
        // check for existing records to update them
        if (updateExisting) {
            final var existingRecords = get(map.keySet());
            final var keys = existingRecords.keySet();
            final var toRemove = new HashSet<>();

            if (keys.size() > 2) {
                keys.parallelStream().forEach(k -> {
                    for (final var entry : existingRecords.get(k).entrySet()) {
                        entry.setValue(map.get(k));
                        toRemove.add(k);
                    }
                });
            } else {
                for (final var k : keys) {
                    for (final var entry : existingRecords.get(k).entrySet()) {
                        entry.setValue(map.get(k));
                        toRemove.add(k);
                    }
                }
            }

            map.keySet().removeAll(toRemove);

            for (final var entry : existingRecords.entrySet()) {
                put(entry.getValue(), entry.getKey());
            }
        }

        put(map, getInsertFile(map.keySet().size()));
    }

    /**
     * Current size of the data
     * 
     * @return int size
     * @throws IOException
     */
    default int size() throws IOException {
        var size = 0;

        for (final String file : getFiles()) {
            final var db = db(file);
            size += db.size();
            db.close();
        }

        return size;
    }

    /**
     * Search the chronicle map based on values
     * 
     * @param search object search
     * @return a map of the fitting values
     * @throws IOException
     */
    default ConcurrentMap<K, V> search(final Search search) throws IOException {
        final var map = new ConcurrentHashMap<K, V>();
        final var files = getFiles();

        if (multiThreaded(files)) {
            files.parallelStream().forEach(HandleConsumer.handleConsumerBuilder(file -> {
                final var db = db(file);
                for (final var entry : db.entrySet()) {
                    CHRONICLE_UTILS.search(search, entry.getKey(), entry.getValue(), map);
                }
                db.close();
            }));

            return map;
        }

        for (final String file : files) {
            final var db = db(file);
            for (final var entry : db.entrySet()) {
                CHRONICLE_UTILS.search(search, entry.getKey(), entry.getValue(), map);
            }
            db.close();
        }

        return map;
    }

    /**
     * Search the chronicle map based on values
     * 
     * @param search object search
     * @return a map of the fitting values
     * @throws IOException
     */
    default ConcurrentMap<K, V> search(final Search search, final int limit)
            throws IOException {
        final var map = new ConcurrentHashMap<K, V>();
        final var files = getFiles();

        if (multiThreaded(files)) {
            files.parallelStream().allMatch(file -> {
                try (final var db = db(file)) {
                    for (final var entry : db.entrySet()) {
                        CHRONICLE_UTILS.search(search, entry.getKey(), entry.getValue(), map);
                        if (map.size() == limit)
                            break;
                    }
                    db.close();
                } catch (final IOException e) {
                    CHRONICLE_UTILS.dbFetchError(name(), file);
                }

                return map.size() == limit;
            });

            return map.entrySet().stream().limit(limit).collect(Collectors.toConcurrentMap(Map.Entry::getKey,
                    Map.Entry::getValue));
        } else {
            for (final String file : files) {
                final var db = db(file);
                for (final var entry : db.entrySet()) {
                    CHRONICLE_UTILS.search(search, entry.getKey(), entry.getValue(), map);
                    if (map.size() == limit)
                        break;
                }
                db.close();
                if (map.size() == limit)
                    break;
            }

            return map;
        }
    }

    /**
     * Only runs to initialize an index on the field first time
     * 
     * @param field the enum field of the V value object
     * @return boolean value if index is initialized
     * @throws IOException
     * 
     */
    default boolean initIndex(final String field) throws IOException {
        Files.delete(Paths.get(getIndexPath(field)));
        final var files = getFiles();
        var updated = false;
        final var indexDb = MapDb.MAP_DB.db(getIndexPath(field));
        final var index = (ConcurrentMap<String, Map<Object, List<K>>>) indexDb.hashMap("map").createOrOpen();

        if (multiThreaded(files)) {
            files.parallelStream().forEach(HandleConsumer.handleConsumerBuilder(f -> {
                final var db = db(f);
                CHRONICLE_UTILS.index(db, name(), field, index, f);
                db.close();
            }));
            updated = index.size() != 0;
            indexDb.close();
            return updated;
        }

        for (final String file : files) {
            final var db = db(file);
            CHRONICLE_UTILS.index(db, name(), field, index, file);
            db.close();
        }
        updated = index.size() != 0;
        indexDb.close();

        return updated;
    }

    private ConcurrentMap<String, Map<Object, List<K>>> fileAtIndex(
            final ConcurrentMap<String, Map<Object, List<K>>> index, final Object searchTerm) {
        final ConcurrentMap<String, Map<Object, List<K>>> recordsAtMap = new ConcurrentHashMap<>();

        for (final var entry : index.entrySet()) {
            final var value = entry.getValue().get(searchTerm);
            if (value != null) {
                recordsAtMap.put(entry.getKey(), entry.getValue());
            }
        }

        return recordsAtMap;
    }

    /**
     * Refer to @BaseDao.super.indexedSearch
     */
    default ConcurrentMap<K, V> indexedSearch(final Search search) throws IOException {
        final var map = new ConcurrentHashMap<K, V>();
        final DB indexDb = MapDb.MAP_DB.db(getIndexPath(search.field()));
        final var index = (ConcurrentMap<String, Map<Object, List<K>>>) indexDb.hashMap("map").createOrOpen();
        final var recordsAtMap = fileAtIndex(index, search.searchTerm());

        if (recordsAtMap.size() > 2) {
            recordsAtMap.entrySet().parallelStream().forEach(HandleConsumer.handleConsumerBuilder(entry -> {
                map.putAll(BaseDao.super.indexedSearch(search, db(entry.getKey()), entry.getValue()));
            }));
            indexDb.close();
            return map;
        }

        for (final var entry : recordsAtMap.entrySet()) {
            map.putAll(BaseDao.super.indexedSearch(search, db(entry.getKey()), entry.getValue()));
        }
        indexDb.close();
        return map;
    }

    /**
     * Refer to @BaseDao.super.indexedSearch
     */
    default ConcurrentMap<K, V> indexedSearch(final Search search, final int limit) throws IOException {
        final var map = new ConcurrentHashMap<K, V>();
        final DB indexDb = MapDb.MAP_DB.db(getIndexPath(search.field()));
        final var index = (ConcurrentMap<String, Map<Object, List<K>>>) indexDb.hashMap("map").createOrOpen();
        final var recordsAtMap = fileAtIndex(index, search.searchTerm());

        if (recordsAtMap.size() > 2) {
            recordsAtMap.entrySet().parallelStream().allMatch(entry -> {
                try {
                    map.putAll(BaseDao.super.indexedSearch(search, db(entry.getKey()), entry.getValue(), limit));
                } catch (final IOException e) {
                    CHRONICLE_UTILS.dbFetchError(name(), entry.getKey());
                }
                return map.size() == limit;
            });

            indexDb.close();
            return map.entrySet().stream().limit(limit).collect(Collectors.toConcurrentMap(Map.Entry::getKey,
                    Map.Entry::getValue));
        }

        for (final var entry : recordsAtMap.entrySet()) {
            map.putAll(BaseDao.super.indexedSearch(search, db(entry.getKey()), entry.getValue(), limit));
            if (map.size() == limit)
                break;
        }

        indexDb.close();
        return map;
    }
}