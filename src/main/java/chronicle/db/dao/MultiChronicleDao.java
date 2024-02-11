package chronicle.db.dao;

import static chronicle.db.dao.ChronicleUtils.CHRONICLE_UTILS;
import static chronicle.db.service.ChronicleDb.CHRONICLE_DB;
import static chronicle.db.service.MapDb.MAP_DB;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
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

import org.mapdb.HTreeMap;
import org.tinylog.Logger;

import chronicle.db.entity.PutStatus;
import chronicle.db.entity.Search;
import chronicle.db.service.HandleConsumer;
import net.openhft.chronicle.map.ChronicleMap;

/**
 *
 * @param <K> Type of the unique identifier
 * @param <V> Type of the single element
 */
public interface MultiChronicleDao<K, V> extends BaseDao<K, V> {
    default List<String> getFiles() throws IOException {
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
            final var db = db(file);
            if (db.size() + records <= entries()) {
                db.close();
                return file;
            }
            db.close();
        }
        return String.valueOf(files.size() + 1);
    }

    /**
     * Get the db object, you must close the map manually
     * 
     * @param path the file name
     * @throws IOException
     */
    default ChronicleMap<K, V> db(final String path) throws IOException {
        return CHRONICLE_DB.createOrGet(name(), entries(), averageKey(), averageValue(), dataPath() + "/data/" + path,
                bloatFactor());
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
                    CHRONICLE_UTILS.removeFromIndex(file, name(), dataPath(), indexFileNames(), Map.of(key, value));
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
        final Map<K, V> updated = new HashMap<>();

        if (size != 0) {
            CHRONICLE_UTILS.deleteAllLog(name() + " at file " + file);
            for (final K key : keys) {
                final var value = db.getUsing(key, using());
                if (Objects.nonNull(value)) {
                    deleted += 1;
                    updated.put(key, value);
                }
            }
            if (deleted != 0) {
                db.keySet().removeAll(keys);
            }
        }

        if (containsIndexes()) {
            CHRONICLE_UTILS.removeFromIndex(file, name(), dataPath(), indexFileNames(), updated);
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
    default PutStatus put(final K key, final V value, final String file, final List<String> indexFileNames)
            throws IOException {
        Logger.info("Inserting into {} at file {} using key {}.", name(), file, key);

        final var db = db(file);
        final var prevValue = db.put(key, value);
        final var updated = prevValue != null;
        final var prevValueMap = new HashMap<>();
        if (updated)
            prevValueMap.put(key, prevValue);
        db.close();

        if (indexFileNames.size() != 0) {
            CHRONICLE_UTILS.updateIndex(file, name(), dataPath(), indexFileNames, Map.of(key, value), prevValueMap);
        }
        return updated ? PutStatus.UPDATED : PutStatus.INSERTED;
    }

    /**
     * Refers to the method above
     */
    default PutStatus put(final K key, final V value, final String file) throws IOException {
        return put(key, value, file, indexFileNames());
    }

    /**
     * Add a value
     * 
     * @param key   the key
     * @param value the value
     * @return true if updated else false
     * @throws IOException
     */
    default PutStatus put(final K key, final V value, final List<String> indexFileNames) throws IOException {
        final var getMap = get(key);

        if (getMap.size() != 0) {
            final var file = getMap.keySet().iterator().next();
            return put(key, value, file, indexFileNames);
        }

        return put(key, value, getInsertFile(1), indexFileNames);
    }

    /**
     * Refers to the method above
     */
    default PutStatus put(final K key, final V value) throws IOException {
        return put(key, value, indexFileNames());
    }

    /**
     * Add multiple keys and values into the db if you know the file
     * 
     * @param map  the map to add
     * @param file the file name
     * @throws IOException
     */
    default void put(final Map<K, V> map, final String file, final List<String> indexFileNames) throws IOException {
        if (map.size() > entries()) {
            Logger.error("Insert size bigger than entry size.");
            return;
        }
        Logger.info("Inserting multiple values into {}.", name());
        final var db = db(file);
        final var prevValues = new HashMap<K, V>();

        for (final var entry : map.entrySet()) {
            final var updated = db.put(entry.getKey(), entry.getValue());
            if (updated != null)
                prevValues.put(entry.getKey(), updated);
        }
        db.close();

        if (indexFileNames.size() != 0) {
            CHRONICLE_UTILS.updateIndex(file, name(), dataPath(), indexFileNames, map, prevValues);
        }
    }

    /**
     * Refers to the method above
     */
    default void put(final Map<K, V> map, final String file) throws IOException {
        put(map, file, indexFileNames());
    }

    /**
     * Add multiple keys and values into the db
     * this method will update existing records and insert new ones
     * 
     * @param map            the map to add
     * @param updateExisting if we should update or just insert.
     * @throws IOException
     */
    default void put(final Map<K, V> map, final boolean updateExisting, final List<String> indexFileNames)
            throws IOException {
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
                put(entry.getValue(), entry.getKey(), indexFileNames);
            }
        }

        put(map, getInsertFile(map.keySet().size()), indexFileNames);
    }

    /**
     * Refers to the method above
     */
    default void put(final Map<K, V> map, final boolean updateExisting) throws IOException {
        put(map, updateExisting, indexFileNames());
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
    default ConcurrentMap<String, ConcurrentMap<K, V>> search(final Search search) throws IOException {
        final var map = new ConcurrentHashMap<String, ConcurrentMap<K, V>>();
        final var files = getFiles();

        if (multiThreaded(files)) {
            files.parallelStream().forEach(file -> {
                try (var db = db(file)) {
                    final var currentFileMap = new ConcurrentHashMap<K, V>();
                    for (final var entry : db.entrySet()) {
                        try {
                            CHRONICLE_UTILS.search(search, entry.getKey(), entry.getValue(), currentFileMap);
                            map.put(file, currentFileMap);
                        } catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException
                                | SecurityException e) {
                            Logger.error("No such field: {} exists on searching. {}", search.field(), e);
                            break;
                        }
                    }
                    db.close();
                } catch (final IOException e) {
                    Logger.error("Db does not exist: {}.", file);
                }
            });

            return map;
        }

        for (final String file : files) {
            final var db = db(file);
            final var currentFileMap = new ConcurrentHashMap<K, V>();
            for (final var entry : db.entrySet()) {
                try {
                    CHRONICLE_UTILS.search(search, entry.getKey(), entry.getValue(), currentFileMap);
                    map.put(file, currentFileMap);
                } catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException
                        | SecurityException e) {
                    Logger.error("No such field: {} exists on searching. {}", search.field(), e);
                    break;
                }
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
    default ConcurrentMap<String, ConcurrentMap<K, V>> search(final Search search, final int limit)
            throws IOException {
        final var map = new ConcurrentHashMap<String, ConcurrentMap<K, V>>();
        final var files = getFiles();

        if (multiThreaded(files)) {
            files.parallelStream().allMatch(file -> {
                try (final var db = db(file)) {
                    final var currentFileMap = new ConcurrentHashMap<K, V>();
                    for (final var entry : db.entrySet()) {
                        try {
                            CHRONICLE_UTILS.search(search, entry.getKey(), entry.getValue(), currentFileMap);
                            map.put(file, currentFileMap);
                        } catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException
                                | SecurityException e) {
                            Logger.error("No such field: {} exists on searching. {}", search.field(), e);
                            break;
                        }
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
                final var currentFileMap = new ConcurrentHashMap<K, V>();
                for (final var entry : db.entrySet()) {
                    try {
                        CHRONICLE_UTILS.search(search, entry.getKey(), entry.getValue(), currentFileMap);
                        map.put(file, currentFileMap);
                    } catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException
                            | SecurityException e) {
                        Logger.error("No such field: {} exists on searching. {}", search.field(), e);
                        break;
                    }
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
     * @throws IOException
     * 
     */
    default void initIndex(final String[] fields) throws IOException {
        final var files = getFiles();

        for (final var field : fields) {
            final String path = getIndexPath(field);
            CHRONICLE_UTILS.deleteFileIfExists(path);
            final HTreeMap<String, Map<Object, List<K>>> indexDb = MAP_DB.getDb(path);
            if (multiThreaded(files)) {
                files.parallelStream().forEach(HandleConsumer.handleConsumerBuilder(f -> {
                    final var db = db(f);
                    CHRONICLE_UTILS.index(db, name(), field, indexDb, f);
                    db.close();
                }));
            } else
                for (final String f : files) {
                    final var db = db(f);
                    CHRONICLE_UTILS.index(db, name(), field, indexDb, f);
                    db.close();
                }
            indexDb.close();
        }
    }

    /**
     * Delete and rerun all indexes. Faster when inserting a lot of records.
     * 
     * @throws IOException
     */
    default void refreshIndexes() throws IOException {
        initIndex(deleteIndexes());
    }

    /**
     * Refer to @BaseDao.super.indexedSearch
     * 
     * @throws SecurityException
     * @throws NoSuchFieldException
     */
    default ConcurrentMap<String, ConcurrentMap<K, V>> indexedSearch(final Search search)
            throws IOException, NoSuchFieldException, SecurityException {
        final var map = new ConcurrentHashMap<String, ConcurrentMap<K, V>>();
        final var indexFilePath = getIndexPath(search.field());
        if (!Files.exists(Paths.get(indexFilePath))) {
            Logger.info("Index file does not exist, it will be created.");
            initIndex(new String[] { search.field() });
        }
        final HTreeMap<String, Map<Object, List<K>>> indexDb = MAP_DB.getDb(indexFilePath);
        final var files = getFiles();

        if (multiThreaded(files)) {
            files.parallelStream().forEach(HandleConsumer.handleConsumerBuilder(file -> {
                final var db = db(file);
                map.put(file, BaseDao.super.indexedSearch(search, db, indexDb.get(file)));
                db.close();
            }));
            indexDb.close();
            return map;
        }

        for (final var file : files) {
            final var db = db(file);
            map.put(file, BaseDao.super.indexedSearch(search, db, indexDb.get(file)));
            db.close();
        }
        indexDb.close();
        return map;
    }

    /**
     * Refer to @BaseDao.super.indexedSearch
     * 
     * @throws SecurityException
     * @throws NoSuchFieldException
     */
    default ConcurrentMap<String, ConcurrentMap<K, V>> indexedSearch(final Search search, final int limit)
            throws IOException, NoSuchFieldException, SecurityException {
        final var map = new ConcurrentHashMap<String, ConcurrentMap<K, V>>();
        final HTreeMap<String, Map<Object, List<K>>> indexDb = MAP_DB.getDb(getIndexPath(search.field()));
        final var files = getFiles();

        if (multiThreaded(files)) {
            files.parallelStream().allMatch(file -> {
                try {
                    final var db = db(file);
                    map.put(file, BaseDao.super.indexedSearch(search, db, indexDb.get(file), limit));
                    db.close();
                } catch (final IOException e) {
                    CHRONICLE_UTILS.dbFetchError(name(), file);
                }
                return map.size() == limit;
            });

            indexDb.close();
            return map.entrySet().stream().limit(limit).collect(Collectors.toConcurrentMap(Map.Entry::getKey,
                    Map.Entry::getValue));
        }

        for (final var file : files) {
            final var db = db(file);
            map.put(file, BaseDao.super.indexedSearch(search, db, indexDb.get(file), limit));
            db.close();
            if (map.size() == limit)
                break;
        }

        indexDb.close();
        return map;
    }

    /**
     * Refer to @BaseDao.super.indexedSearch
     * 
     * @throws SecurityException
     * @throws NoSuchFieldException
     */
    default ConcurrentMap<String, ConcurrentMap<K, V>> indexedSearch(
            final ConcurrentMap<String, ConcurrentMap<K, V>> db, final Search search)
            throws IOException, NoSuchFieldException, SecurityException {
        final var map = new ConcurrentHashMap<String, ConcurrentMap<K, V>>();
        final HTreeMap<String, Map<Object, List<K>>> indexDb = MAP_DB.getDb(getIndexPath(search.field()));
        final var files = db.keySet().stream().collect(Collectors.toList());

        if (multiThreaded(files)) {
            files.parallelStream().forEach(HandleConsumer.handleConsumerBuilder(file -> {
                map.put(file, BaseDao.super.indexedSearch(search, db.get(file), indexDb.get(file)));
            }));
            indexDb.close();
            return map;
        }

        for (final var file : files) {
            map.put(file, BaseDao.super.indexedSearch(search, db.get(file), indexDb.get(file)));
        }
        indexDb.close();
        return map;
    }

    /**
     * Refer to @BaseDao.super.indexedSearch
     * 
     * @throws SecurityException
     * @throws NoSuchFieldException
     * @throws IOException
     */
    default ConcurrentMap<String, ConcurrentMap<K, V>> indexedSearch(
            final ConcurrentMap<String, ConcurrentMap<K, V>> db, final Search search, final int limit)
            throws NoSuchFieldException, SecurityException, IOException {
        final var map = new ConcurrentHashMap<String, ConcurrentMap<K, V>>();
        final HTreeMap<String, Map<Object, List<K>>> indexDb = MAP_DB.getDb(getIndexPath(search.field()));
        final var files = db.keySet().stream().collect(Collectors.toList());

        if (multiThreaded(files)) {
            files.parallelStream().allMatch(file -> {
                try {
                    map.put(file, BaseDao.super.indexedSearch(search, db.get(file), indexDb.get(file), limit));
                } catch (final IOException e) {
                    CHRONICLE_UTILS.dbFetchError(name(), file);
                }
                return map.size() == limit;
            });

            indexDb.close();
            return map.entrySet().stream().limit(limit).collect(Collectors.toConcurrentMap(Map.Entry::getKey,
                    Map.Entry::getValue));
        }

        for (final var file : files) {
            map.put(file, BaseDao.super.indexedSearch(search, db.get(file), indexDb.get(file), limit));
            if (map.size() == limit)
                break;
        }

        indexDb.close();
        return map;
    }

    default void clearDb(final String file) throws IOException {
        final var db = db(file);
        db.clear();
        db.close();
    }

    default void clearDb() throws IOException {
        final var files = getFiles();

        for (final String file : files) {
            final var db = db(file);
            db.clear();
            db.close();
        }
    }
}
