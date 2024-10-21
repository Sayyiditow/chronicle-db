package chronicle.db.dao;

import static chronicle.db.dao.ChronicleUtils.CHRONICLE_UTILS;
import static chronicle.db.service.ChronicleDb.CHRONICLE_DB;
import static chronicle.db.service.MapDb.MAP_DB;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.tinylog.Logger;

import com.jsoniter.spi.TypeLiteral;

import chronicle.db.entity.PutStatus;
import chronicle.db.entity.Search;
import chronicle.db.entity.Search.SearchType;
import net.openhft.chronicle.map.ChronicleMap;

/**
 *
 * @param <K> Type of the unique identifier
 * @param <V> Type of the single element
 */
public interface ChronicleDao<K, V> {
    ConcurrentMap<String, Object> LOCKS = new ConcurrentHashMap<>();
    String DATA_DIR = "/data/", INDEX_DIR = "/indexes/", FILES_DIR = "/files/", BACKUP_DIR = "/backup/",
            DATA_FILE = "data", CORRUPTED_FILE = "corrupted", RECOVER_FILE = "recovery";
    String[] dbDirs = { DATA_DIR, INDEX_DIR, FILES_DIR, BACKUP_DIR };

    /**
     * Name of db for logging purposes
     */
    default String name() {
        return averageValue().getClass().getSimpleName();
    }

    /**
     * Max entries per file for multiple mode, intiial size for single mode.
     */
    long entries();

    /**
     * The average key value
     */
    K averageKey();

    /**
     * The average key value
     */
    V averageValue();

    /**
     * Path to the directory where the files will reside
     */
    String dataPath();

    /**
     * Reusable value object
     */
    V using();

    /**
     * Typeliteral to be used when casting a json object into the required java
     * class
     */
    TypeLiteral<V> jsonType();

    /**
     * The bloatFactor is used when the file contents can grow much more than the
     * average value,
     * defaults to 1
     */
    default double bloatFactor() {
        return 1;
    }

    /**
     * Create the folders required on init
     */
    default void createDataDirs() {
        if (!Files.exists(Path.of(dataPath()))) {
            for (final String dir : dbDirs) {
                try {
                    Files.createDirectories(Path.of(dataPath() + dir));
                } catch (final IOException e) {
                    Logger.error("Error on db directory creation for {}. {}.", dataPath(), e.getMessage());
                }
            }
        }
    }

    /**
     * Helps to backup all data files in /data to /backup
     */
    default void backup() {
        try {
            final var dataPath = dataPath() + DATA_DIR;
            final var backupPath = dataPath() + BACKUP_DIR;
            final var backupDirPath = Path.of(backupPath);
            final var dataFiles = CHRONICLE_UTILS.getFileList(dataPath);
            Files.createDirectories(backupDirPath);

            for (final var file : dataFiles) {
                Files.copy(Path.of(dataPath + file), Path.of(backupPath + file), REPLACE_EXISTING);
            }
        } catch (final IOException e) {
            Logger.error("Error on db backup for {}. {}.", dataPath(), e.getMessage());
        }
    }

    /**
     * If this database object contains indexes
     * 
     * @throws IOException
     */
    default boolean containsIndexes() throws IOException {
        return CHRONICLE_UTILS.getFileList(dataPath() + INDEX_DIR).size() > 0;
    }

    default List<String> indexFileNames() throws IOException {
        return CHRONICLE_UTILS.getFileList(dataPath() + INDEX_DIR);
    }

    /**
     * Delete and rerun all indexes. Faster when inserting a lot of records.
     * 
     * @throws IOException
     */
    default String[] deleteIndexes() throws IOException {
        final var available = indexFileNames();
        available.forEach(f -> {
            CHRONICLE_UTILS.deleteFileIfExists(dataPath() + INDEX_DIR + f);
        });

        return available.toArray(new String[available.size()]);
    }

    /**
     * Get the index map to use
     * 
     * @param field the field of the V value object
     * @return map of the index
     * @throws IOException
     */
    default String getIndexPath(final String field) {
        return dataPath() + INDEX_DIR + field;
    }

    /**
     * Get the db object, close with closeDb()
     * 
     * @return ChronicleMap<K, V>
     * @throws IOException
     */
    private ChronicleMap<K, V> getDb() throws IOException {
        return CHRONICLE_DB.getDb(name(), entries(), averageKey(), averageValue(), dataPath() + DATA_DIR + DATA_FILE,
                bloatFactor());
    }

    /**
     * Close db object
     * 
     * @throws IOException
     */
    private void closeDb() {
        CHRONICLE_DB.closeDb(dataPath() + DATA_DIR + DATA_FILE);
    }

    /**
     * Only runs to initialize an index on the field first time
     * 
     * @param field the field of the V value object
     * @throws IOException
     * 
     */
    default void initIndex(final String[] fields) throws IOException {
        final var db = fetch();
        CHRONICLE_UTILS.index(db, name(), fields, dataPath(), dataPath() + INDEX_DIR);
    }

    /**
     * Delete and rerun all indexes. Faster when inserting a lot of records.
     * 
     * @throws IOException
     */
    default void refreshIndexes() throws IOException {
        Logger.info("Re-initializing indexes at {}.", dataPath());
        initIndex(deleteIndexes());
    }

    /**
     * Initialize indexes at dao creation
     * 
     * @param fields
     * @throws IOException
     */
    default void initDefaultIndexes(final String[] fields) throws IOException {
        if (!CHRONICLE_UTILS.getFileList(dataPath() + DATA_DIR).isEmpty()) {
            final var indexFiles = new HashSet<>(indexFileNames());
            if (indexFiles.size() != fields.length) {
                final var toIndex = Arrays.stream(fields).filter(field -> !indexFiles.contains(field))
                        .collect(Collectors.toList());

                if (!toIndex.isEmpty()) {
                    initIndex(toIndex.toArray(new String[toIndex.size()]));
                }
            }
        }
    }

    /**
     * In cases onf data corruption, we can recover the db using this method
     */
    default void recoverData() throws IOException {
        final var db = CHRONICLE_DB.recoverDb(name(), entries(), averageKey(), averageValue(),
                dataPath() + DATA_DIR + DATA_FILE, bloatFactor());
        final var dbRecovery = CHRONICLE_DB.getDb(name(), entries(), averageKey(), averageValue(),
                dataPath() + DATA_DIR + RECOVER_FILE, bloatFactor());
        dbRecovery.putAll(db);
        Files.move(Path.of(dataPath() + DATA_DIR + DATA_FILE), Path.of(dataPath() + DATA_DIR + CORRUPTED_FILE),
                REPLACE_EXISTING);
        Files.move(Path.of(dataPath() + DATA_DIR + RECOVER_FILE), Path.of(dataPath() + DATA_DIR + DATA_FILE),
                REPLACE_EXISTING);
        refreshIndexes();
    }

    /**
     * Fetches all records in the db
     * 
     * @return Map<K, V>
     * @throws IOException
     */
    default Map<K, V> fetch() throws IOException {
        Logger.info("Fetching all data at {}.", dataPath());
        final ChronicleMap<K, V> db = getDb();
        final Map<K, V> map = new HashMap<>(db);
        closeDb();
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
        final var db = getDb();
        CHRONICLE_UTILS.getLog(name(), key, dataPath());
        final V value = db.getUsing(key, using());
        closeDb();
        return value;
    }

    /**
     * Get all the values for a list of keys
     * 
     * @param keys list of keys
     * @return Map<K, V> values
     * @throws IOException
     */
    default Map<K, V> get(final Set<K> keys) throws IOException {
        Logger.info("Querying {} using multiple keys {} at {}.", name(), keys, dataPath());
        final var map = new HashMap<K, V>(keys.size());
        final var db = getDb();
        for (final K key : keys) {
            final V value = db.getUsing(key, using());
            if (Objects.nonNull(value))
                map.put(key, value);
        }
        closeDb();
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
        final var db = getDb();
        CHRONICLE_UTILS.deleteLog(name(), key, dataPath());
        final V value = db.remove(key);
        closeDb();

        if (value != null) {
            CHRONICLE_UTILS.successDeleteLog(name(), key, dataPath());
            if (containsIndexes()) {
                CHRONICLE_UTILS.removeFromIndex(name(), dataPath(), indexFileNames(), Map.of(key, value));
            }
            return true;
        }

        return false;
    }

    /**
     * Remove a value using a list of keys
     * 
     * @param keys the keys to remove
     * @return true if updated else false
     * @throws IOException
     * @throws InterruptedException
     */
    default boolean delete(final Set<K> keys) throws IOException, InterruptedException {
        Logger.info("Deleting multiple values from {} using keys {} at {}.", name(), keys, dataPath());
        final var db = getDb();
        Map<K, V> updatedMap = new HashMap<>();

        if (containsIndexes()) {
            updatedMap = get(keys);
        }

        final var updated = db.keySet().removeAll(keys);

        if (updated) {
            Logger.info("Objects with keys {} deleted from {} at {}.", keys, name(), dataPath());
            if (containsIndexes()) {
                CHRONICLE_UTILS.removeFromIndex(name(), dataPath(), indexFileNames(), updatedMap);
            }
        }
        closeDb();

        return updated;
    }

    /**
     * Create a bigger file if size == entries
     * 
     * @param db
     * @return
     * @throws IOException
     */
    private ChronicleMap<K, V> createNewDb(final ChronicleMap<K, V> db) throws IOException {
        Logger.info("Increasing entry size on db {}.", name());
        final var dataFilePath = dataPath() + DATA_DIR + DATA_FILE;
        final var backupDataFilePath = dataPath() + BACKUP_DIR + DATA_FILE;
        final var tempDataFilePath = dataPath() + DATA_DIR + "data.tmp";
        final var newSize = entries() * ((db.size() / entries()) + 1) + entries();
        final var newDb = CHRONICLE_DB.getDb(name(), newSize, averageKey(), averageValue(), tempDataFilePath,
                bloatFactor());
        newDb.putAll(db);
        closeDb();
        Files.move(Path.of(dataFilePath), Path.of(backupDataFilePath), REPLACE_EXISTING);
        Files.move(Path.of(tempDataFilePath), Path.of(dataFilePath), REPLACE_EXISTING);
        return newDb;
    }

    /**
     * Add a value
     * 
     * @param key   the key
     * @param value the value
     * @return true if updated else false
     * @throws IOException
     */
    default PutStatus put(final K key, final V value, final List<String> indexFileNames)
            throws IOException {
        // create a bigger file if records in db are equal to multiple of entries()
        var status = PutStatus.INSERTED;
        var db = getDb();
        final Object lock = LOCKS.computeIfAbsent(name(), k -> new Object());
        synchronized (lock) {
            if (db.size() != 0 && db.size() % entries() == 0)
                db = createNewDb(db);
        }
        final V prevValue = db.put(key, value);
        closeDb();
        final var updated = prevValue != null;
        final var prevValueMap = new HashMap<K, V>(1);
        if (updated) {
            prevValueMap.put(key, prevValue);
            status = PutStatus.UPDATED;
        }
        if (containsIndexes()) {
            CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames, Map.of(key, value), prevValueMap);
        }

        Logger.info("{} into {} using key {} at {}.", status, name(), key, dataPath());

        return status;
    }

    /**
     * Refer to method above
     * 
     */
    default PutStatus put(final K key, final V value) throws IOException {
        return put(key, value, indexFileNames());
    }

    /**
     * Add multiple values into the db, then update all indexes related
     * 
     * @param map the map to add
     * @throws IOException
     */
    default void put(final Map<K, V> map) throws IOException {
        if (map.size() > entries()) {
            Logger.error("Insert size bigger than entry size.");
            return;
        }

        Logger.info("Inserting multiple values into {} at {}.", name(), dataPath());
        var db = getDb();
        final var prevValues = new HashMap<K, V>(map.size());
        final Object lock = LOCKS.computeIfAbsent(name(), k -> new Object());
        synchronized (lock) {
            if (db.size() + map.size() > entries())
                db = createNewDb(db);
        }

        for (final var entry : map.entrySet()) {
            final K key = entry.getKey();
            final V updated = db.put(key, entry.getValue());
            if (updated != null)
                prevValues.put(key, updated);
        }
        closeDb();

        CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames(), map, prevValues);
    }

    /**
     * Add multiple values into the db with no indexing
     * 
     * @param map the map to add
     * @throws IOException
     */
    default void putAll(final Map<K, V> map) throws IOException {
        if (map.size() > entries()) {
            Logger.error("Insert size bigger than entry size.");
            return;
        }

        Logger.info("Inserting multiple values into {} at {}.", name(), dataPath());
        var db = getDb();
        final Object lock = LOCKS.computeIfAbsent(name(), k -> new Object());
        synchronized (lock) {
            if (db.size() + map.size() > entries())
                db = createNewDb(db);
        }
        db.putAll(map);
        closeDb();
    }

    /**
     * Search the chronicle map based on values
     * 
     * @param db     the map
     * @param search object search
     * @return a map of the fitting values
     * @throws IOException
     */
    default Map<K, V> search(final Map<K, V> db, final Search search) throws IOException {
        Logger.info("Searching DB at {} for {}.", dataPath(), search);
        final Map<K, V> map = new HashMap<>();

        for (final var entry : db.entrySet()) {
            try {
                CHRONICLE_UTILS.search(search, entry.getKey(), entry.getValue(), map);
            } catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException e) {
                Logger.error("No such field: {} exists on searching {}. {}", search.field(), name(), e);
                break;
            }
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
    default Map<K, V> search(final Map<K, V> db, final Search search, final int limit)
            throws IOException {
        Logger.info("Searching DB at {} for {} with limit {}.", dataPath(), search, limit);
        final Map<K, V> map = new HashMap<>();

        for (final var entry : db.entrySet()) {
            try {
                CHRONICLE_UTILS.search(search, entry.getKey(), entry.getValue(), map);
            } catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException e) {
                Logger.error("No such field: {} exists on searching {}. {}", search.field(), name(), e);
                break;
            }
            if (map.size() == limit) {
                break;
            }
        }

        return map;
    }

    /**
     * Search the chronicle map based on values
     * 
     * @param db     the map
     * @param search object search
     * @return a map of the fitting values
     * @throws IOException
     */
    default Map<K, V> search(final Search search) throws IOException {
        Logger.info("Searching DB at {} for {}.", dataPath(), search);
        final var db = getDb();
        final Map<K, V> map = search(db, search);
        closeDb();
        return map;
    }

    /**
     * Search the chronicle map based on values
     * 
     * @param search object search
     * @return a map of the fitting values
     * @throws IOException
     */
    default Map<K, V> search(final Search search, final int limit)
            throws IOException {
        Logger.info("Searching DB at {} for {} with limit {}.", dataPath(), search, limit);
        final var db = getDb();
        final Map<K, V> map = search(db, search, limit);
        closeDb();
        return map;
    }

    private void addSearchedValues(final List<K> keys, final Map<K, V> db, final Map<K, V> match) {
        if (keys != null)
            for (final var key : keys) {
                final V value = db.get(key);
                if (value != null)
                    match.put(key, value);
            }
    }

    private void addSearchedValues(final List<K> keys, final Map<K, V> db, final Map<K, V> match,
            final int limit) {
        if (keys != null)
            for (final var key : keys) {
                final V value = db.get(key);
                if (value != null)
                    match.put(key, value);

                if (match.size() == limit) {
                    return;
                }
            }
    }

    /**
     * Searches the objects using an index, without needed to loop over every record
     * Only useful for @code SearchType.EQUAL and @code SearchType.NOT_EQUAL
     * 
     * @param search the Search object
     * @param db
     * @param index
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    private Map<K, V> indexedSearch(final Search search, final Map<K, V> db,
            final Map<Object, List<K>> index) throws IOException {
        Logger.info("Index searching DB at {} for {}.", dataPath(), search);
        final var match = new HashMap<K, V>();
        if (index != null) {
            final var keys = new ArrayList<K>();
            final var keySet = index.keySet();
            List<Object> searchTermList = new ArrayList<>();

            if (keySet.size() > 0) {
                final var fieldClass = keySet.stream().filter(Objects::nonNull).findFirst().get().getClass();
                final Object searchTerm = CHRONICLE_UTILS.setSearchTerm(search.searchTerm(), fieldClass);
                if (search.searchType() == SearchType.IN || search.searchType() == SearchType.NOT_IN) {
                    searchTermList = CHRONICLE_UTILS.setSearchTerm((List<Object>) search.searchTerm(), fieldClass);
                }

                switch (search.searchType()) {
                    case EQUAL:
                        addSearchedValues(index.get(searchTerm), db, match);
                        break;
                    case NOT_EQUAL:
                        index.keySet().remove(searchTerm);
                        for (final var list : index.entrySet()) {
                            addSearchedValues(list.getValue(), db, match);
                        }
                        break;
                    case LESS:
                        for (final var entry : index.entrySet()) {
                            if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) < 0)
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case GREATER:
                        for (final var entry : index.entrySet()) {
                            if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) > 0)
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case LESS_OR_EQUAL:
                        for (final var entry : index.entrySet()) {
                            if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) <= 0)
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case GREATER_OR_EQUAL:
                        for (final var entry : index.entrySet()) {
                            if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) >= 0)
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case LIKE:
                        for (final var entry : index.entrySet()) {
                            if (CHRONICLE_UTILS.containsIgnoreCase(entry.getKey(), searchTerm))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case NOT_LIKE:
                        for (final var entry : index.entrySet()) {
                            if (!CHRONICLE_UTILS.containsIgnoreCase(entry.getKey(), searchTerm))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case CONTAINS:
                        for (final var entry : index.entrySet()) {
                            if (Collections.singleton(entry.getKey()).contains(searchTerm))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case NOT_CONTAINS:
                        for (final var entry : index.entrySet()) {
                            if (!Collections.singleton(entry.getKey()).contains(searchTerm))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case STARTS_WITH:
                        for (final var entry : index.entrySet()) {
                            if (String.valueOf(entry.getKey()).startsWith(String.valueOf(searchTerm)))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case ENDS_WITH:
                        for (final var entry : index.entrySet()) {
                            if (String.valueOf(entry.getKey()).endsWith(String.valueOf(searchTerm)))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case IN:
                        for (final var entry : index.entrySet()) {
                            if (searchTermList.contains(entry.getKey()))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case NOT_IN:
                        for (final var entry : index.entrySet()) {
                            if (!searchTermList.contains(entry.getKey()))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                }
            }
        }
        return match;
    }

    /**
     * Searches the objects using an index, without needed to loop over every record
     * Only useful for @code SearchType.EQUAL and @code SearchType.NOT_EQUAL
     * 
     * @param search the Search object
     * @param db
     * @param index
     * @param limit
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    private Map<K, V> indexedSearch(final Search search, final Map<K, V> db,
            final Map<Object, List<K>> index, final int limit) throws IOException {
        Logger.info("Index searching DB at {} for {} with limit {}.", dataPath(), search, limit);
        final var match = new HashMap<K, V>();
        if (index != null) {
            final var keys = new ArrayList<K>();
            final var keySet = index.keySet();
            List<Object> searchTermList = new ArrayList<>();

            if (keySet.size() > 0) {
                final var fieldClass = keySet.iterator().next().getClass();
                final Object searchTerm = CHRONICLE_UTILS.setSearchTerm(search.searchTerm(), fieldClass);
                if (search.searchType() == SearchType.IN || search.searchType() == SearchType.NOT_IN) {
                    searchTermList = CHRONICLE_UTILS.setSearchTerm((List<Object>) search.searchTerm(), fieldClass);
                }

                switch (search.searchType()) {
                    case EQUAL:
                        addSearchedValues(index.get(searchTerm), db, match, limit);
                        break;
                    case NOT_EQUAL:
                        index.keySet().remove(searchTerm);
                        for (final var list : index.entrySet()) {
                            addSearchedValues(list.getValue(), db, match, limit);
                        }
                        break;
                    case LESS:
                        for (final var entry : index.entrySet()) {
                            if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) < 0)
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match, limit);
                        break;
                    case GREATER:
                        for (final var entry : index.entrySet()) {
                            if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) > 0)
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match, limit);
                        break;
                    case LESS_OR_EQUAL:
                        for (final var entry : index.entrySet()) {
                            if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) <= 0)
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match, limit);
                        break;
                    case GREATER_OR_EQUAL:
                        for (final var entry : index.entrySet()) {
                            if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) >= 0)
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match, limit);
                        break;
                    case LIKE:
                        for (final var entry : index.entrySet()) {
                            if (CHRONICLE_UTILS.containsIgnoreCase(entry.getKey(), searchTerm))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match, limit);
                        break;
                    case NOT_LIKE:
                        for (final var entry : index.entrySet()) {
                            if (!CHRONICLE_UTILS.containsIgnoreCase(entry.getKey(), searchTerm))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match, limit);
                        break;
                    case CONTAINS:
                        for (final var entry : index.entrySet()) {
                            if (Collections.singleton(entry.getKey()).contains(searchTerm))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match, limit);
                        break;
                    case NOT_CONTAINS:
                        for (final var entry : index.entrySet()) {
                            if (!Collections.singleton(entry.getKey()).contains(searchTerm))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match, limit);
                        break;
                    case STARTS_WITH:
                        for (final var entry : index.entrySet()) {
                            if (String.valueOf(entry.getKey()).startsWith(String.valueOf(searchTerm)))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match, limit);
                        break;
                    case ENDS_WITH:
                        for (final var entry : index.entrySet()) {
                            if (String.valueOf(entry.getKey()).endsWith(String.valueOf(searchTerm)))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match, limit);
                        break;
                    case IN:
                        for (final var entry : index.entrySet()) {
                            if (searchTermList.contains(entry.getKey()))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case NOT_IN:
                        for (final var entry : index.entrySet()) {
                            if (!searchTermList.contains(entry.getKey()))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                }
            }
        }
        return match;
    }

    default Map<K, V> indexedSearch(final Search search) throws IOException {
        final var indexFilePath = getIndexPath(search.field());
        final var db = getDb();

        try {
            return indexedSearch(search, db, MAP_DB.getDb(indexFilePath));
        } catch (final IOException e) {
            CHRONICLE_UTILS.indexedSearchErrorLog(name(), dataPath());
            return new HashMap<K, V>();
        } finally {
            closeDb();
            MAP_DB.closeDb(indexFilePath);
        }
    }

    default Map<K, V> indexedSearch(final Search search, final int limit) throws IOException {
        final var indexFilePath = getIndexPath(search.field());
        final var db = getDb();

        try {
            return indexedSearch(search, db, MAP_DB.getDb(indexFilePath), limit);
        } catch (final IOException e) {
            CHRONICLE_UTILS.indexedSearchErrorLog(name(), dataPath());
            return new HashMap<K, V>();
        } finally {
            closeDb();
            MAP_DB.closeDb(indexFilePath);
        }
    }

    default Map<K, V> indexedSearch(final Map<K, V> db, final Search search) {
        final var indexFilePath = getIndexPath(search.field());

        try {
            return indexedSearch(search, db, MAP_DB.getDb(indexFilePath));
        } catch (final IOException e) {
            CHRONICLE_UTILS.indexedSearchErrorLog(name(), dataPath());
            return new HashMap<K, V>();
        } finally {
            MAP_DB.closeDb(indexFilePath);
        }
    }

    default Map<K, V> indexedSearch(final Map<K, V> db, final Search search, final int limit) {
        final var indexFilePath = getIndexPath(search.field());
        try {
            return indexedSearch(search, db, MAP_DB.getDb(indexFilePath), limit);
        } catch (final IOException e) {
            CHRONICLE_UTILS.indexedSearchErrorLog(name(), dataPath());
            return new HashMap<K, V>();
        } finally {
            MAP_DB.closeDb(indexFilePath);
        }
    }

    /**
     * Cases where the data being selected is a subset of the whole object
     * this will be used to return a map of key, map of required fields and the
     * values
     * 
     * @param initialMap the map containing the whole object fields
     * @param fields     the required fields
     */
    default Map<K, LinkedHashMap<String, Object>> subsetOfValues(final Map<K, V> initialMap,
            final String[] fields) {
        final var map = new HashMap<K, LinkedHashMap<String, Object>>();

        for (final var entry : initialMap.entrySet()) {
            CHRONICLE_UTILS.subsetOfValues(fields, entry, map, name());
        }
        return map;
    }

    /**
     * Current size of the data
     * 
     * @return int size
     * @throws IOException
     */
    default int size() throws IOException {
        Logger.info("Getting DB size at {}.", dataPath());
        final var db = getDb();
        final var size = db.size();
        closeDb();
        return size;
    }

    default void clearDb() throws IOException {
        Logger.info("Truncating database at {}.", dataPath());
        final var db = getDb();
        db.clear();
        closeDb();
    }

    default void deleteDataFiles() throws IOException {
        CHRONICLE_UTILS.deleteFileIfExists(dataPath() + DATA_DIR + DATA_FILE);
    }
}
