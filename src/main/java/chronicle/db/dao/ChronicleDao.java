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

import org.mapdb.HTreeMap;
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
            DATA_FILE = "data", CORRUPTED_FILE = "corrupted", RECOVER_FILE = "recovery", ENTRY_SIZE_FILE = "entrySize";
    String[] dbDirs = { DATA_DIR, INDEX_DIR, FILES_DIR, BACKUP_DIR };

    /**
     * Name of db for logging purposes
     */
    default String name() {
        return averageValue().getClass().getSimpleName();
    }

    /**
     * Everage entries per file, resize when required
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

    private void createDataDirs(final String dataPath) {
        if (!Files.exists(Path.of(dataPath))) {
            for (final String dir : dbDirs) {
                try {
                    Files.createDirectories(Path.of(dataPath + dir));
                } catch (final IOException e) {
                    Logger.error("Error on db directory creation for {}. {}.", dataPath, e.getMessage());
                }
            }
        }
    }

    /**
     * Create the folders required on init
     */
    default void createDataDirs() {
        createDataDirs(dataPath());
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
     * Create db by dynamic entry size from file and not from the java
     */
    default void createDbWithEntrySize() throws IOException {
        final var newDb = CHRONICLE_DB.getDb(name(), getCurrentEntrySize(), averageKey(), averageValue(),
                dataPath() + DATA_DIR + DATA_FILE, bloatFactor());
        newDb.close();
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
     * Only runs to initialize an index on the field first time
     * 
     * @param field the field of the V value object
     * @throws IOException
     * 
     */
    private void initIndex(final Map<K, V> db, final String[] fields, final String indexDirPath) throws IOException {
        CHRONICLE_UTILS.index(db, name(), fields, dataPath(), indexDirPath);
    }

    /**
     * Only runs to initialize an index on the field first time
     * 
     * @param field the field of the V value object
     * @throws IOException
     * 
     */
    default void initIndex(final String[] fields) throws IOException {
        initIndex(fetch(), fields, dataPath() + INDEX_DIR);
    }

    /**
     * Delete and rerun all indexes. Faster when inserting a lot of records.
     * 
     * @throws IOException
     */
    default void refreshIndexes() throws IOException {
        Logger.info("Re-initializing indexes at {}.", dataPath());
        final var indexFiles = indexFileNames();
        initIndex(indexFiles.toArray(new String[indexFiles.size()]));
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
                final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
                synchronized (lock) {
                    final var toIndex = Arrays.stream(fields).filter(field -> !indexFiles.contains(field))
                            .collect(Collectors.toList());

                    if (!toIndex.isEmpty()) {
                        initIndex(toIndex.toArray(new String[toIndex.size()]));
                    }
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
        final var db = getDb();
        CHRONICLE_UTILS.getLog(name(), key, dataPath());
        final V value = db.getUsing(key, using());
        db.close();
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
            if (value != null)
                map.put(key, value);
        }
        db.close();
        return map;
    }

    /**
     * Archives data from the active object to speed up access
     * The files/ will not be moved, only the data and indexes
     * 
     * @param folderName the arhive folder name - prefer monthly yyyyMM
     * @param keys       the set of keys to move out from the main object
     */
    default void archive(final String folderName, final Set<K> keys) throws IOException {
        final Map<K, V> values = get(keys);
        final var archiveDataPath = dataPath().replace(name(), "") + folderName + "/" + name();
        createDataDirs(archiveDataPath);
        final var archiveDb = CHRONICLE_DB.getDb(name(), values.size(), averageKey(), averageValue(),
                archiveDataPath + DATA_DIR + DATA_FILE, bloatFactor());
        try {
            archiveDb.putAll(values);
            final var indexFiles = indexFileNames();
            final var fields = indexFiles.toArray(new String[indexFiles.size()]);
            initIndex(archiveDb, fields, archiveDataPath + INDEX_DIR);

            for (final K key : keys) {
                CHRONICLE_UTILS.moveDirContentsStartsWith(Path.of(dataPath() + FILES_DIR),
                        Path.of(archiveDataPath + FILES_DIR), key.toString());
            }

            delete(archiveDb.keySet());
            initIndex(fields);
        } finally {
            archiveDb.close();
        }
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
        V value = null;
        try {
            value = db.remove(key);
        } finally {
            db.close();
        }

        if (value != null) {
            CHRONICLE_UTILS.successDeleteLog(name(), key, dataPath());
            CHRONICLE_UTILS.removeFromIndex(name(), dataPath(), indexFileNames(), Map.of(key, value));
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
     */
    default boolean delete(final Set<K> keys) throws IOException {
        Logger.info("Deleting multiple values from {} using keys {} at {}.", name(), keys, dataPath());
        final var db = getDb();
        Map<K, V> updatedMap = new HashMap<>();
        var updated = false;

        if (containsIndexes()) {
            updatedMap = get(keys);
        }

        try {
            updated = db.keySet().removeAll(keys);
        } finally {
            db.close();
        }

        if (updated) {
            Logger.info("Objects with keys {} deleted from {} at {}.", keys, name(), dataPath());
            CHRONICLE_UTILS.removeFromIndex(name(), dataPath(), indexFileNames(), updatedMap);
        }

        return updated;
    }

    private long getCurrentEntrySize() throws NumberFormatException, IOException {
        return Long.valueOf(Files.readString(Path.of(dataPath() + DATA_DIR + ENTRY_SIZE_FILE)));
    }

    default void setCurrentEntrySize() throws IOException {
        final var path = Path.of(dataPath() + DATA_DIR + ENTRY_SIZE_FILE);

        if (!Files.exists(path)) {
            final var db = getDb();
            final long divided = (db.size() / entries());
            db.close();
            final long currentEntrySize = entries() * (divided == 0 ? 1 : divided);
            try {
                Files.createFile(path);
                Files.writeString(path, String.valueOf(currentEntrySize));
            } catch (final IOException e) {
                Logger.error("Could not create entry size file for {}.", dataPath());
            }
        }
    }

    private long getCurrentEntrySize(final ChronicleMap<K, V> db) {
        final var path = Path.of(dataPath() + DATA_DIR + ENTRY_SIZE_FILE);
        final long divided = (db.size() / entries());
        final long currentEntrySize = entries() * (divided == 0 ? 1 : divided);

        if (!Files.exists(path)) {
            try {
                Files.createFile(path);
                Files.writeString(path, String.valueOf(currentEntrySize));
            } catch (final IOException e) {
                Logger.error("Could not create entry size file for {}.", dataPath());
                return currentEntrySize;
            }
        }

        try {
            return Long.valueOf(Files.readString(path));
        } catch (NumberFormatException | IOException e) {
            Logger.error("Could not read entry size file for {}.", dataPath());
            try {
                Files.writeString(path, String.valueOf(currentEntrySize));
            } catch (final IOException e1) {
            }
            return currentEntrySize;
        }
    }

    private void writeCurrentEntrySize(final long size) {
        final var path = Path.of(dataPath() + DATA_DIR + ENTRY_SIZE_FILE);
        try {
            Files.writeString(path, String.valueOf(size));
        } catch (final IOException e) {
            Logger.error("Could not write current entry size for {}.", dataPath());
        }
    }

    /**
     * Create a bigger file if size >= currentEntrySize
     * 
     * @return
     * @throws IOException
     */
    default void resizeDb() throws IOException {
        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        final var db = getDb();

        synchronized (lock) {
            final long currentEntrySize = getCurrentEntrySize(db);
            Logger.info("Checking entry size on db {}.", dataPath());
            if (db.size() >= currentEntrySize) {
                final var newSize = currentEntrySize + entries();
                Logger.info("Increasing entry size on db {} from {} to {}.", dataPath(), currentEntrySize, newSize);
                final var dataFilePath = dataPath() + DATA_DIR + DATA_FILE;
                final var backupDataFilePath = dataPath() + BACKUP_DIR + DATA_FILE;
                final var tempDataFilePath = dataPath() + DATA_DIR + "data.tmp";
                writeCurrentEntrySize(newSize);
                final var newDb = CHRONICLE_DB.getDb(name(), newSize, averageKey(), averageValue(), tempDataFilePath,
                        bloatFactor());
                newDb.putAll(db);
                db.close();
                Files.move(Path.of(dataFilePath), Path.of(backupDataFilePath), REPLACE_EXISTING);
                Files.move(Path.of(tempDataFilePath), Path.of(dataFilePath), REPLACE_EXISTING);
                newDb.close();
            } else {
                db.close();
            }
        }
    }

    /**
     * Add/Update a value
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
        final var db = getDb();
        V prevValue = null;
        try {
            prevValue = db.put(key, value);
        } finally {
            db.close();
        }

        final var prevValueMap = new HashMap<K, V>(1);
        if (prevValue != null) {
            prevValueMap.put(key, prevValue);
            status = PutStatus.UPDATED;
        }

        CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames, Map.of(key, value), prevValueMap);

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
     * Update a value without bothering about db creation
     * 
     * @param key   the key
     * @param value the value
     * @return true if updated else false
     * @throws IOException
     */
    default PutStatus update(final K key, final V value, final List<String> indexFileNames)
            throws IOException {
        final var db = getDb();
        V prevValue = null;
        try {
            prevValue = db.put(key, value);
        } finally {
            db.close();
        }

        final var prevValueMap = new HashMap<K, V>(1);
        prevValueMap.put(key, prevValue);
        CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames, Map.of(key, value), prevValueMap);
        Logger.info("UPDATED into {} using key {} at {}.", name(), key, dataPath());

        return PutStatus.UPDATED;
    }

    /**
     * Refer to method above
     * 
     */
    default PutStatus update(final K key, final V value) throws IOException {
        return update(key, value, indexFileNames());
    }

    /**
     * Add/Update multiple values into the db, then update all indexes related
     * 
     * @param map the map to add
     * @throws IOException
     */
    default PutStatus put(final Map<K, V> map) throws IOException {
        if (map.size() > entries()) {
            Logger.error("Insert size bigger than entry size.");
            return PutStatus.FAILED;
        }

        Logger.info("Inserting multiple values into {} at {}.", name(), dataPath());
        final var db = getDb();
        final var prevValues = new HashMap<K, V>(map.size());

        try {
            for (final var entry : map.entrySet()) {
                final K key = entry.getKey();
                if (db.containsKey(key)) {
                    prevValues.put(key, db.get(key));
                }
            }
            db.putAll(map);
        } finally {
            db.close();
        }

        CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames(), map, prevValues);

        return PutStatus.INSERTED;
    }

    default PutStatus put(final Map<K, V> map, final Map<K, V> prevValues) throws IOException {
        if (map.size() > entries()) {
            Logger.error("Insert size bigger than entry size.");
            return PutStatus.FAILED;
        }

        Logger.info("Inserting multiple values into {} at {}.", name(), dataPath());
        final var db = getDb();

        try {
            db.putAll(map);
        } finally {
            db.close();
        }

        CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames(), map, prevValues);

        return PutStatus.INSERTED;
    }

    /**
     * Update multiple values into the db, then update all indexes related
     * This is useful as it does not increase db size
     * 
     * @param map the map to add
     * @throws IOException
     */
    default PutStatus update(final Map<K, V> map) throws IOException {
        if (map.size() > entries()) {
            Logger.error("Update size bigger than entry size.");
            return PutStatus.FAILED;
        }

        Logger.info("Updating multiple values into {} at {}.", name(), dataPath());
        final var db = getDb();
        final var prevValues = new HashMap<K, V>(map.size());

        try {
            for (final var entry : map.entrySet()) {
                final K key = entry.getKey();
                prevValues.put(key, db.get(key));
            }
            db.putAll(map);
        } finally {
            db.close();
        }

        CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames(), map, prevValues);

        return PutStatus.UPDATED;
    }

    /**
     * Add/Update multiple values into the db with no indexing
     * 
     * @param map the map to add
     * @throws IOException
     */
    default PutStatus putAll(final Map<K, V> map) throws IOException {
        if (map.size() > entries()) {
            Logger.error("Insert size bigger than entry size.");
            return PutStatus.FAILED;
        }

        Logger.info("Inserting multiple values into {} at {}.", name(), dataPath());
        final var db = getDb();

        try {
            db.putAll(map);
        } finally {
            db.close();
        }

        return PutStatus.INSERTED;
    }

    /**
     * Migrate all records
     * 
     * @param map the map to add
     * @throws IOException
     */
    default void migrate(final Map<K, V> map) throws IOException {
        Logger.info("Migrating values into {} at {}.", name(), dataPath());
        final var db = getDb();

        try {
            db.putAll(map);
        } finally {
            db.close();
        }
    }

    /**
     * Search the chronicle map based on values
     * 
     * @param db     the map
     * @param search object search
     * @return a map of the fitting values
     */
    default Map<K, V> search(final Map<K, V> db, final Search search) {
        Logger.info("Searching DB at {} for {}.", dataPath(), search);
        final Map<K, V> map = new HashMap<>();

        for (final var entry : db.entrySet()) {
            try {
                CHRONICLE_UTILS.search(search, entry.getKey(), entry.getValue(), map);
            } catch (IllegalAccessException | NoSuchFieldException e) {
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
     */
    default Map<K, V> search(final Map<K, V> db, final Search search, final int limit) {
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
        db.close();
        return map;
    }

    /**
     * Search the chronicle map based on values
     * 
     * @param search object search
     * @return a map of the fitting values
     * @throws IOException
     */
    default Map<K, V> search(final Search search, final int limit) throws IOException {
        Logger.info("Searching DB at {} for {} with limit {}.", dataPath(), search, limit);
        final var db = getDb();
        final Map<K, V> map = search(db, search, limit);
        db.close();
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
     */
    @SuppressWarnings("unchecked")
    private Map<K, V> indexedSearch(final Search search, final Map<K, V> db, final Map<Object, List<K>> index) {
        Logger.info("Index searching DB at {} for {}.", dataPath(), search);
        if (index == null || index.isEmpty())
            return new HashMap<>();

        final Map<K, V> match = new HashMap<>();
        final List<K> keys = new ArrayList<>();
        final SearchType searchType = search.searchType();
        final Class<?> fieldClass = index.keySet().stream().filter(Objects::nonNull).findFirst()
                .map(Object::getClass).orElse(null);
        if (fieldClass == null)
            return match;

        final Object searchTerm = CHRONICLE_UTILS.setSearchTerm(search.searchTerm(), fieldClass);
        final List<Object> searchTermList = (searchType == SearchType.IN || searchType == SearchType.NOT_IN)
                ? CHRONICLE_UTILS.setSearchTerm((List<Object>) search.searchTerm(), fieldClass)
                : null;

        switch (searchType) {
            case EQUAL -> addSearchedValues(index.get(searchTerm), db, match);
            case NOT_EQUAL -> {
                for (final var entry : index.entrySet()) {
                    if (!entry.getKey().equals(searchTerm)) {
                        addSearchedValues(entry.getValue(), db, match);
                    }
                }
            }
            case LESS -> {
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) < 0) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
            case GREATER -> {
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) > 0) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
            case LESS_OR_EQUAL -> {
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) <= 0) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
            case GREATER_OR_EQUAL -> {
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) >= 0) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
            case LIKE -> {
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.containsIgnoreCase(entry.getKey(), searchTerm)) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
            case NOT_LIKE -> {
                for (final var entry : index.entrySet()) {
                    if (!CHRONICLE_UTILS.containsIgnoreCase(entry.getKey(), searchTerm)) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
            case CONTAINS -> {
                for (final var entry : index.entrySet()) {
                    if (Collections.singleton(entry.getKey()).contains(searchTerm)) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
            case NOT_CONTAINS -> {
                for (final var entry : index.entrySet()) {
                    if (!Collections.singleton(entry.getKey()).contains(searchTerm)) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
            case STARTS_WITH -> {
                for (final var entry : index.entrySet()) {
                    if (String.valueOf(entry.getKey()).startsWith(String.valueOf(searchTerm))) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
            case ENDS_WITH -> {
                for (final var entry : index.entrySet()) {
                    if (String.valueOf(entry.getKey()).endsWith(String.valueOf(searchTerm))) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
            case IN -> {
                for (final var entry : index.entrySet()) {
                    if (searchTermList.contains(entry.getKey())) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
            case NOT_IN -> {
                for (final var entry : index.entrySet()) {
                    if (!searchTermList.contains(entry.getKey())) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
        }

        if (!keys.isEmpty())
            addSearchedValues(keys, db, match);
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
     */
    @SuppressWarnings("unchecked")
    private Map<K, V> indexedSearch(final Search search, final Map<K, V> db, final Map<Object, List<K>> index,
            final int limit) {
        Logger.info("Index searching DB at {} for {} with limit {}.", dataPath(), search, limit);
        if (index == null || index.isEmpty())
            return new HashMap<>();

        final Map<K, V> match = new HashMap<>();
        final List<K> keys = new ArrayList<>();
        final SearchType searchType = search.searchType();
        final Class<?> fieldClass = index.keySet().stream().filter(Objects::nonNull).findFirst()
                .map(Object::getClass).orElse(null);
        if (fieldClass == null)
            return match;

        final Object searchTerm = CHRONICLE_UTILS.setSearchTerm(search.searchTerm(), fieldClass);
        final List<Object> searchTermList = (searchType == SearchType.IN || searchType == SearchType.NOT_IN)
                ? CHRONICLE_UTILS.setSearchTerm((List<Object>) search.searchTerm(), fieldClass)
                : null;

        switch (searchType) {
            case EQUAL -> addSearchedValues(index.get(searchTerm), db, match, limit);
            case NOT_EQUAL -> {
                for (final var entry : index.entrySet()) {
                    if (!entry.getKey().equals(searchTerm)) {
                        addSearchedValues(entry.getValue(), db, match, limit);
                    }
                }
            }
            case LESS -> {
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) < 0) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
            case GREATER -> {
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) > 0) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
            case LESS_OR_EQUAL -> {
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) <= 0) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
            case GREATER_OR_EQUAL -> {
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) >= 0) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
            case LIKE -> {
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.containsIgnoreCase(entry.getKey(), searchTerm)) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
            case NOT_LIKE -> {
                for (final var entry : index.entrySet()) {
                    if (!CHRONICLE_UTILS.containsIgnoreCase(entry.getKey(), searchTerm)) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
            case CONTAINS -> {
                for (final var entry : index.entrySet()) {
                    if (Collections.singleton(entry.getKey()).contains(searchTerm)) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
            case NOT_CONTAINS -> {
                for (final var entry : index.entrySet()) {
                    if (!Collections.singleton(entry.getKey()).contains(searchTerm)) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
            case STARTS_WITH -> {
                for (final var entry : index.entrySet()) {
                    if (String.valueOf(entry.getKey()).startsWith(String.valueOf(searchTerm))) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
            case ENDS_WITH -> {
                for (final var entry : index.entrySet()) {
                    if (String.valueOf(entry.getKey()).endsWith(String.valueOf(searchTerm))) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
            case IN -> {
                for (final var entry : index.entrySet()) {
                    if (searchTermList.contains(entry.getKey())) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
            case NOT_IN -> {
                for (final var entry : index.entrySet()) {
                    if (!searchTermList.contains(entry.getKey())) {
                        keys.addAll(entry.getValue());
                    }
                }
            }
        }

        if (!keys.isEmpty())
            addSearchedValues(keys, db, match, limit);
        return match;
    }

    default Map<K, V> indexedSearch(final Search search) throws IOException {
        final var indexFilePath = getIndexPath(search.field());
        final var db = getDb();

        final HTreeMap<Object, List<K>> indexDb = MAP_DB.getDb(indexFilePath, true);
        try {
            return indexedSearch(search, db, indexDb);
        } finally {
            db.close();
            MAP_DB.close(indexFilePath);
        }
    }

    default Map<K, V> indexedSearch(final Search search, final int limit) throws IOException {
        final var indexFilePath = getIndexPath(search.field());
        final var db = getDb();

        final HTreeMap<Object, List<K>> indexDb = MAP_DB.getDb(indexFilePath, true);
        try {
            return indexedSearch(search, db, indexDb, limit);
        } finally {
            db.close();
            MAP_DB.close(indexFilePath);
        }
    }

    default Map<K, V> indexedSearch(final Map<K, V> db, final Search search) {
        final var indexFilePath = getIndexPath(search.field());
        final HTreeMap<Object, List<K>> indexDb = MAP_DB.getDb(indexFilePath, true);

        try {
            return indexedSearch(search, db, indexDb);
        } finally {
            MAP_DB.close(indexFilePath);
        }
    }

    default Map<K, V> indexedSearch(final Map<K, V> db, final Search search, final int limit) {
        final var indexFilePath = getIndexPath(search.field());
        final HTreeMap<Object, List<K>> indexDb = MAP_DB.getDb(indexFilePath, true);

        try {
            return indexedSearch(search, db, indexDb, limit);
        } finally {
            MAP_DB.close(indexFilePath);
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
        db.close();
        return size;
    }

    default void clearDb() throws IOException {
        Logger.info("Truncating database at {}.", dataPath());
        final var db = getDb();
        db.clear();
        db.close();
    }

    default void deleteDataFiles() throws IOException {
        CHRONICLE_UTILS.deleteFileIfExists(dataPath() + DATA_DIR + DATA_FILE);
    }

    default boolean exists(final K key) throws IOException {
        final var db = getDb();
        final var exists = db.containsKey(key);
        db.close();
        return exists;
    }
}
