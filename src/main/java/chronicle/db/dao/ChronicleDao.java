package chronicle.db.dao;

import static chronicle.db.dao.ChronicleUtils.CHRONICLE_UTILS;
import static chronicle.db.service.ChronicleDb.CHRONICLE_DB;
import static chronicle.db.service.MapDb.MAP_DB;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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
@SuppressWarnings("unchecked")
public interface ChronicleDao<K, V> {
    ConcurrentMap<String, Object> LOCKS = new ConcurrentHashMap<>();
    ConcurrentMap<String, Set<String>> DATA_FILE_CACHE = new ConcurrentHashMap<>();
    ConcurrentMap<String, HTreeMap<?, String>> KEY_MAP_CACHE = new ConcurrentHashMap<>();
    String DATA_DIR = "/data/", INDEX_DIR = "/indexes/", FILES_DIR = "/files/", BACKUP_DIR = "/backup/",
            DATA_FILE = "data", CORRUPTED_FILE = "corrupted", RECOVER_FILE = "recovery", ENTRY_SIZE_FILE = "entrySize",
            KEY_FILE = "keys";
    String[] DB_DIRS = { DATA_DIR, INDEX_DIR, FILES_DIR, BACKUP_DIR };

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

    /**
     * If an object needs indexes, use this to declare.
     */
    default List<String> indexFileNames() throws IOException {
        return Collections.emptyList();
    }

    private void createDataDirs(final String dataPath) {
        if (!Files.exists(Path.of(dataPath))) {
            for (final String dir : DB_DIRS) {
                try {
                    Files.createDirectories(Path.of(dataPath + dir));
                } catch (final IOException e) {
                    Logger.error("Error on db directory creation for {}. {}.", dataPath, e.getMessage());
                }
            }
        }
    }

    private void populateKeyMap(final Set<String> dataFiles, final HTreeMap<K, String> keyMap) throws IOException {
        for (final String file : dataFiles) {
            try (final var db = getDb(file)) {
                for (final K key : db.keySet()) {
                    keyMap.put(key, file);
                }
            }
        }
    }

    /**
     * Create the folders required on init
     *
     */
    default void createDataDirs() {
        createDataDirs(dataPath());
        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            if (!KEY_MAP_CACHE.containsKey(dataPath())) {
                final var dataFiles = getDataFiles();
                if (dataFiles.size() > 1) {
                    final HTreeMap<K, String> keyMap = MAP_DB.getMemoryDirectDb();
                    try {
                        populateKeyMap(dataFiles, keyMap);
                    } catch (final IOException e) {
                    }
                    KEY_MAP_CACHE.put(dataPath(), keyMap);
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
     * Delete and rerun all indexes. Faster when inserting a lot of records.
     * 
     * @throws IOException
     */
    default List<String> deleteIndexes() throws IOException {
        final var available = indexFileNames();

        available.forEach(f -> {
            CHRONICLE_UTILS.deleteFileIfExists(dataPath() + INDEX_DIR + f);
        });

        return available;
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

    private ChronicleMap<K, V> getDb(final String fileName) throws IOException {
        return CHRONICLE_DB.getDb(name(), entries(), averageKey(), averageValue(), dataPath() + DATA_DIR + fileName,
                bloatFactor());
    }

    /**
     * Cache to store data file names
     */
    private Set<String> getDataFiles() {
        return DATA_FILE_CACHE.computeIfAbsent(dataPath(), k -> {
            try {
                final Set<String> dataFiles = CHRONICLE_UTILS.getFileList(dataPath() + DATA_DIR).stream()
                        .filter(file -> file.startsWith("data"))
                        .collect(Collectors.toSet());
                if (dataFiles.isEmpty()) {
                    final var defaultSet = new HashSet<String>();
                    defaultSet.add("data");
                    return defaultSet;
                }
                return dataFiles;
            } catch (final IOException e) {
                Logger.error("Failed to initialize data file cache for {}. {}", dataPath(), e.getMessage());
                return new HashSet<>();
            }
        });
    }

    /**
     * Only runs to initialize an index on the field first time
     * 
     * @param field the field of the V value object
     * @throws IOException
     * 
     */
    private void initIndex(final Map<K, V> db, final List<String> fields, final String indexDirPath)
            throws IOException {
        CHRONICLE_UTILS.index(db, name(), fields, dataPath(), indexDirPath);
    }

    /**
     * Only runs to initialize an index on the field first time
     * 
     * @param field the field of the V value object
     * @throws IOException
     * 
     */
    private void initIndex(final List<String> fields) throws IOException {
        for (final String file : getDataFiles()) {
            try (final var db = getDb(file)) {
                initIndex(db, fields, dataPath() + INDEX_DIR);
            }
        }
    }

    /**
     * Delete and rerun all indexes. Faster when inserting a lot of records.
     * 
     * @throws IOException
     */
    default void refreshIndexes() throws IOException {
        final var indexFiles = indexFileNames();
        if (!indexFiles.isEmpty()) {
            Logger.info("Re-initializing indexes at [{}].", dataPath());
            for (final String field : indexFiles) {
                CHRONICLE_UTILS.deleteFileIfExists(getIndexPath(field));
            }
            initIndex(indexFiles);
        }
    }

    default List<String> availableIndexes() throws IOException {
        return CHRONICLE_UTILS.getFileList(dataPath() + INDEX_DIR);
    }

    /**
     * Initialize indexes at dao creation
     * 
     * @param fields
     * @throws IOException
     */
    default void initDefaultIndexes() throws IOException {
        if (!getDataFiles().isEmpty()) {
            final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());

            synchronized (lock) {
                final var availableIndexes = availableIndexes();
                final var indexFileNames = indexFileNames();
                if (availableIndexes.size() != indexFileNames.size()) {
                    // Find items in indexFileNames not in availableIndexes
                    final List<String> missingIndexes = new ArrayList<>(indexFileNames);
                    missingIndexes.removeAll(availableIndexes);

                    if (!missingIndexes.isEmpty()) {
                        initIndex(missingIndexes);
                    }
                }
            }
        }
    }

    /**
     * In cases onf data corruption, we can recover the db using this method
     */
    default void recoverData(final String dataFileName) throws IOException {
        final var dataFileStr = dataPath() + DATA_DIR + dataFileName;
        final var db = CHRONICLE_DB.recoverDb(name(), entries(), averageKey(), averageValue(),
                dataFileStr, bloatFactor());
        final var dbRecovery = getDb(RECOVER_FILE);
        dbRecovery.putAll(db);
        db.close();
        dbRecovery.close();
        final var dataFilePath = Path.of(dataFileStr);
        Files.move(dataFilePath, Path.of(dataFileStr + "." + CORRUPTED_FILE), REPLACE_EXISTING);
        Files.move(Path.of(dataPath() + DATA_DIR + RECOVER_FILE), dataFilePath, REPLACE_EXISTING);
    }

    /**
     * Fetches all records in the db, never run directly for huge files
     * 
     * @return Map<K, V>
     * @throws IOException
     */
    default Map<K, V> fetch() throws IOException {
        Logger.info("Fetching all data at [{}].", dataPath());
        final Map<K, V> result = new HashMap<>();
        for (final String file : getDataFiles()) {
            try (final var db = getDb(file)) {
                result.putAll(db);
            }
        }
        return result;
    }

    private String getDbFile(final K key, final HTreeMap<?, String> keyMap) {
        if (keyMap == null) {
            return DATA_FILE;
        }
        final var file = keyMap.get(key);
        if (file == null) {
            return DATA_FILE;
        }

        return file;
    }

    private Map<String, Set<K>> getDbFiles(final Set<K> keys, final HTreeMap<?, String> keyMap) {
        if (keyMap == null) {
            return Map.of(DATA_FILE, keys);
        }

        final var fileMap = new HashMap<String, Set<K>>();
        for (final K k : keys) {
            final var file = keyMap.get(k);
            if (file != null) {
                fileMap.computeIfAbsent(file, f -> new HashSet<>()).add(k);
            }
        }
        return fileMap;
    }

    /**
     * Get a value using key
     * 
     * @param key the key to search in the db
     * @return V value
     * @throws IOException
     */
    default V get(final K key) throws IOException {
        if (key == null) {
            return null;
        }
        final var file = getDbFile(key, KEY_MAP_CACHE.get(dataPath()));
        Logger.info("Querying key [{}] at [{}].", key, dataPath());

        try (final var db = getDb(file)) {
            return db.getUsing(key, using());
        }
    }

    /**
     * Get all the values for a list of keys
     * 
     * @param keys list of keys
     * @return Map<K, V> values
     * @throws IOException
     */
    default Map<K, V> get(final Set<K> keys) throws IOException {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyMap();
        }
        Logger.info("Querying {} keys at [{}].", keys.size(), dataPath());
        final var map = new HashMap<K, V>(keys.size());

        final var keyMap = KEY_MAP_CACHE.get(dataPath());
        if (keyMap == null) {
            try (final var db = getDb()) {
                for (final K key : keys) {
                    final V value = db.getUsing(key, using());
                    if (value != null) {
                        map.put(key, value);
                    }
                }
            }
            return map;
        }

        final var dbFiles = getDbFiles(keys, keyMap);
        for (final var entry : dbFiles.entrySet()) {
            try (final var db = getDb(entry.getKey())) {
                for (final K key : entry.getValue()) {
                    map.put(key, db.getUsing(key, using()));
                }
            }
        }

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
        if (key == null) {
            return false;
        }

        final var keyMap = KEY_MAP_CACHE.get(dataPath());
        final var file = getDbFile(key, keyMap);
        Logger.info("Deleting key [{}] at [{}].", key, dataPath());
        final var keyLock = LOCKS.computeIfAbsent(dataPath() + key, k -> new Object());

        V value = null;
        synchronized (keyLock) {
            try (final var db = getDb(file)) {
                value = db.remove(key);
            }

            if (value == null) {
                return false;
            }

            if (keyMap != null) {
                keyMap.remove(key);
            }
            final var indexFileNames = indexFileNames();
            CHRONICLE_UTILS.removeFromIndex(name(), dataPath(), indexFileNames, Map.of(key, value));
            return true;
        }
    }

    private void removeFromIndex(final Map<K, V> deletedMap) throws IOException {
        Logger.info("{} record(s) deleted at [{}].", deletedMap.size(), dataPath());
        CHRONICLE_UTILS.removeFromIndex(name(), dataPath(), indexFileNames(), deletedMap);
    }

    /**
     * Remove a value using a list of keys
     * 
     * @param keys the keys to remove
     * @return true if updated else false
     * @throws IOException
     */
    default boolean delete(final Set<K> keys) throws IOException {
        if (keys == null || keys.isEmpty()) {
            return false;
        }

        Logger.info("Deleting {} keys at [{}].", keys.size(), dataPath());
        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            final var deletedMap = new HashMap<K, V>();
            final var keyMap = KEY_MAP_CACHE.get(dataPath());
            if (keyMap == null) {
                try (final var db = getDb()) {
                    for (final K key : keys) {
                        final var deleted = db.remove(key);
                        if (deleted != null) {
                            deletedMap.put(key, deleted);
                        }
                    }
                }

                if (deletedMap.isEmpty()) {
                    return false;
                }
                removeFromIndex(deletedMap);
                return true;
            }

            final var dbFiles = getDbFiles(keys, keyMap);
            if (!dbFiles.isEmpty()) {
                for (final var entry : dbFiles.entrySet()) {
                    try (final var db = getDb(entry.getKey())) {
                        for (final K key : entry.getValue()) {
                            final var deleted = db.remove(key);
                            if (deleted != null) {
                                keyMap.remove(key);
                                deletedMap.put(key, deleted);
                            }
                        }
                    }
                }
            }

            if (deletedMap.isEmpty()) {
                return false;
            }
            removeFromIndex(deletedMap);
            return true;
        }
    }

    default void resizeDb(final long newSize) throws IOException {
        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            try (final var db = getDb()) {
                final long currentEntrySize = db.size();
                if (newSize <= currentEntrySize) {
                    Logger.warn("New size {} is not larger than current size {} at [{}]. Skipping resize.",
                            newSize, currentEntrySize, dataPath());
                    return;
                }
                final var dataFilePath = dataPath() + DATA_DIR + DATA_FILE;
                final var backupDataFilePath = dataPath() + BACKUP_DIR + DATA_FILE;
                final var tempDataFilePath = dataPath() + DATA_DIR + "data.tmp";
                try (final var newDb = CHRONICLE_DB.getDb(name(), newSize, averageKey(), averageValue(),
                        tempDataFilePath, bloatFactor())) {
                    newDb.putAll(db);
                }
                Files.move(Path.of(dataFilePath), Path.of(backupDataFilePath), REPLACE_EXISTING);
                Files.move(Path.of(tempDataFilePath), Path.of(dataFilePath), REPLACE_EXISTING);
                Logger.info("Resized DB at [{}] from {} to {}.", dataPath(), currentEntrySize, newSize);
            }
        }
    }

    /**
     * Rotate files and keep the data file as latest
     * 
     * @throws IOException
     */
    private void rotateFile(final HTreeMap<K, String> keyMap) throws IOException {
        final String rotatedFile = "data-" + System.currentTimeMillis();
        final var currentPath = Path.of(dataPath() + DATA_DIR + DATA_FILE);
        final var rotatedPath = Path.of(dataPath() + DATA_DIR + rotatedFile);
        Files.move(currentPath, rotatedPath, REPLACE_EXISTING);

        try (final var oldDb = getDb(rotatedFile)) {
            for (final K oldKey : oldDb.keySet()) {
                keyMap.put(oldKey, rotatedFile);
            }
        }

        final var currentFiles = getDataFiles();
        currentFiles.add(rotatedFile);
        DATA_FILE_CACHE.put(dataPath(), currentFiles);

        Logger.info("Rotated data file at [{}] to {}.", dataPath(), rotatedFile);
    }

    /**
     * First time rotation when keyMap is null
     */
    private HTreeMap<K, String> rotateFile() throws IOException {
        final String rotatedFile = "data-" + System.currentTimeMillis();
        final var currentPath = Path.of(dataPath() + DATA_DIR + DATA_FILE);
        final var rotatedPath = Path.of(dataPath() + DATA_DIR + rotatedFile);
        Files.move(currentPath, rotatedPath, REPLACE_EXISTING);
        final HTreeMap<K, String> keyMap = MAP_DB.getMemoryDirectDb();
        KEY_MAP_CACHE.put(dataPath(), keyMap);

        try (final var oldDb = getDb(rotatedFile)) {
            for (final K oldKey : oldDb.keySet()) {
                keyMap.put(oldKey, rotatedFile);
            }
        }

        final var currentFiles = getDataFiles();
        currentFiles.add(rotatedFile);
        DATA_FILE_CACHE.put(dataPath(), currentFiles);

        Logger.info("Rotated data file at [{}] to {}.", dataPath(), rotatedFile);

        return keyMap;
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
        if (key == null) {
            return PutStatus.FAILED;
        }

        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            var keyMap = (HTreeMap<K, String>) KEY_MAP_CACHE.get(dataPath());
            var file = getDbFile(key, keyMap);
            var db = getDb(file);
            V prevValue = null;

            try {
                if (DATA_FILE.equals(file)) {
                    if (db.size() >= entries()) {
                        db.close();
                        if (keyMap == null) {
                            keyMap = rotateFile();
                        } else {
                            rotateFile(keyMap);
                        }
                        db = getDb();
                        file = DATA_FILE;
                    }
                }
                prevValue = db.put(key, value);
            } finally {
                db.close();
            }
            var status = PutStatus.INSERTED;
            if (prevValue != null) {
                CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames, Map.of(key, value),
                        Map.of(key, prevValue));
                status = PutStatus.UPDATED;
            } else {
                if (keyMap != null)
                    keyMap.put(key, file);
                CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames, Map.of(key, value),
                        Collections.emptyMap());
            }
            Logger.info("Put status [{}] using key [{}] at [{}].", status, key, dataPath());
            return status;
        }
    }

    /**
     * Refer to method above
     * 
     */
    default PutStatus put(final K key, final V value) throws IOException {
        return put(key, value, indexFileNames());
    }

    /**
     * Update a value without bothering about db creation, only use for updates
     * 
     * @param key   the key
     * @param value the value
     * @return true if updated else false
     * @throws IOException
     */
    default PutStatus update(final K key, final V value, final List<String> indexFileNames)
            throws IOException {
        if (key == null) {
            return PutStatus.FAILED;
        }

        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            final var file = getDbFile(key, KEY_MAP_CACHE.get(dataPath()));
            var status = PutStatus.FAILED;
            V prevValue = null;

            try (final var db = getDb(file)) {
                if (db.containsKey(key)) {
                    status = PutStatus.UPDATED;
                    prevValue = db.put(key, value);
                }
            }

            if (status == PutStatus.UPDATED) {
                CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames, Map.of(key, value),
                        Map.of(key, prevValue));
            }
            Logger.info("Update status [{}] using key [{}] at [{}].", status, key, dataPath());

            return status;
        }
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
        if (map == null || map.isEmpty()) {
            return PutStatus.FAILED;
        }

        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            final var prevValues = new HashMap<K, V>(map.size());

            // update old records first then only move to new record inserts.
            var keyMap = (HTreeMap<K, String>) KEY_MAP_CACHE.get(dataPath());
            final var dbFiles = getDbFiles(map.keySet(), keyMap);

            for (final var entry : dbFiles.entrySet()) {
                try (final var db = getDb(entry.getKey())) {
                    for (final K key : entry.getValue()) {
                        prevValues.put(key, db.put(key, map.get(key)));
                    }
                }
            }

            final var status = !prevValues.isEmpty() ? PutStatus.UPDATED : PutStatus.INSERTED;
            final var indexCopyMap = new HashMap<>(map);
            // now do inserts after removing the updating keys
            map.keySet().removeAll(prevValues.keySet());

            if (!map.isEmpty()) {
                var db = getDb();
                try {
                    for (final var entry : map.entrySet()) {
                        final K key = entry.getKey();
                        final V value = entry.getValue();
                        if (db.size() >= entries()) {
                            db.close();
                            if (keyMap == null) {
                                keyMap = rotateFile();
                            } else {
                                rotateFile(keyMap);
                            }
                            db = getDb();
                        }
                        db.put(key, value);
                        if (keyMap != null) {
                            keyMap.put(key, DATA_FILE);
                        }
                    }
                } finally {
                    db.close();
                }
            }

            CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames(), indexCopyMap, prevValues);
            Logger.info("Put {} records at [{}].", map.size(), dataPath());

            return status;
        }
    }

    /**
     * Update multiple values into the db, then update all indexes related
     * This is useful as it does not increase db size. Never run with non existent
     * keys
     * it wont insert
     * 
     * @param map the map to add
     * @throws IOException
     */
    default PutStatus update(final Map<K, V> map) throws IOException {
        if (map == null || map.isEmpty()) {
            return PutStatus.FAILED;
        }

        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            final var keyMap = (HTreeMap<K, String>) KEY_MAP_CACHE.get(dataPath());
            final var dbFiles = getDbFiles(map.keySet(), keyMap);
            final var prevValues = new HashMap<K, V>(map.size());

            for (final var entry : dbFiles.entrySet()) {
                try (final var db = getDb(entry.getKey())) {
                    for (final K key : entry.getValue()) {
                        if (db.containsKey(key)) {
                            prevValues.put(key, db.put(key, map.get(key)));
                        }
                    }
                }
            }

            if (prevValues.size() != map.size()) {
                Logger.error("Update map contains {} new or missing keys, expected all existing records at [{}].",
                        map.size() - prevValues.size(), dataPath());
                return PutStatus.FAILED;
            }

            CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames(), map, prevValues);
            Logger.info("Update {} records at [{}].", prevValues.size(), dataPath());
            return PutStatus.UPDATED;
        }
    }

    /**
     * Add/Update multiple values into the db with no indexing
     * 
     * @param map the map to add
     * @throws IOException
     */
    default PutStatus putAll(final Map<K, V> map) throws IOException {
        if (map == null || map.isEmpty()) {
            return PutStatus.FAILED;
        }

        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            final var prevValues = new HashMap<K, V>(map.size());

            // update old records first then only move to new record inserts.
            var keyMap = (HTreeMap<K, String>) KEY_MAP_CACHE.get(dataPath());
            final var dbFiles = getDbFiles(map.keySet(), keyMap);

            for (final var entry : dbFiles.entrySet()) {
                try (final var db = getDb(entry.getKey())) {
                    for (final K key : entry.getValue()) {
                        prevValues.put(key, db.put(key, map.get(key)));
                    }
                }
            }

            final var status = !prevValues.isEmpty() ? PutStatus.UPDATED : PutStatus.INSERTED;
            // now do inserts after removing the updating keys
            map.keySet().removeAll(prevValues.keySet());

            if (!map.isEmpty()) {
                var db = getDb();
                try {
                    for (final var entry : map.entrySet()) {
                        final K key = entry.getKey();
                        final V value = entry.getValue();
                        if (db.size() >= entries()) {
                            db.close();
                            if (keyMap == null) {
                                keyMap = rotateFile();
                            } else {
                                rotateFile(keyMap);
                            }
                            db = getDb();
                        }
                        db.put(key, value);
                        if (keyMap != null) {
                            keyMap.put(key, DATA_FILE);
                        }
                    }
                } finally {
                    db.close();
                }
            }
            Logger.info("Put {} records at [{}].", map.size(), dataPath());

            return status;
        }
    }

    /**
     * Search the chronicle map based on values
     * 
     * @param db     the map
     * @param search object search
     * @return a map of the fitting values
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    default Map<K, V> search(final Map<K, V> db, final Search search)
            throws IllegalArgumentException, IllegalAccessException {
        Logger.info("Searching DB at {} for {}.", dataPath(), search);
        final Map<K, V> map = new HashMap<>();

        for (final var entry : db.entrySet()) {
            CHRONICLE_UTILS.search(search, entry.getKey(), entry.getValue(), map);
        }

        return map;
    }

    /**
     * Search the chronicle map based on values
     * 
     * @param search object search
     * @return a map of the fitting values
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    default Map<K, V> search(final Map<K, V> db, final Search search, final int limit)
            throws IllegalArgumentException, IllegalAccessException {
        Logger.info("Searching DB at {} for {} with limit {}.", dataPath(), search, limit);
        final Map<K, V> map = new HashMap<>();

        for (final var entry : db.entrySet()) {
            CHRONICLE_UTILS.search(search, entry.getKey(), entry.getValue(), map);
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
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    default Map<K, V> search(final Search search) throws IOException, IllegalArgumentException, IllegalAccessException {
        Logger.info("Searching DB at [{}] for {}.", dataPath(), search);
        final Map<K, V> results = new HashMap<>();
        final var files = getDataFiles();

        for (final String file : files) {
            try (final var db = getDb(file)) {
                results.putAll(search(db, search));
            }
        }

        return results;
    }

    /**
     * Search the chronicle map based on values
     * 
     * @param search object search
     * @return a map of the fitting values
     * @throws IOException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    default Map<K, V> search(final Search search, final int limit)
            throws IOException, IllegalArgumentException, IllegalAccessException {
        Logger.info("Searching DB at [{}] for {} with limit {}.", dataPath(), search, limit);
        final Map<K, V> results = new HashMap<>();
        final var files = getDataFiles();

        for (final String file : files) {
            if (results.size() >= limit) {
                break;
            }
            try (final var db = getDb(file)) {
                results.putAll(search(db, search, limit));
            }
        }

        return results;
    }

    /**
     * Searches the objects using an index, without needed to loop over every record
     * Only useful for @code SearchType.EQUAL and @code SearchType.NOT_EQUAL
     * 
     * @param search the Search object
     * @param db
     * @param index
     */
    private Set<K> indexedSearch(final Search search, final Map<Object, List<K>> index) {
        Logger.info("Index searching at [{}] for {}.", dataPath(), search);
        if (index == null || index.isEmpty()) {
            return Collections.emptySet();
        }

        final Set<K> matchingKeys = new HashSet<>();
        final SearchType searchType = search.searchType();
        final Class<?> fieldClass = index.keySet().stream().filter(Objects::nonNull).findFirst()
                .map(Object::getClass).orElse(null);
        if (fieldClass == null) {
            return matchingKeys;
        }

        final Object searchTerm = CHRONICLE_UTILS.setSearchTerm(search.searchTerm(), fieldClass);
        final Set<Object> searchTermSet = (searchType == SearchType.IN || searchType == SearchType.NOT_IN
                || searchType == SearchType.CONTAINS || searchType == SearchType.NOT_CONTAINS)
                        ? CHRONICLE_UTILS.setSearchTerm((List<Object>) search.searchTerm(), fieldClass)
                        : null;

        switch (searchType) {
            case EQUAL -> {
                final List<K> keys = index.get(searchTerm);
                if (keys != null)
                    matchingKeys.addAll(keys);
            }
            case NOT_EQUAL -> {
                for (final var entry : index.entrySet()) {
                    if (!entry.getKey().equals(searchTerm)) {
                        matchingKeys.addAll(entry.getValue());
                    }
                }
            }
            case LESS -> {
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) < 0) {
                        matchingKeys.addAll(entry.getValue());
                    }
                }
            }
            case GREATER -> {
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) > 0) {
                        matchingKeys.addAll(entry.getValue());
                    }
                }
            }
            case LESS_OR_EQUAL -> {
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) <= 0) {
                        matchingKeys.addAll(entry.getValue());
                    }
                }
            }
            case GREATER_OR_EQUAL -> {
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) >= 0) {
                        matchingKeys.addAll(entry.getValue());
                    }
                }
            }
            case LIKE -> {
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.containsIgnoreCase(entry.getKey(), searchTerm)) {
                        matchingKeys.addAll(entry.getValue());
                    }
                }
            }
            case NOT_LIKE -> {
                for (final var entry : index.entrySet()) {
                    if (!CHRONICLE_UTILS.containsIgnoreCase(entry.getKey(), searchTerm)) {
                        matchingKeys.addAll(entry.getValue());
                    }
                }
            }
            // for arrays
            case CONTAINS -> {
                for (final var entry : index.entrySet()) {
                    for (final var obj : (Object[]) entry.getKey()) {
                        if (searchTermSet.contains(obj)) {
                            matchingKeys.addAll(entry.getValue());
                            break;
                        }
                    }
                }
            }
            case NOT_CONTAINS -> {
                for (final var entry : index.entrySet()) {
                    for (final var obj : (Object[]) entry.getKey()) {
                        if (!searchTermSet.contains(obj)) {
                            matchingKeys.addAll(entry.getValue());
                            break;
                        }
                    }
                }
            }
            case STARTS_WITH -> {
                for (final var entry : index.entrySet()) {
                    if (String.valueOf(entry.getKey()).startsWith(String.valueOf(searchTerm))) {
                        matchingKeys.addAll(entry.getValue());
                    }
                }
            }
            case ENDS_WITH -> {
                for (final var entry : index.entrySet()) {
                    if (String.valueOf(entry.getKey()).endsWith(String.valueOf(searchTerm))) {
                        matchingKeys.addAll(entry.getValue());
                    }
                }
            }
            case IN -> {
                for (final var entry : index.entrySet()) {
                    if (searchTermSet.contains(entry.getKey())) {
                        matchingKeys.addAll(entry.getValue());
                    }
                }
            }
            case NOT_IN -> {
                for (final var entry : index.entrySet()) {
                    if (!searchTermSet.contains(entry.getKey())) {
                        matchingKeys.addAll(entry.getValue());
                    }
                }
            }
        }

        return matchingKeys;
    }

    private Set<K> indexedSearch(final Search search, final Map<Object, List<K>> index, final int limit) {
        Logger.info("Index searching at [{}] for {} with limit {}.", dataPath(), search, limit);
        if (index == null || index.isEmpty()) {
            return Collections.emptySet();
        }

        final Set<K> matchingKeys = new HashSet<>();
        final SearchType searchType = search.searchType();
        final Class<?> fieldClass = index.keySet().stream().filter(Objects::nonNull).findFirst()
                .map(Object::getClass).orElse(null);
        if (fieldClass == null) {
            return matchingKeys;
        }

        final Object searchTerm = CHRONICLE_UTILS.setSearchTerm(search.searchTerm(), fieldClass);
        final Set<Object> searchTermSet = (searchType == SearchType.IN || searchType == SearchType.NOT_IN
                || searchType == SearchType.CONTAINS || searchType == SearchType.NOT_CONTAINS)
                        ? CHRONICLE_UTILS.setSearchTerm((List<Object>) search.searchTerm(), fieldClass)
                        : null;
        switch (searchType) {
            case EQUAL -> {
                final List<K> keys = index.get(searchTerm);
                if (keys != null) {
                    if (keys.size() <= limit)
                        matchingKeys.addAll(keys);
                    else
                        keys.stream().limit(limit).forEach(matchingKeys::add);
                }
            }
            case NOT_EQUAL -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (!entry.getKey().equals(searchTerm)) {
                        entry.getValue().stream().limit(limit - matchingKeys.size())
                                .forEach(matchingKeys::add);
                    }
                }
            }
            case LESS -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) < 0) {
                        entry.getValue().stream().limit(limit - matchingKeys.size())
                                .forEach(matchingKeys::add);
                    }
                }
            }
            case GREATER -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) > 0) {
                        entry.getValue().stream().limit(limit - matchingKeys.size())
                                .forEach(matchingKeys::add);
                    }
                }
            }
            case LESS_OR_EQUAL -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) <= 0) {
                        entry.getValue().stream().limit(limit - matchingKeys.size())
                                .forEach(matchingKeys::add);
                    }
                }
            }
            case GREATER_OR_EQUAL -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) >= 0) {
                        entry.getValue().stream().limit(limit - matchingKeys.size())
                                .forEach(matchingKeys::add);
                    }
                }
            }
            case LIKE -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (CHRONICLE_UTILS.containsIgnoreCase(entry.getKey(), searchTerm)) {
                        entry.getValue().stream().limit(limit - matchingKeys.size())
                                .forEach(matchingKeys::add);
                    }
                }
            }
            case NOT_LIKE -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (!CHRONICLE_UTILS.containsIgnoreCase(entry.getKey(), searchTerm)) {
                        entry.getValue().stream().limit(limit - matchingKeys.size())
                                .forEach(matchingKeys::add);
                    }
                }
            }
            case CONTAINS -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (Collections.singleton(entry.getKey()).contains(searchTerm)) {
                        entry.getValue().stream().limit(limit - matchingKeys.size())
                                .forEach(matchingKeys::add);
                    }
                }
            }
            case NOT_CONTAINS -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (!Collections.singleton(entry.getKey()).contains(searchTerm)) {
                        entry.getValue().stream().limit(limit - matchingKeys.size())
                                .forEach(matchingKeys::add);
                    }
                }
            }
            case STARTS_WITH -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (String.valueOf(entry.getKey()).startsWith(String.valueOf(searchTerm))) {
                        entry.getValue().stream().limit(limit - matchingKeys.size())
                                .forEach(matchingKeys::add);
                    }
                }
            }
            case ENDS_WITH -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (String.valueOf(entry.getKey()).endsWith(String.valueOf(searchTerm))) {
                        entry.getValue().stream().limit(limit - matchingKeys.size())
                                .forEach(matchingKeys::add);
                    }
                }
            }
            case IN -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (searchTermSet.contains(entry.getKey())) {
                        entry.getValue().stream().limit(limit - matchingKeys.size())
                                .forEach(matchingKeys::add);
                    }
                }
            }
            case NOT_IN -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (!searchTermSet.contains(entry.getKey())) {
                        entry.getValue().stream().limit(limit - matchingKeys.size())
                                .forEach(matchingKeys::add);
                    }
                }
            }
        }

        return matchingKeys;
    }

    default Map<K, V> indexedSearch(final Search search) throws IOException {
        final var indexFilePath = getIndexPath(search.field());
        Set<K> matchingKeys = new HashSet<K>();

        final HTreeMap<Object, List<K>> indexDb = MAP_DB.getDb(indexFilePath);
        if (indexDb != null) {
            try {
                matchingKeys = indexedSearch(search, indexDb);
            } finally {
                MAP_DB.close(indexFilePath);
            }
        }

        if (!matchingKeys.isEmpty()) {
            return get(matchingKeys);
        }

        return Collections.emptyMap();
    }

    default Map<K, V> indexedSearch(final Search search, final int limit) throws IOException {
        if (limit <= 0) {
            return Collections.emptyMap();
        }

        final var indexFilePath = getIndexPath(search.field());
        Set<K> matchingKeys = new HashSet<K>();

        final HTreeMap<Object, List<K>> indexDb = MAP_DB.getDb(indexFilePath);
        if (indexDb != null) {
            try {
                matchingKeys = indexedSearch(search, indexDb, limit);
            } finally {
                MAP_DB.close(indexFilePath);
            }
        }

        if (!matchingKeys.isEmpty()) {
            return get(matchingKeys);
        }

        return Collections.emptyMap();
    }

    default Map<K, V> indexedSearch(final Map<K, V> db, final Search search) {
        if (db == null || db.isEmpty()) {
            return Collections.emptyMap();
        }

        final var indexFilePath = getIndexPath(search.field());
        Set<K> matchingKeys = new HashSet<>();
        final Map<K, V> results = new HashMap<>();

        final HTreeMap<Object, List<K>> indexDb = MAP_DB.getDb(indexFilePath);
        if (indexDb != null) {
            try {
                matchingKeys = indexedSearch(search, indexDb);
            } finally {
                MAP_DB.close(indexFilePath);
            }
        }

        for (final K key : matchingKeys) {
            final V value = db.get(key);
            if (value != null) {
                results.put(key, value);
            }
        }

        return results;
    }

    default Map<K, V> indexedSearch(final Map<K, V> db, final Search search, final int limit) {
        if (db == null || db.isEmpty() || limit <= 0) {
            return Collections.emptyMap();
        }

        final var indexFilePath = getIndexPath(search.field());
        Set<K> matchingKeys = new HashSet<>();
        final Map<K, V> results = new HashMap<>();

        final HTreeMap<Object, List<K>> indexDb = MAP_DB.getDb(indexFilePath);
        if (indexDb != null) {
            try {
                matchingKeys = indexedSearch(search, indexDb, limit);
            } finally {
                MAP_DB.close(indexFilePath);
            }
        }

        for (final K key : matchingKeys) {
            final V value = db.get(key);
            if (value != null) {
                results.put(key, value);
            }
        }

        return results;
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
        Logger.info("Getting DB size at [{}].", dataPath());
        int size = 0;
        final var files = getDataFiles();

        for (final var file : files) {
            try (final var db = getDb(file)) {
                size += db.size();
            }
        }

        return size;
    }

    default void deleteDataFiles() throws IOException {
        Logger.info("Truncating database at [{}].", dataPath());
        final var files = getDataFiles();
        for (final var file : files) {
            CHRONICLE_UTILS.deleteFileIfExists(dataPath() + DATA_DIR + file);
        }
    }

    default boolean exists(final K key) throws IOException {
        final var keyMap = KEY_MAP_CACHE.get(key);
        final var file = getDbFile(key, keyMap);
        try (final var db = getDb(file)) {
            return db.containsKey(key);
        }
    }
}
