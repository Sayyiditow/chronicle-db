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
 * @param <K> Type of the unique identifier, use String or Long
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
    default Set<String> indexFileNames() throws IOException {
        return Collections.emptySet();
    }

    /**
     * Get the db object, close with closeDb()
     * 
     * @return ChronicleMap<K, V>
     * @throws IOException
     */
    private ChronicleMap<K, V> openDb() throws IOException {
        return CHRONICLE_DB.open(name(), entries(), averageKey(), averageValue(), dataPath() + DATA_DIR + DATA_FILE,
                bloatFactor());
    }

    private ChronicleMap<K, V> openDb(final String fileName) throws IOException {
        return CHRONICLE_DB.open(name(), entries(), averageKey(), averageValue(), dataPath() + DATA_DIR + fileName,
                bloatFactor());
    }

    private ChronicleMap<K, V> openDb(final String fileName, final long entries) throws IOException {
        return CHRONICLE_DB.open(name(), entries, averageKey(), averageValue(), dataPath() + DATA_DIR + fileName,
                bloatFactor());
    }

    private void closeDb() {
        CHRONICLE_DB.close(dataPath() + DATA_DIR + DATA_FILE);
    }

    private void closeDb(final String fileName) {
        CHRONICLE_DB.close(dataPath() + DATA_DIR + fileName);
    }

    private void populateKeyMap(final Set<String> dataFiles, final HTreeMap<K, String> keyMap) throws IOException {
        for (final String file : dataFiles) {
            final var db = openDb(file);
            if (db != null) {
                try {
                    for (final K key : db.keySet()) {
                        keyMap.put(key, file);
                    }
                } finally {
                    closeDb(file);
                }
            }
        }
    }

    /**
     * Create the folders required on init
     *
     */
    default void createDataDirs() {
        // check if the backup directory exists, this is the last dir to be created
        // for each db path
        if (!Files.exists(Path.of(dataPath() + BACKUP_DIR))) {
            for (final String dir : DB_DIRS) {
                try {
                    Files.createDirectories(Path.of(dataPath() + dir));
                } catch (final IOException e) {
                    Logger.error("Error on db directory creation for [{}].", dataPath());
                    Logger.error(e);
                }
            }
        }

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
     * 
     * @throws IOException
     */
    default void backup() throws IOException {
        final var dataPath = dataPath() + DATA_DIR;
        final var backupPath = dataPath() + BACKUP_DIR;
        final var dataFiles = CHRONICLE_UTILS.getFileList(dataPath);

        for (final var file : dataFiles) {
            Files.copy(Path.of(dataPath + file), Path.of(backupPath + file), REPLACE_EXISTING);
        }
    }

    /**
     * Delete and rerun all indexes. Faster when inserting a lot of records.
     * 
     * @throws IOException
     */
    default Set<String> deleteIndexes() throws IOException {
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
     */
    default String getIndexPath(final String field) {
        return dataPath() + INDEX_DIR + field;
    }

    /**
     * Cache to store data file names
     */
    private Set<String> getDataFiles() {
        return DATA_FILE_CACHE.computeIfAbsent(dataPath(), k -> {
            try (final var stream = Files.list(Path.of(dataPath() + DATA_DIR))) {
                final var dataFiles = stream.map(Path::getFileName).map(Path::toString)
                        .filter(file -> file.startsWith("data"))
                        .collect(Collectors.toCollection(HashSet::new));

                if (dataFiles.isEmpty()) {
                    return new HashSet<>(Collections.singleton("data"));
                }
                return dataFiles;
            } catch (final IOException e) {
                // should never happen
                Logger.error("Failed to initialize data file cache for [{}].", dataPath());
                return null;
            }
        });
    }

    /**
     * Only runs to initialize an index on the field first time
     * 
     * @param field the field of the V value object
     * 
     */
    private void initIndex(final ChronicleMap<K, V> db, final Set<String> fields, final String indexDirPath) {
        CHRONICLE_UTILS.index(db, name(), fields, dataPath(), indexDirPath);
    }

    /**
     * Only runs to initialize an index on the field first time
     * 
     * @param field the field of the V value object
     * @throws IOException
     * 
     */
    private void initIndex(final Set<String> fields) throws IOException {
        for (final String file : getDataFiles()) {
            final var db = openDb(file);
            if (db != null) {
                try {
                    initIndex(db, fields, dataPath() + INDEX_DIR);
                } finally {
                    closeDb(file);
                }
            }
        }
    }

    /**
     * Delete and rerun all indexes. Faster when inserting a lot of records.
     * 
     * @throws IOException
     */
    default void refreshIndexes() throws IOException {
        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());

        synchronized (lock) {
            final var indexFiles = indexFileNames();
            if (!indexFiles.isEmpty()) {
                Logger.info("Re-initializing indexes at [{}].", dataPath());
                for (final String field : indexFiles) {
                    CHRONICLE_UTILS.deleteFileIfExists(getIndexPath(field));
                }
                initIndex(indexFiles);
            }
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
                    final Set<String> missingIndexes = new HashSet<>(indexFileNames);
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
        try (final var db = CHRONICLE_DB.recoverDb(name(), entries(), averageKey(), averageValue(),
                dataFileStr, bloatFactor())) {
            final var dbRecovery = openDb(RECOVER_FILE);
            dbRecovery.putAll(db);
            closeDb(RECOVER_FILE);
        }
        final var dataFilePath = Path.of(dataFileStr);
        Files.move(dataFilePath, Path.of(dataPath() + BACKUP_DIR + CORRUPTED_FILE), REPLACE_EXISTING);
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
            final var db = openDb(file);
            if (db != null) {
                try {
                    result.putAll(db);
                } finally {
                    closeDb(file);
                }
            }
        }
        return result;
    }

    /**
     * Get the db file for this key from cache or default file is new key
     * if cache is empty, use default file
     */
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

    /**
     * Get a map of file and set of keys in that file from cache.
     */
    private Map<String, Set<K>> getDbFiles(final Set<K> keys, final HTreeMap<?, String> keyMap) {
        final var fileMap = new HashMap<String, Set<K>>();
        for (final K k : keys) {
            final var file = keyMap.get(k);
            if (file != null) {
                fileMap.computeIfAbsent(file, f -> new HashSet<>(keys.size() / 5)).add(k);
            }
        }

        return fileMap;
    }

    /**
     * Get a map of file and set of keys in that file from DATA_FILE.
     */
    private Map<String, Set<K>> getDbFiles(final Set<K> keys) throws IOException {
        final var fileMap = new HashMap<String, Set<K>>();
        final var db = openDb();

        if (db != null) {
            try {
                for (final K k : keys) {
                    if (db.containsKey(k)) {
                        fileMap.computeIfAbsent(DATA_FILE, f -> new HashSet<>(keys.size())).add(k);
                    }
                }
            } finally {
                closeDb();
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

        Logger.info("Querying key [{}] at [{}].", key, dataPath());
        final var keyMap = KEY_MAP_CACHE.get(dataPath());
        final var file = keyMap == null ? DATA_FILE : keyMap.get(key);

        if (file == null) {
            return null;
        }

        V value = null;
        final var db = openDb(file);

        if (db != null) {
            try {
                value = db.getUsing(key, using());
            } finally {
                closeDb(file);
            }
        }

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
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyMap();
        }
        Logger.info("Querying {} keys at [{}].", keys.size(), dataPath());
        final var map = new HashMap<K, V>(keys.size());

        final var keyMap = KEY_MAP_CACHE.get(dataPath());
        if (keyMap == null) {
            final var db = openDb();
            if (db != null) {
                try {
                    for (final K key : keys) {
                        final V value = db.getUsing(key, using());
                        if (value != null) {
                            map.put(key, value);
                        }
                    }
                } finally {
                    closeDb();
                }
            }
            return map;
        }

        final var dbFiles = getDbFiles(keys, keyMap);
        for (final var entry : dbFiles.entrySet()) {
            final var file = entry.getKey();
            final var db = openDb(file);
            if (db != null) {
                try {
                    for (final K key : entry.getValue()) {
                        map.put(key, db.getUsing(key, using()));
                    }
                } finally {
                    closeDb(file);
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

        Logger.info("Deleting key [{}] at [{}].", key, dataPath());
        final var keyMap = KEY_MAP_CACHE.get(dataPath());
        final var keyMapIsNull = keyMap == null;
        final var file = keyMapIsNull ? DATA_FILE : keyMap.get(key);

        if (file == null) {
            return false;
        }

        final var keyLock = LOCKS.computeIfAbsent(dataPath() + key, k -> new Object());

        V value = null;
        synchronized (keyLock) {
            final var db = openDb(file);
            if (db != null) {
                try {
                    value = db.remove(key);
                } finally {
                    closeDb(file);
                }
            }

            if (value == null) {
                return false;
            }

            if (!keyMapIsNull) {
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
                final var db = openDb();
                if (db != null) {
                    try {
                        for (final K key : keys) {
                            final var deleted = db.remove(key);
                            if (deleted != null) {
                                deletedMap.put(key, deleted);
                            }
                        }
                    } finally {
                        closeDb();
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
                    final var file = entry.getKey();
                    final var db = openDb(file);
                    if (db != null) {
                        try {
                            for (final K key : entry.getValue()) {
                                keyMap.remove(key);
                                deletedMap.put(key, db.remove(key));
                            }
                        } finally {
                            closeDb(file);
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
            final var dataFilePath = dataPath() + DATA_DIR + DATA_FILE;
            final var backupDataFilePath = dataPath() + BACKUP_DIR + DATA_FILE;
            final var tempFileName = "data.tmp";
            final var tempFilePath = dataPath() + DATA_DIR + tempFileName;
            long currentEntrySize = 0;
            boolean success = false;
            final var db = openDb();

            if (db != null) {
                try {
                    currentEntrySize = db.size();
                    if (newSize <= currentEntrySize) {
                        Logger.warn("New size {} is not larger than current size {} at [{}]. Skipping resize.",
                                newSize, currentEntrySize, dataPath());
                        return;
                    }
                    final var newDb = openDb(tempFileName, newSize);
                    if (newDb != null) {
                        try {
                            newDb.putAll(db);
                            success = true;
                        } finally {
                            closeDb(tempFileName);
                        }
                    }
                } finally {
                    closeDb();
                }

                if (success) {
                    Files.move(Path.of(dataFilePath), Path.of(backupDataFilePath), REPLACE_EXISTING);
                    Files.move(Path.of(tempFilePath), Path.of(dataFilePath), REPLACE_EXISTING);
                    Logger.info("Resized DB at [{}] from {} to {}.", dataPath(), currentEntrySize, newSize);
                }
            }
        }
    }

    /**
     * Rotate files and keep the data file as latest
     * 
     * @throws IOException
     */
    private void rotateFile(final HTreeMap<K, String> keyMap) throws IOException {
        final var currentFiles = getDataFiles();
        final String rotatedFile = "data-" + (currentFiles.size() + 1);
        final var currentPath = Path.of(dataPath() + DATA_DIR + DATA_FILE);
        final var rotatedPath = Path.of(dataPath() + DATA_DIR + rotatedFile);
        Files.move(currentPath, rotatedPath);

        final var oldDb = openDb(rotatedFile);
        if (oldDb != null) {
            try {
                for (final K key : oldDb.keySet()) {
                    keyMap.put(key, rotatedFile);
                }
            } finally {
                closeDb(rotatedFile);
            }
        }

        currentFiles.add(rotatedFile);
        DATA_FILE_CACHE.put(dataPath(), currentFiles);

        Logger.info("Rotated data file at [{}] to {}.", dataPath(), rotatedFile);
    }

    /**
     * First time rotation when keyMap is null
     */
    private HTreeMap<K, String> rotateFile() throws IOException {
        final var currentFiles = getDataFiles();
        final String rotatedFile = "data-" + (currentFiles.size() + 1);
        final var currentPath = Path.of(dataPath() + DATA_DIR + DATA_FILE);
        final var rotatedPath = Path.of(dataPath() + DATA_DIR + rotatedFile);
        Files.move(currentPath, rotatedPath, REPLACE_EXISTING);
        final HTreeMap<K, String> keyMap = MAP_DB.getMemoryDirectDb();
        KEY_MAP_CACHE.put(dataPath(), keyMap);

        final var oldDb = openDb(rotatedFile);
        if (oldDb != null) {
            try {
                for (final K key : oldDb.keySet()) {
                    keyMap.put(key, rotatedFile);
                }
            } finally {
                closeDb(rotatedFile);
            }
        }

        currentFiles.add(rotatedFile);
        DATA_FILE_CACHE.put(dataPath(), currentFiles);

        Logger.info("Rotated data file at [{}] to {}.", dataPath(), rotatedFile);

        return keyMap;
    }

    private ChronicleMap<K, V> checkAndRotate(ChronicleMap<K, V> db, HTreeMap<K, String> keyMap) throws IOException {
        if (db.size() >= entries()) {
            closeDb();
            if (keyMap == null) {
                keyMap = rotateFile();
            } else {
                rotateFile(keyMap);
            }
            db = openDb();
        }

        return db;
    }

    /**
     * Add/Update a value
     * 
     * @param key   the key
     * @param value the value
     * @return true if updated else false
     * @throws IOException
     */
    default PutStatus put(final K key, final V value, final Set<String> indexFileNames)
            throws IOException {
        if (key == null) {
            return PutStatus.FAILED;
        }

        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            final var keyMap = (HTreeMap<K, String>) KEY_MAP_CACHE.get(dataPath());
            final var file = getDbFile(key, keyMap);
            var db = openDb(file);
            V prevValue = null;

            if (db != null) {
                try {
                    // only rotate if current file is full and insert mode
                    if (DATA_FILE.equals(file) && !db.containsKey(key)) {
                        db = checkAndRotate(db, keyMap);
                    }
                    prevValue = db.put(key, value);
                } finally {
                    closeDb(file);
                }
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
            Logger.info("[{}] using key [{}] at [{}].", status, key, dataPath());
            return status;
        }
    }

    /**
     * Refer to method above
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
    default PutStatus update(final K key, final V value, final Set<String> indexFileNames)
            throws IOException {
        if (key == null) {
            return PutStatus.FAILED;
        }

        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            final var file = getDbFile(key, KEY_MAP_CACHE.get(dataPath()));
            var status = PutStatus.FAILED;
            V prevValue = null;
            final var db = openDb(file);

            if (db != null) {
                try {
                    if (db.containsKey(key)) {
                        status = PutStatus.UPDATED;
                        prevValue = db.put(key, value);
                    }
                } finally {
                    closeDb(file);
                }
            }

            if (status == PutStatus.UPDATED) {
                CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames, Map.of(key, value),
                        Map.of(key, prevValue));
            }
            Logger.info("[{}] using key [{}] at [{}].", status, key, dataPath());

            return status;
        }
    }

    /**
     * Refer to method above
     */
    default PutStatus update(final K key, final V value) throws IOException {
        return update(key, value, indexFileNames());
    }

    /**
     * Add a value, will not check if it exists
     * 
     * @param key   the key
     * @param value the value
     * @return true if updated else false
     * @throws IOException
     */
    default PutStatus insert(final K key, final V value, final Set<String> indexFileNames)
            throws IOException {
        if (key == null) {
            return PutStatus.FAILED;
        }

        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            final var keyMap = (HTreeMap<K, String>) KEY_MAP_CACHE.get(dataPath());
            var db = openDb();

            if (db != null) {
                try {
                    // only rotate if current file is full and insert mode
                    db = checkAndRotate(db, keyMap);
                    db.put(key, value);
                } finally {
                    closeDb();
                }
            }

            if (keyMap != null)
                keyMap.put(key, DATA_FILE);
            CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames, Map.of(key, value), Collections.emptyMap());
            Logger.info("[{}] using key [{}] at [{}].", PutStatus.INSERTED, key, dataPath());
            return PutStatus.INSERTED;
        }
    }

    /**
     * Refer to method above
     */
    default PutStatus insert(final K key, final V value) throws IOException {
        return insert(key, value, indexFileNames());
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
            final int putSize = map.size();
            final var prevValues = new HashMap<K, V>(putSize);
            final Set<K> keysToInsert = new HashSet<>(map.keySet());

            // update old records first then only move to new record inserts.
            final var keyMap = (HTreeMap<K, String>) KEY_MAP_CACHE.get(dataPath());
            final var dbFiles = keyMap == null ? getDbFiles(map.keySet()) : getDbFiles(map.keySet(), keyMap);

            for (final var entry : dbFiles.entrySet()) {
                final var file = entry.getKey();
                final var db = openDb(file);
                if (db != null) {
                    try {
                        for (final K key : entry.getValue()) {
                            prevValues.put(key, db.put(key, map.get(key)));
                            keysToInsert.remove(key);
                        }
                    } finally {
                        closeDb(file);
                    }
                }
            }

            final var status = !prevValues.isEmpty() ? PutStatus.UPDATED : PutStatus.INSERTED;
            if (!map.isEmpty()) {
                // Insert new records (only keys in keysToInsert)
                var db = openDb();
                if (db != null) {
                    try {
                        for (final K key : keysToInsert) {
                            final V value = map.get(key);
                            db = checkAndRotate(db, keyMap);
                            db.put(key, value);
                            if (keyMap != null) {
                                keyMap.put(key, DATA_FILE);
                            }
                        }
                    } finally {
                        closeDb();
                    }
                }
            }

            CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames(), map, prevValues);
            Logger.info("Put {} records at [{}].", putSize, dataPath());

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
            final var dbFiles = keyMap == null ? getDbFiles(map.keySet()) : getDbFiles(map.keySet(), keyMap);
            final var mapSize = map.size();
            final var prevValues = new HashMap<K, V>(mapSize);
            var status = PutStatus.UPDATED;

            for (final var entry : dbFiles.entrySet()) {
                final var file = entry.getKey();
                final var db = openDb(file);
                if (db != null) {
                    try {
                        for (final K key : entry.getValue()) {
                            prevValues.put(key, db.put(key, map.get(key)));
                        }
                    } finally {
                        closeDb(file);
                    }
                }
            }

            final var prevValueSize = prevValues.size();
            if (prevValueSize != mapSize) {
                final var mapKeySet = map.keySet();
                final var prevValueKeySet = prevValues.keySet();
                final var extraSize = mapKeySet.size() - prevValueKeySet.size();
                mapKeySet.removeAll(prevValues.keySet());

                Logger.error("{} extra keys found during update at [{}]. New keys: [{}].", extraSize, dataPath(),
                        mapKeySet);
                status = prevValueSize == 0 ? PutStatus.FAILED : PutStatus.PARTIAL;
            }

            CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames(), map, prevValues);
            Logger.info("Updated {} records at [{}].", prevValueSize, dataPath());
            return status;
        }
    }

    /**
     * Add multiple values into the db, this will not check if values exist or not
     * 
     * @param map the map to add
     * @throws IOException
     */
    default PutStatus insert(final Map<K, V> map) throws IOException {
        if (map == null || map.isEmpty()) {
            return PutStatus.FAILED;
        }

        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            final int putSize = map.size();

            // update old records first then only move to new record inserts.
            final var keyMap = (HTreeMap<K, String>) KEY_MAP_CACHE.get(dataPath());
            var db = openDb();
            if (db != null) {
                try {
                    for (final var entry : map.entrySet()) {
                        final K key = entry.getKey();
                        final V value = entry.getValue();
                        db = checkAndRotate(db, keyMap);
                        db.put(key, value);
                        if (keyMap != null) {
                            keyMap.put(key, DATA_FILE);
                        }
                    }
                } finally {
                    closeDb();
                }
            }

            CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames(), map, Collections.emptyMap());
            Logger.info("Inserted {} records at [{}].", putSize, dataPath());

            return PutStatus.INSERTED;
        }
    }

    /**
     * Search the chronicle map based on values
     * 
     * @param db     the map
     * @param search object search
     * @return a map of the fitting values
     * @throws Throwable
     */
    default Map<K, V> search(final Map<K, V> db, final Search search) throws Throwable {
        Logger.info("Searching DB at [{}] for {}.", dataPath(), search);
        final Map<K, V> map = new HashMap<>();

        for (final var entry : db.entrySet()) {
            final K key = entry.getKey();
            final V value = entry.getValue();
            if (CHRONICLE_UTILS.search(search, key, value)) {
                map.put(key, value);
            }
        }

        return map;
    }

    /**
     * Same as above, just faster with forEachEntry for first search using whole db
     */
    private Map<K, V> search(final ChronicleMap<K, V> db, final Search search) {
        Logger.info("Searching DB at [{}] for {}.", dataPath(), search);
        final Map<K, V> map = new HashMap<>(Math.min(db.size(), 1000));

        db.forEachEntry(entry -> {
            try {
                final K key = entry.key().get();
                if (CHRONICLE_UTILS.search(search, key, entry.value().get())) {
                    map.put(key, db.getUsing(key, using()));
                }
            } catch (final Throwable e) {
                Logger.error("Search failed. {}", search);
                Logger.error(e);
            }
        });

        return map;
    }

    /**
     * Search the chronicle map based on values
     * 
     * @param search object search
     * @return a map of the fitting values
     * @throws Throwable
     */
    default Map<K, V> search(final Map<K, V> db, final Search search, final int limit)
            throws Throwable {
        Logger.info("Searching DB at [{}] for {} with limit {}.", dataPath(), search, limit);
        final Map<K, V> map = new HashMap<>(Math.min(db.size(), 1000));

        for (final var entry : db.entrySet()) {
            final K key = entry.getKey();
            final V value = entry.getValue();
            if (CHRONICLE_UTILS.search(search, key, value)) {
                map.put(key, value);
            }
            if (map.size() == limit) {
                break;
            }
        }

        return map;
    }

    /**
     * Same as above, just faster with forEachEntry for first search using whole db
     */
    private Map<K, V> search(final ChronicleMap<K, V> db, final Search search, final int limit) {
        Logger.info("Searching DB at [{}] for {} with limit {}.", dataPath(), search, limit);
        final Map<K, V> map = new HashMap<>(Math.min(db.size(), 1000));

        try {
            db.forEachEntry(entry -> {
                try {
                    final K key = entry.key().get();
                    if (CHRONICLE_UTILS.search(search, key, entry.value().get())) {
                        map.put(key, db.getUsing(key, using()));
                    }
                } catch (final Throwable e) {
                    Logger.error("Search with limit failed. {}", search);
                    Logger.error(e);
                }
                if (map.size() == limit) {
                    throw new RuntimeException("Breaking forEachEntry.");
                }
            });
        } catch (final RuntimeException e) {// ignored
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
        Logger.info("Searching DB at [{}] for {}.", dataPath(), search);
        final Map<K, V> results = new HashMap<>();
        final var files = getDataFiles();

        for (final String file : files) {
            final var db = openDb(file);
            if (db != null) {
                try {
                    results.putAll(search(db, search));
                } finally {
                    closeDb(file);
                }
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
     */
    default Map<K, V> search(final Search search, final int limit) throws IOException {
        Logger.info("Searching DB at [{}] for {} with limit {}.", dataPath(), search, limit);
        final Map<K, V> results = new HashMap<>();
        final var files = getDataFiles();

        for (final String file : files) {
            if (results.size() >= limit) {
                break;
            }
            final var db = openDb(file);
            if (db != null) {
                try {
                    results.putAll(search(db, search, limit));
                } finally {
                    closeDb(file);
                }
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
        final Class<?> fieldClass = CHRONICLE_UTILS.getFieldClass(index);
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

    private void addKeysUpToLimit(final List<K> keys, final Set<K> matchingKeys, final int limit) {
        final int remaining = limit - matchingKeys.size();
        if (remaining <= 0)
            return; // Early exit if no space left

        final var iterator = keys.iterator();
        int count = 0;
        while (iterator.hasNext() && count < remaining) {
            matchingKeys.add(iterator.next());
            count++;
        }
    }

    private Set<K> indexedSearch(final Search search, final Map<Object, List<K>> index, final int limit) {
        Logger.info("Index searching at [{}] for {} with limit {}.", dataPath(), search, limit);
        if (index == null || index.isEmpty()) {
            return Collections.emptySet();
        }

        final Set<K> matchingKeys = new HashSet<>();
        final SearchType searchType = search.searchType();
        final Class<?> fieldClass = CHRONICLE_UTILS.getFieldClass(index);
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
                    addKeysUpToLimit(keys, matchingKeys, limit);
                }
            }
            case NOT_EQUAL -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (!entry.getKey().equals(searchTerm)) {
                        addKeysUpToLimit(entry.getValue(), matchingKeys, limit);
                    }
                }
            }
            case LESS -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) < 0) {
                        addKeysUpToLimit(entry.getValue(), matchingKeys, limit);
                    }
                }
            }
            case GREATER -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) > 0) {
                        addKeysUpToLimit(entry.getValue(), matchingKeys, limit);
                    }
                }
            }
            case LESS_OR_EQUAL -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) <= 0) {
                        addKeysUpToLimit(entry.getValue(), matchingKeys, limit);
                    }
                }
            }
            case GREATER_OR_EQUAL -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) >= 0) {
                        addKeysUpToLimit(entry.getValue(), matchingKeys, limit);
                    }
                }
            }
            case LIKE -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (CHRONICLE_UTILS.containsIgnoreCase(entry.getKey(), searchTerm)) {
                        addKeysUpToLimit(entry.getValue(), matchingKeys, limit);
                    }
                }
            }
            case NOT_LIKE -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (!CHRONICLE_UTILS.containsIgnoreCase(entry.getKey(), searchTerm)) {
                        addKeysUpToLimit(entry.getValue(), matchingKeys, limit);
                    }
                }
            }
            case CONTAINS -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    for (final var obj : (Object[]) entry.getKey()) {
                        if (searchTermSet.contains(obj)) {
                            addKeysUpToLimit(entry.getValue(), matchingKeys, limit);
                        }
                    }
                }
            }
            case NOT_CONTAINS -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    for (final var obj : (Object[]) entry.getKey()) {
                        if (!searchTermSet.contains(obj)) {
                            addKeysUpToLimit(entry.getValue(), matchingKeys, limit);
                        }
                    }
                }
            }
            case STARTS_WITH -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (String.valueOf(entry.getKey()).startsWith(String.valueOf(searchTerm))) {
                        addKeysUpToLimit(entry.getValue(), matchingKeys, limit);
                    }
                }
            }
            case ENDS_WITH -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (String.valueOf(entry.getKey()).endsWith(String.valueOf(searchTerm))) {
                        addKeysUpToLimit(entry.getValue(), matchingKeys, limit);
                    }
                }
            }
            case IN -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (searchTermSet.contains(entry.getKey())) {
                        addKeysUpToLimit(entry.getValue(), matchingKeys, limit);
                    }
                }
            }
            case NOT_IN -> {
                for (final var entry : index.entrySet()) {
                    if (matchingKeys.size() >= limit)
                        break;
                    if (!searchTermSet.contains(entry.getKey())) {
                        addKeysUpToLimit(entry.getValue(), matchingKeys, limit);
                    }
                }
            }
        }

        return matchingKeys;
    }

    default Map<K, V> indexedSearch(final Search search) throws IOException {
        final var indexFilePath = getIndexPath(search.field());
        Set<K> matchingKeys = new HashSet<K>();

        final HTreeMap<Object, List<K>> indexDb = MAP_DB.open(indexFilePath);
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

        final HTreeMap<Object, List<K>> indexDb = MAP_DB.open(indexFilePath);
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

        final HTreeMap<Object, List<K>> indexDb = MAP_DB.open(indexFilePath);
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

        final HTreeMap<Object, List<K>> indexDb = MAP_DB.open(indexFilePath);
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
    default Map<K, LinkedHashMap<String, Object>> subsetOfValues(final Map<K, V> initialMap, final String[] fields) {
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

        final var keyMap = KEY_MAP_CACHE.get(dataPath());

        if (keyMap != null) {
            return keyMap.size();
        }

        int size = 0;
        final var files = getDataFiles();

        for (final var file : files) {
            final var db = openDb(file);
            if (db != null) {
                try {
                    size += db.size();
                } finally {
                    closeDb(file);
                }
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
        Logger.info("Checking [{}] existence at [{}].", key, dataPath());
        final var keyMap = KEY_MAP_CACHE.get(dataPath());

        if (keyMap != null) {
            return keyMap.containsKey(key);
        }

        final var db = openDb();
        if (db != null) {
            try {
                return db.containsKey(key);
            } finally {
                closeDb();
            }
        }

        return false;
    }

    default Map<K, Boolean> existsMultiple(final Set<K> keys) throws IOException {
        Logger.info("Checking [{}] key(s) existence at [{}].", keys.size(), dataPath());
        final var keyMap = KEY_MAP_CACHE.get(dataPath());
        final var containsMap = new HashMap<K, Boolean>();

        if (keyMap != null) {
            for (final var key : keys) {
                containsMap.put(key, keyMap.containsKey(key));
            }

            return containsMap;
        }

        final var db = openDb();
        if (db != null) {
            try {
                for (final var key : keys) {
                    containsMap.put(key, db.containsKey(key));
                }
            } finally {
                closeDb();
            }
        }

        return containsMap;
    }

    /**
     * Returns only the keys that exist
     */
    default List<K> existsList(final Set<K> keys) throws IOException {
        Logger.info("Checking [{}] key(s) existence at [{}].", keys.size(), dataPath());
        final var keyMap = KEY_MAP_CACHE.get(dataPath());
        final var list = new ArrayList<K>(keys.size());

        if (keyMap != null) {
            for (final var key : keys) {
                if (keyMap.containsKey(key)) {
                    list.add(key);
                }
            }

            return list;
        }

        final var db = openDb();
        if (db != null) {
            try {
                for (final var key : keys) {
                    if (db.containsKey(key)) {
                        list.add(key);
                    }
                }
            } finally {
                closeDb();
            }
        }

        return list;
    }

    /**
     * Returns only the keys that dont exist
     */
    default List<K> notExistsList(final Set<K> keys) throws IOException {
        Logger.info("Checking [{}] key(s) existence at [{}].", keys.size(), dataPath());
        final var keyMap = KEY_MAP_CACHE.get(dataPath());
        final var list = new ArrayList<K>(keys.size());

        if (keyMap != null) {
            for (final var key : keys) {
                if (!keyMap.containsKey(key)) {
                    list.add(key);
                }
            }

            return list;
        }

        final var db = openDb();
        if (db != null) {
            try {
                for (final var key : keys) {
                    if (!db.containsKey(key)) {
                        list.add(key);
                    }
                }
            } finally {
                closeDb();
            }
        }

        return list;
    }
}
