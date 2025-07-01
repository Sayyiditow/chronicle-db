package chronicle.db.dao;

import static chronicle.db.dao.ChronicleUtils.CHRONICLE_UTILS;
import static chronicle.db.service.ChronicleDb.CHRONICLE_DB;
import static chronicle.db.service.MapDb.MAP_DB;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.mapdb.HTreeMap;
import org.tinylog.Logger;

import com.jsoniter.spi.TypeLiteral;

import chronicle.db.entity.PutStatus;
import chronicle.db.entity.Search;
import chronicle.db.entity.Search.SearchType;
import chronicle.db.service.MapDb.SearchResult;
import net.openhft.chronicle.map.ChronicleMap;

/**
 *
 * @param <V> Type of the single element
 */
@SuppressWarnings("unchecked")
public interface ChronicleDao<V> {
    ConcurrentMap<String, Object> LOCKS = new ConcurrentHashMap<>();
    ConcurrentMap<String, Set<String>> DATA_FILE_CACHE = new ConcurrentHashMap<>();
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
     * The average key value = String
     */
    String averageKey();

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
    default Set<String> indexFileNames() {
        return Collections.emptySet();
    }

    /**
     * Get the db object, close with closeDb()
     * 
     * @return ChronicleMap<String, V>
     * @throws IOException
     */
    default ChronicleMap<String, V> openDb() throws IOException {
        return CHRONICLE_DB.open(name(), entries(), averageKey(), averageValue(), dataPath() + DATA_DIR + DATA_FILE,
                bloatFactor());
    }

    default ChronicleMap<String, V> openDb(final String fileName) throws IOException {
        return CHRONICLE_DB.open(name(), entries(), averageKey(), averageValue(), dataPath() + DATA_DIR + fileName,
                bloatFactor());
    }

    default ChronicleMap<String, V> openDb(final String fileName, final long entries) throws IOException {
        return CHRONICLE_DB.open(name(), entries, averageKey(), averageValue(), dataPath() + DATA_DIR + fileName,
                bloatFactor());
    }

    default void closeDb() {
        CHRONICLE_DB.close(dataPath() + DATA_DIR + DATA_FILE);
    }

    default void closeDb(final String fileName) {
        CHRONICLE_DB.close(dataPath() + DATA_DIR + fileName);
    }

    private String getKeyMapPath() {
        return dataPath() + DATA_DIR + KEY_FILE;
    }

    private void populateKeyMap(final Set<String> dataFiles, final HTreeMap<String, String> keyMap) throws IOException {
        for (final String file : dataFiles) {
            final var db = openDb(file);
            if (db != null) {
                try {
                    db.forEachEntry(entry -> keyMap.put(entry.key().get(), file));
                } finally {
                    closeDb(file);
                }
            }
        }
    }

    default void rebuildKeyMap() {
        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            CHRONICLE_UTILS.deleteFileIfExists(getKeyMapPath());
            final var dataFiles = getDataFiles();
            if (dataFiles.size() > 1) {
                final HTreeMap<String, String> keyMap = MAP_DB.openMap(getKeyMapPath());
                if (keyMap != null) {
                    try {
                        populateKeyMap(dataFiles, keyMap);
                    } catch (final IOException e) {
                        // ignored
                        Logger.warn("Failed to populate key map at [{}]: {}", getKeyMapPath(), e.getMessage());
                    } finally {
                        MAP_DB.closeMap(getKeyMapPath());
                    }
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
            final var dataFiles = getDataFiles();
            if (dataFiles.size() > 1 && !Files.exists(Path.of(getKeyMapPath()))) {
                final HTreeMap<String, String> keyMap = MAP_DB.openMap(getKeyMapPath());
                if (keyMap != null) {
                    try {
                        if (keyMap.isEmpty()) {
                            populateKeyMap(dataFiles, keyMap);
                        }
                    } catch (final IOException e) {
                        // ignored
                        Logger.warn("Failed to populate key map at [{}]: {}", getKeyMapPath(), e.getMessage());
                    } finally {
                        MAP_DB.closeMap(getKeyMapPath());
                    }
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
    default Set<String> deleteIndexes() {
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
    default Set<String> getDataFiles() {
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
    private void initIndex(final ChronicleMap<String, V> db, final Set<String> fields, final String indexDirPath) {
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
        getDataFiles().parallelStream().forEach(file -> {
            final Object lock = LOCKS.computeIfAbsent(dataPath() + file, k -> new Object());
            synchronized (lock) {
                try {
                    final var db = openDb(file);
                    if (db != null) {
                        try {
                            initIndex(db, fields, dataPath() + INDEX_DIR);
                        } finally {
                            closeDb(file);
                        }
                    }
                } catch (final IOException e) {
                    // ignored
                }
            }
        });
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
     * @return Map<String, V>
     * @throws IOException
     */
    default Map<String, V> fetch() throws IOException {
        Logger.info("Fetching all data at [{}].", dataPath());
        final Map<String, V> result = new HashMap<>();
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
    private String getDbFile(final String key, final Set<String> dbFiles) {
        if (dbFiles.size() <= 1) {
            return DATA_FILE;
        }

        final HTreeMap<String, String> keyMap = MAP_DB.openMap(getKeyMapPath());
        if (keyMap != null) {
            try {
                final var file = keyMap.get(key);
                if (file == null) {
                    Logger.debug("Key [{}] not found across {} files at [{}].", key, dbFiles.size(), dataPath());
                    return null;
                }
                return file;
            } finally {
                MAP_DB.closeMap(getKeyMapPath());
            }
        }

        return DATA_FILE;
    }

    /**
     * Get a map of file and set of keys in that file from cache.
     */
    private Map<String, Set<String>> getDbFiles(final Iterable<String> keys, final Set<String> dbFiles)
            throws IOException {
        final var fileMap = new HashMap<String, Set<String>>();
        final var dataFileSize = dbFiles.size();

        if (dataFileSize <= 1) {
            final var db = openDb();

            if (db != null) {
                try {
                    for (final String k : keys) {
                        if (db.get(k) != null) {
                            fileMap.computeIfAbsent(DATA_FILE, f -> new HashSet<>(1000)).add(k);
                        }
                    }
                } finally {
                    closeDb();
                }
            }

            return fileMap;
        }

        final HTreeMap<String, String> keyMap = MAP_DB.openMap(getKeyMapPath());
        if (keyMap != null) {
            try {
                for (final String k : keys) {
                    final var file = keyMap.get(k);
                    if (file != null) {
                        fileMap.computeIfAbsent(file, f -> new HashSet<>(10000 / dataFileSize)).add(k);
                    }
                }
            } finally {
                MAP_DB.closeMap(getKeyMapPath());
            }
        }

        return fileMap;
    }

    private void removeFromKeyMap(final String key) {
        final HTreeMap<String, String> keyMap = MAP_DB.openMap(getKeyMapPath());
        if (keyMap != null) {
            try {
                keyMap.remove(key);
            } finally {
                MAP_DB.closeMap(getKeyMapPath());
            }
        }
    }

    private void removeAllFromKeyMap(final Set<String> keys) {
        final HTreeMap<String, String> keyMap = MAP_DB.openMap(getKeyMapPath());
        if (keyMap != null) {
            try {
                keyMap.keySet().removeAll(keys);
            } finally {
                MAP_DB.closeMap(getKeyMapPath());
            }
        }
    }

    private void addToKeyMap(final String key, final String file) {
        final HTreeMap<String, String> keyMap = MAP_DB.openMap(getKeyMapPath());
        if (keyMap != null) {
            try {
                keyMap.put(key, file);
            } finally {
                MAP_DB.closeMap(getKeyMapPath());
            }
        }
    }

    private void addToKeyMap(final Map<String, String> map) {
        final HTreeMap<String, String> keyMap = MAP_DB.openMap(getKeyMapPath());
        if (keyMap != null) {
            try {
                keyMap.putAll(map);
            } finally {
                MAP_DB.closeMap(getKeyMapPath());
            }
        }
    }

    private int getKeyMapSize() {
        final HTreeMap<String, String> keyMap = MAP_DB.openMap(getKeyMapPath());
        if (keyMap != null) {
            try {
                return keyMap.size();
            } finally {
                MAP_DB.closeMap(getKeyMapPath());
            }
        }

        return 0;
    }

    private boolean getKeyMapExists(final String key) {
        final HTreeMap<String, String> keyMap = MAP_DB.openMap(getKeyMapPath());
        if (keyMap != null) {
            try {
                return keyMap.containsKey(key);
            } finally {
                MAP_DB.closeMap(getKeyMapPath());
            }
        }

        return false;
    }

    private Map<String, Boolean> getKeyMapExists(final Set<String> keys) {
        final var containsMap = new HashMap<String, Boolean>();
        final HTreeMap<String, String> keyMap = MAP_DB.openMap(getKeyMapPath());
        if (keyMap != null) {
            try {
                for (final String k : keys) {
                    if (keyMap.get(k) != null) {
                        containsMap.put(k, keyMap.containsKey(k));
                    }
                }
            } finally {
                MAP_DB.closeMap(getKeyMapPath());
            }
        }

        return containsMap;
    }

    private List<String> getKeyMapExistsList(final Set<String> keys) {
        final var containsList = new ArrayList<String>(keys.size());
        final HTreeMap<String, String> keyMap = MAP_DB.openMap(getKeyMapPath());
        if (keyMap != null) {
            try {
                for (final String k : keys) {
                    if (keyMap.get(k) != null) {
                        containsList.add(k);
                    }
                }
            } finally {
                MAP_DB.closeMap(getKeyMapPath());
            }
        }

        return containsList;
    }

    private List<String> getKeyMapNotExistsList(final Set<String> keys) {
        final var containsList = new ArrayList<String>(keys.size());
        final HTreeMap<String, String> keyMap = MAP_DB.openMap(getKeyMapPath());
        if (keyMap != null) {
            try {
                for (final String k : keys) {
                    if (keyMap.get(k) == null) {
                        containsList.add(k);
                    }
                }
            } finally {
                MAP_DB.closeMap(getKeyMapPath());
            }
        }

        return containsList;
    }

    /**
     * Get a value using key
     * 
     * @param key the key to search in the db
     * @return V value
     * @throws IOException
     */
    default V get(final String key) throws IOException {
        if (key == null) {
            return null;
        }

        Logger.info("Querying key [{}] at [{}].", key, dataPath());
        final var file = getDbFile(key, getDataFiles());
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
     * @return Map<String, V> values
     * @throws IOException
     */
    default Map<String, V> get(final Iterable<String> keys) throws IOException {
        if (keys == null || !keys.iterator().hasNext()) {
            return Collections.emptyMap();
        }
        Logger.info("Querying multiple keys at [{}].", dataPath());
        final var map = new HashMap<String, V>(1000);
        final var dbFiles = getDataFiles();

        if (dbFiles.size() <= 1) {
            final var db = openDb();
            if (db != null) {
                try {
                    for (final String key : keys) {
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

        final var keyFiles = getDbFiles(keys, dbFiles);
        for (final var entry : keyFiles.entrySet()) {
            final var file = entry.getKey();
            final var db = openDb(file);
            if (db != null) {
                try {
                    for (final String key : entry.getValue()) {
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
    default boolean delete(final String key) throws IOException {
        if (key == null) {
            return false;
        }

        Logger.info("Deleting key [{}] at [{}].", key, dataPath());
        final var dataFiles = getDataFiles();
        final var file = getDbFile(key, dataFiles);
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
        }

        if (value == null) {
            return false;
        }
        if (dataFiles.size() > 1) {
            removeFromKeyMap(key);
        }

        final var indexFileNames = indexFileNames();
        CHRONICLE_UTILS.removeFromIndex(name(), dataPath(), indexFileNames, Map.of(key, value));
        return true;
    }

    private void removeFromIndex(final Map<String, V> deletedMap) throws IOException {
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
    default boolean delete(final Set<String> keys) throws IOException {
        if (keys == null || keys.isEmpty()) {
            return false;
        }

        final var dataFiles = getDataFiles();
        final var deletedMap = new HashMap<String, V>();
        Logger.info("Deleting {} keys at [{}].", keys.size(), dataPath());

        if (dataFiles.size() <= 1) {
            final Object lock = LOCKS.computeIfAbsent(dataPath() + DATA_FILE, k -> new Object());
            synchronized (lock) {
                final var db = openDb();
                if (db != null) {
                    try {
                        for (final String key : keys) {
                            final var deleted = db.remove(key);
                            if (deleted != null) {
                                deletedMap.put(key, deleted);
                            }
                        }
                    } finally {
                        closeDb();
                    }
                }
            }

            if (deletedMap.isEmpty()) {
                return false;
            }

            removeFromIndex(deletedMap);
            return true;
        }

        final var dbFiles = getDbFiles(keys, dataFiles);
        // Multi-file case
        if (dbFiles.isEmpty()) {
            return false;
        }

        for (final var entry : dbFiles.entrySet()) {
            final var file = entry.getKey();
            final Object lock = LOCKS.computeIfAbsent(dataPath() + file, k -> new Object());
            synchronized (lock) {
                final var db = openDb(file);
                if (db != null) {
                    try {
                        for (final String key : entry.getValue()) {
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

        removeAllFromKeyMap(deletedMap.keySet());
        removeFromIndex(deletedMap);
        return true;
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
     * First time rotation when keyMap is null
     */
    private void rotateFile() throws IOException {
        final var currentFiles = getDataFiles();
        final String rotatedFile = "data-" + (currentFiles.size() + 1);
        final var currentPath = Path.of(dataPath() + DATA_DIR + DATA_FILE);
        final var rotatedPath = Path.of(dataPath() + DATA_DIR + rotatedFile);
        Files.move(currentPath, rotatedPath, REPLACE_EXISTING);

        final HTreeMap<String, String> keyMap = MAP_DB.openMap(getKeyMapPath());
        if (keyMap != null) {
            try {
                final var oldDb = openDb(rotatedFile);
                if (oldDb != null) {
                    try {
                        oldDb.forEachEntry(entry -> {
                            keyMap.put(entry.key().get(), rotatedFile);
                        });
                    } finally {
                        closeDb(rotatedFile);
                    }
                }
            } finally {
                MAP_DB.closeMap(getKeyMapPath());
            }
        }

        currentFiles.add(rotatedFile);
        DATA_FILE_CACHE.put(dataPath(), currentFiles);

        Logger.info("Rotated data file at [{}] to {}.", dataPath(), rotatedFile);
    }

    private ChronicleMap<String, V> checkAndRotate(ChronicleMap<String, V> db) throws IOException {
        if (db.size() >= entries()) {
            closeDb();
            rotateFile();
            db = openDb();
        }

        return db;
    }

    private ChronicleMap<String, V> checkAndRotate(ChronicleMap<String, V> db, final Map<String, String> keyMapUpdate)
            throws IOException {
        if (db.size() >= entries()) {
            closeDb();
            rotateFile();
            keyMapUpdate.clear();
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
    default PutStatus put(final String key, final V value, final Set<String> indexFileNames)
            throws IOException {
        if (key == null) {
            return PutStatus.FAILED;
        }

        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        V prevValue = null;
        Set<String> dataFiles;
        var file = DATA_FILE;
        synchronized (lock) {
            dataFiles = getDataFiles();
            file = getDbFile(key, dataFiles);
            if (file == null) {
                file = DATA_FILE;
            }

            var db = openDb(file);
            if (db != null) {
                try {
                    // only rotate if current file is full and insert mode
                    if (DATA_FILE.equals(file) && !db.containsKey(key)) {
                        db = checkAndRotate(db);
                    }
                    prevValue = db.put(key, value);
                } finally {
                    closeDb(file);
                }
            }
        }

        var status = PutStatus.INSERTED;
        if (prevValue != null) {
            CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames, Map.of(key, value),
                    Map.of(key, prevValue));
            status = PutStatus.UPDATED;
        } else {
            if (dataFiles.size() > 1) {
                addToKeyMap(key, file);
            }
            CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames, Map.of(key, value),
                    Collections.emptyMap());
        }
        Logger.info("[{}] using key [{}] at [{}].", status, key, dataPath());
        return status;
    }

    /**
     * Refer to method above
     */
    default PutStatus put(final String key, final V value) throws IOException {
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
    default PutStatus update(final String key, final V value, final Set<String> indexFileNames)
            throws IOException {
        if (key == null) {
            return PutStatus.FAILED;
        }

        V prevValue = null;
        var status = PutStatus.FAILED;
        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            final var file = getDbFile(key, getDataFiles());
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
        }

        if (status == PutStatus.UPDATED) {
            CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames, Map.of(key, value),
                    Map.of(key, prevValue));
        }
        Logger.info("[{}] using key [{}] at [{}].", status, key, dataPath());

        return status;
    }

    /**
     * Refer to method above
     */
    default PutStatus update(final String key, final V value) throws IOException {
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
    default PutStatus insert(final String key, final V value, final Set<String> indexFileNames)
            throws IOException {
        if (key == null) {
            return PutStatus.FAILED;
        }

        Set<String> dataFiles;
        var file = DATA_FILE;
        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            dataFiles = getDataFiles();
            file = getDbFile(key, dataFiles);
            if (file == null) {
                file = DATA_FILE;
            }
            if (file != null && !DATA_FILE.equals(file)) {
                return PutStatus.FAILED; // already exists
            }

            var db = openDb();
            if (db != null) {
                try {
                    // only rotate if current file is full and insert mode
                    db = checkAndRotate(db);
                    db.put(key, value);
                } finally {
                    closeDb();
                }
            }
        }

        if (dataFiles.size() > 1) {
            addToKeyMap(key, DATA_FILE);
        }
        CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames, Map.of(key, value), Collections.emptyMap());
        Logger.info("[{}] using key [{}] at [{}].", PutStatus.INSERTED, key, dataPath());
        return PutStatus.INSERTED;
    }

    /**
     * Refer to method above
     */
    default PutStatus insert(final String key, final V value) throws IOException {
        return insert(key, value, indexFileNames());
    }

    /**
     * Add/Update multiple values into the db, then update all indexes related
     * 
     * @param map the map to add
     * @throws IOException
     */
    default PutStatus put(final Map<String, V> map) throws IOException {
        if (map == null || map.isEmpty()) {
            return PutStatus.FAILED;
        }

        final int putSize = map.size();
        final var prevValues = new HashMap<String, V>(putSize);
        final Set<String> keysToInsert = new HashSet<>(map.keySet());
        final var keyMapUpdate = new HashMap<String, String>(putSize);
        var status = PutStatus.INSERTED;

        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            // update old records first then only move to new record inserts.
            final var dataFiles = getDataFiles();
            final var dbFiles = getDbFiles(keysToInsert, dataFiles);

            for (final var entry : dbFiles.entrySet()) {
                final var file = entry.getKey();
                final var db = openDb(file);
                if (db != null) {
                    try {
                        for (final String key : entry.getValue()) {
                            final V prevValue = db.put(key, map.get(key));
                            if (prevValue != null) { // Only track if it was an update
                                prevValues.put(key, prevValue);
                                keysToInsert.remove(key);
                            }
                        }
                    } finally {
                        closeDb(file);
                    }
                }
            }

            if (!prevValues.isEmpty()) {
                status = PutStatus.UPDATED;
            }
            if (!keysToInsert.isEmpty()) {
                // Insert new records (only keys in keysToInsert)
                var db = openDb();
                if (db != null) {
                    try {
                        for (final String key : keysToInsert) {
                            final V value = map.get(key);
                            db = checkAndRotate(db, keyMapUpdate);
                            db.put(key, value);
                            if (dataFiles.size() > 1) {
                                keyMapUpdate.put(key, DATA_FILE);
                            }
                        }
                    } finally {
                        closeDb();
                    }
                }
            }
        }

        if (!keyMapUpdate.isEmpty()) {
            addToKeyMap(keyMapUpdate);
        }

        CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames(), map, prevValues);
        Logger.info("Put {} records at [{}].", putSize, dataPath());

        return status;
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
    default PutStatus update(final Map<String, V> map) throws IOException {
        if (map == null || map.isEmpty()) {
            return PutStatus.FAILED;
        }

        var status = PutStatus.UPDATED;
        final var mapSize = map.size();
        final var prevValues = new HashMap<String, V>(mapSize);
        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            final var dbFiles = getDbFiles(map.keySet(), getDataFiles());

            for (final var entry : dbFiles.entrySet()) {
                final var file = entry.getKey();
                final var db = openDb(file);
                if (db != null) {
                    try {
                        for (final String key : entry.getValue()) {
                            prevValues.put(key, db.put(key, map.get(key)));
                        }
                    } finally {
                        closeDb(file);
                    }
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

    /**
     * Add multiple values into the db, this will not check if values exist or not
     * 
     * @param map the map to add
     * @throws IOException
     */
    default PutStatus insert(final Map<String, V> map) throws IOException {
        if (map == null || map.isEmpty()) {
            return PutStatus.FAILED;
        }

        final int putSize = map.size();
        final var keyMapUpdate = new HashMap<String, String>();

        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            final var dataFiles = getDataFiles();
            var db = openDb();
            if (db != null) {
                try {
                    for (final var entry : map.entrySet()) {
                        final String key = entry.getKey();
                        final V value = entry.getValue();
                        db = checkAndRotate(db, keyMapUpdate);
                        db.put(key, value);
                        if (dataFiles.size() > 1) {
                            keyMapUpdate.put(key, DATA_FILE);
                        }
                    }
                } finally {
                    closeDb();
                }
            }
        }

        if (!keyMapUpdate.isEmpty()) {
            addToKeyMap(keyMapUpdate);
        }

        CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames(), map, Collections.emptyMap());
        Logger.info("Inserted {} records at [{}].", putSize, dataPath());

        return PutStatus.INSERTED;
    }

    /**
     * Search the chronicle map based on values
     * 
     * @param db     the map
     * @param search object search
     * @return a map of the fitting values
     * @throws Throwable
     */
    default Map<String, V> search(final Map<String, V> db, final Search search) throws Throwable {
        Logger.info("Searching DB at [{}] for {}.", dataPath(), search);
        final Map<String, V> map = new HashMap<>(Math.min(db.size(), 1000));

        for (final var entry : db.entrySet()) {
            if (CHRONICLE_UTILS.search(search, entry.getKey(), entry.getValue())) {
                map.put(entry.getKey(), db.get(entry.getKey()));
            }
        }

        return map;
    }

    /**
     * Same as above, just faster with forEachEntry for first search using whole db
     * as chronicle map doesnt allow parallel streams until second map
     */
    private Map<String, V> search(final ChronicleMap<String, V> db, final Search search) {
        Logger.info("Searching DB at [{}] for {}.", dataPath(), search);
        final Map<String, V> map = new HashMap<>(Math.min(db.size(), 1000));

        db.forEachEntry(entry -> {
            try {
                final String key = entry.key().get();
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
     * @param db     the map
     * @param search object search
     * @return a map of the fitting values
     * @throws IOException
     */
    default Map<String, V> search(final Search search) throws IOException {
        Logger.info("Searching DB at [{}] for {}.", dataPath(), search);
        final Map<String, V> results = new HashMap<>();
        final int limit = search.limit();
        final var files = getDataFiles();

        for (final String file : files) {
            if (limit != -1 && results.size() >= limit) {
                break;
            }

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
     * Searches the objects using an index, without needed to loop over every record
     * Only useful for @code SearchType.EQUAL and @code SearchType.NOT_EQUAL
     * 
     * @param search the Search object
     * @param db
     * @param index
     */
    default SearchResult<byte[]> indexedSearch(final Search search, final NavigableSet<byte[]> index) {
        Logger.info("Index searching at [{}] for {}.", dataPath(), search);
        if (index == null || index.isEmpty()) {
            return new SearchResult<byte[]>(Collections.emptyList(), new AtomicInteger(0));
        }

        final SearchType searchType = search.searchType();
        final String searchTerm = String.valueOf(search.searchTerm());
        final Set<String> searchTermSet = (searchType == SearchType.IN || searchType == SearchType.NOT_IN)
                ? new HashSet<>((List<String>) search.searchTerm())
                : null;
        final var searchTermBetween = searchType == SearchType.BETWEEN ? (List<Object>) search.searchTerm() : null;

        switch (searchType) {
            case EQUAL -> {
                return MAP_DB.getEqualIndexSearch(index, searchTerm, search.limit());
            }
            case NOT_EQUAL -> {
                final var keysBefore = MAP_DB.getBeforeIndexSearch(index, searchTerm, search.limit());
                var combinedResults = keysBefore.results();
                final var combinedSize = keysBefore.size();
                if (search.limit() != -1 && keysBefore.size().get() < search.limit()) {
                    final var keysAfter = MAP_DB.getAfterIndexSearch(index, searchTerm, search.limit());
                    combinedResults = CHRONICLE_UTILS.concatIterable(keysBefore.results(), keysAfter.results());
                    combinedSize.set(combinedSize.get() + keysAfter.size().get());
                }
                return new SearchResult<byte[]>(combinedResults, combinedSize);
            }
            case LESS -> {
                return MAP_DB.getLessThanIndexSearch(index, searchTerm, search.limit());
            }
            case LESS_OR_EQUAL -> {
                return MAP_DB.getLessThanOrEqualIndexSearch(index, searchTerm, search.limit());
            }
            case GREATER -> {
                return MAP_DB.getGreaterThanIndexSearch(index, searchTerm, search.limit());
            }
            case GREATER_OR_EQUAL -> {
                return MAP_DB.getGreaterThanOrEqualIndexSearch(index, searchTerm, search.limit());
            }
            case LIKE -> {
                return MAP_DB.getLikeIndexSearch(index, searchTerm, search.limit());
            }
            case NOT_LIKE -> {
                return MAP_DB.getNotLikeIndexSearch(index, searchTerm, search.limit());
            }
            case STARTS_WITH -> {
                return MAP_DB.getStartsWithIndexSearch(index, searchTerm, search.limit());
            }
            case ENDS_WITH -> {
                return MAP_DB.getEndsWithIndexSearch(index, searchTerm, search.limit());
            }
            case IN -> {
                return MAP_DB.getInIndexSearch(index, searchTermSet, search.limit());
            }
            case NOT_IN -> {
                return MAP_DB.getNotInIndexSearch(index, searchTermSet, search.limit());
            }
            case BETWEEN -> {
                return MAP_DB.getBetweenIndexSearch(index, searchTermBetween.get(0).toString(),
                        searchTermBetween.get(1).toString(), search.limit());
            }
            default -> throw new UnsupportedOperationException("Search type not supported: " + searchType);
        }
    }

    default SearchResult<byte[]> indexedSearch(final Search search, final NavigableSet<byte[]> index,
            final SearchResult<byte[]> prevSearchResult) {
        Logger.info("Index searching at [{}] for {}.", dataPath(), search);

        if (index == null || index.isEmpty()) {
            return new SearchResult<byte[]>(Collections.emptyList(), new AtomicInteger(0));
        }

        final SearchType searchType = search.searchType();
        final String searchTerm = String.valueOf(search.searchTerm());
        final Set<byte[]> searchTermBytesSet;
        if (searchType == SearchType.IN || searchType == SearchType.NOT_IN) {
            final List<String> termList = (List<String>) search.searchTerm();
            searchTermBytesSet = termList.stream()
                    .map(s -> s.getBytes(StandardCharsets.UTF_8))
                    .collect(Collectors.toSet());
        } else {
            searchTermBytesSet = null;
        }
        final byte[] sanitizedBetween1;
        final byte[] sanitizedBetween2;

        if (searchType == SearchType.BETWEEN) {
            final var searchTermBetween = (List<Object>) search.searchTerm();
            sanitizedBetween1 = MAP_DB.getSanitizedByte(searchTermBetween.get(0));
            sanitizedBetween2 = MAP_DB.getSanitizedByte(searchTermBetween.get(1));
        } else {
            sanitizedBetween1 = null;
            sanitizedBetween2 = null;
        }
        final var sanitizedSearchTerm = MAP_DB.getSanitizedByte(searchTerm);

        final AtomicInteger count = new AtomicInteger(0);
        final Iterable<byte[]> filtered = () -> new Iterator<>() {
            private final Iterator<byte[]> it = prevSearchResult.results().iterator();
            private byte[] nextValid = null;
            private boolean hasNextComputed = false;

            @Override
            public boolean hasNext() {
                if (hasNextComputed) {
                    return nextValid != null;
                }

                while (it.hasNext()) {
                    if (search.limit() != -1 && count.get() >= search.limit()) {
                        nextValid = null;
                        hasNextComputed = true;
                        return false;
                    }
                    final byte[] key = it.next();
                    final boolean removeKey = switch (searchType) {
                        case EQUAL -> {
                            final byte[] searchKey = MAP_DB.createIndexKey(sanitizedSearchTerm, key);
                            yield !index.contains(searchKey);
                        }
                        case NOT_EQUAL -> {
                            final byte[] searchKey = MAP_DB.createIndexKey(sanitizedSearchTerm, key);
                            yield index.contains(searchKey);
                        }
                        case LESS -> !MAP_DB.isLessThanIndexMatch(index, sanitizedSearchTerm, key);
                        case LESS_OR_EQUAL -> !MAP_DB.isLessThanOrEqualIndexMatch(index, sanitizedSearchTerm, key);
                        case GREATER -> !MAP_DB.isGreaterThanIndexMatch(index, sanitizedSearchTerm, key);
                        case GREATER_OR_EQUAL ->
                            !MAP_DB.isGreaterThanOrEqualIndexMatch(index, sanitizedSearchTerm, key);
                        case LIKE -> !MAP_DB.isLikeIndexMatch(index, sanitizedSearchTerm, key);
                        case NOT_LIKE -> MAP_DB.isLikeIndexMatch(index, sanitizedSearchTerm, key);
                        case STARTS_WITH -> !MAP_DB.isStartsWithIndexMatch(index, sanitizedSearchTerm, key);
                        case ENDS_WITH -> !MAP_DB.isEndsWithIndexMatch(index, sanitizedSearchTerm, key);
                        case IN -> {
                            boolean found = false;
                            for (final byte[] term : searchTermBytesSet) {
                                final byte[] compositeKey = MAP_DB.createIndexKey(term, key);
                                if (index.contains(compositeKey)) {
                                    found = true;
                                    break;
                                }
                            }
                            yield !found;
                        }
                        case NOT_IN -> {
                            boolean found = false;
                            for (final byte[] term : searchTermBytesSet) {
                                final byte[] compositeKey = MAP_DB.createIndexKey(term, key);
                                if (index.contains(compositeKey)) {
                                    found = true;
                                    break;
                                }
                            }
                            yield !found;
                        }
                        case BETWEEN ->
                            !MAP_DB.isBetweenIndexMatch(index, sanitizedBetween1, sanitizedBetween2, key);
                        default -> throw new UnsupportedOperationException("Search type not supported: " + searchType);
                    };

                    if (!removeKey) {
                        nextValid = key;
                        hasNextComputed = true;
                        return true;
                    }
                }

                nextValid = null;
                hasNextComputed = true;
                return false;
            }

            @Override
            public byte[] next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                count.incrementAndGet();
                final byte[] result = nextValid;
                nextValid = null;
                hasNextComputed = false;
                return result;
            }
        };

        return new SearchResult<byte[]>(filtered, count);
    }

    default SearchResult<byte[]> indexedSearch(final Search search, final NavigableSet<byte[]> index,
            final Set<String> matchingKeys) {
        Logger.info("Index searching at [{}] for {}.", dataPath(), search);
        if (index == null || index.isEmpty() || matchingKeys == null || matchingKeys.isEmpty()) {
            return new SearchResult<>(Collections.emptyList(), new AtomicInteger(0));
        }

        final SearchType searchType = search.searchType();
        final String searchTerm = String.valueOf(search.searchTerm());
        final Set<byte[]> searchTermBytesSet;
        if (searchType == SearchType.IN || searchType == SearchType.NOT_IN) {
            final List<String> termList = (List<String>) search.searchTerm();
            searchTermBytesSet = termList.stream()
                    .map(s -> s.getBytes(StandardCharsets.UTF_8))
                    .collect(Collectors.toSet());
        } else {
            searchTermBytesSet = null;
        }
        final var sanitizedSearchTerm = MAP_DB.getSanitizedByte(searchTerm);
        final byte[] sanitizedBetween1;
        final byte[] sanitizedBetween2;

        if (searchType == SearchType.BETWEEN) {
            final var searchTermBetween = (List<Object>) search.searchTerm();
            sanitizedBetween1 = MAP_DB.getSanitizedByte(searchTermBetween.get(0));
            sanitizedBetween2 = MAP_DB.getSanitizedByte(searchTermBetween.get(1));
        } else {
            sanitizedBetween1 = null;
            sanitizedBetween2 = null;
        }

        final AtomicInteger count = new AtomicInteger(0);
        final Iterable<byte[]> results = () -> new Iterator<>() {
            final Iterator<String> it = matchingKeys.iterator();
            byte[] next = null;

            @Override
            public boolean hasNext() {
                while (it.hasNext()) {
                    final String key = it.next();
                    final var sanitizedKey = MAP_DB.getSanitizedByte(key);
                    final boolean skip = switch (searchType) {
                        case EQUAL -> !index.contains(MAP_DB.createIndexKey(sanitizedSearchTerm, sanitizedKey));
                        case NOT_EQUAL -> index.contains(MAP_DB.createIndexKey(sanitizedSearchTerm, sanitizedKey));
                        case LESS -> !MAP_DB.isLessThanIndexMatch(index, sanitizedSearchTerm, sanitizedKey);
                        case LESS_OR_EQUAL ->
                            !MAP_DB.isLessThanOrEqualIndexMatch(index, sanitizedSearchTerm, sanitizedKey);
                        case GREATER -> !MAP_DB.isGreaterThanIndexMatch(index, sanitizedSearchTerm, sanitizedKey);
                        case GREATER_OR_EQUAL ->
                            !MAP_DB.isGreaterThanOrEqualIndexMatch(index, sanitizedSearchTerm, sanitizedKey);
                        case LIKE -> !MAP_DB.isLikeIndexMatch(index, sanitizedSearchTerm, sanitizedKey);
                        case NOT_LIKE -> MAP_DB.isLikeIndexMatch(index, sanitizedSearchTerm, sanitizedKey);
                        case STARTS_WITH -> !MAP_DB.isStartsWithIndexMatch(index, sanitizedSearchTerm, sanitizedKey);
                        case ENDS_WITH -> !MAP_DB.isEndsWithIndexMatch(index, sanitizedSearchTerm, sanitizedKey);
                        case IN -> {
                            boolean found = false;
                            for (final byte[] term : searchTermBytesSet) {
                                final byte[] compositeKey = MAP_DB.createIndexKey(term, sanitizedKey);
                                if (index.contains(compositeKey)) {
                                    found = true;
                                    break;
                                }
                            }
                            yield !found;
                        }
                        case NOT_IN -> {
                            boolean found = false;
                            for (final byte[] term : searchTermBytesSet) {
                                final byte[] compositeKey = MAP_DB.createIndexKey(term, sanitizedKey);
                                if (index.contains(compositeKey)) {
                                    found = true;
                                    break;
                                }
                            }
                            yield !found;
                        }
                        case BETWEEN ->
                            !MAP_DB.isBetweenIndexMatch(index, sanitizedBetween1, sanitizedBetween2, sanitizedKey);
                        default -> throw new UnsupportedOperationException("Unsupported search type: " + searchType);
                    };

                    if (!skip) {
                        next = sanitizedKey;
                        return true;
                    }
                }
                return false;
            }

            @Override
            public byte[] next() {
                if (next == null && !hasNext()) {
                    throw new NoSuchElementException();
                }
                count.incrementAndGet();
                final byte[] result = next;
                next = null;
                return result;
            }
        };

        return new SearchResult<>(results, count);
    }

    default Iterable<String> toStringIterable(final Iterable<byte[]> byteKeys) {
        return () -> new Iterator<>() {
            final Iterator<byte[]> it = byteKeys.iterator();

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public String next() {
                return MAP_DB.extractIndexKey(it.next());
            }
        };
    }

    default Map<String, V> indexedSearch(final Search search) throws IOException {
        final var indexFilePath = getIndexPath(search.field());
        final var indexDb = MAP_DB.openIndex(indexFilePath);
        if (indexDb != null) {
            try {
                return get(toStringIterable(indexedSearch(search, indexDb).results()));
            } finally {
                MAP_DB.closeIndex(indexFilePath);
            }
        }

        return Collections.emptyMap();
    }

    default Map<String, V> indexedSearch(final Set<String> keys, final Search search) throws IOException {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyMap();
        }

        final var indexFilePath = getIndexPath(search.field());
        final var indexDb = MAP_DB.openIndex(indexFilePath);
        if (indexDb != null) {
            try {
                return get(toStringIterable(indexedSearch(search, indexDb, keys).results()));
            } finally {
                MAP_DB.closeIndex(indexFilePath);
            }
        }

        return Collections.emptyMap();
    }

    private <T> boolean isResultEmpty(final SearchResult<T> result) {
        return result == null || !result.results().iterator().hasNext();
    }

    default Map<String, V> multiSearch(final List<Search> searches) throws Throwable {
        if (searches == null || searches.isEmpty()) {
            return Collections.emptyMap();
        }

        // Step 1: Split searches into indexed and non-indexed
        final List<Search> indexedSearches = new ArrayList<>();
        final List<Search> nonIndexedSearches = new ArrayList<>();
        final Set<String> indexFileNames = indexFileNames();

        for (final Search search : searches) {
            if (indexFileNames.contains(search.field())) {
                indexedSearches.add(search);
            } else {
                nonIndexedSearches.add(search);
            }
        }

        Map<String, V> db = null;
        // Step 2: Process indexed searches to get intersecting keys
        if (!indexedSearches.isEmpty()) {
            final var openIndexes = new ArrayList<String>();
            SearchResult<byte[]> searchResult = null;

            try {
                for (final Search search : indexedSearches) {
                    final var indexPath = getIndexPath(search.field());
                    final var indexDb = MAP_DB.openIndex(indexPath);

                    if (indexDb != null) {
                        openIndexes.add(indexPath);
                        searchResult = (searchResult == null)
                                ? indexedSearch(search, indexDb)
                                : indexedSearch(search, indexDb, searchResult);

                        if (isResultEmpty(searchResult)) {
                            return Collections.emptyMap();
                        }
                    }
                }

                db = get(toStringIterable(searchResult.results()));
                if (nonIndexedSearches.isEmpty()) {
                    return db;
                }
            } finally {
                for (final String path : openIndexes) {
                    MAP_DB.closeIndex(path);
                }
            }
        }

        // Step 3: Process non-indexed searches
        for (final var search : nonIndexedSearches) {
            if (db == null) {
                // First non-indexed search and no indexed searches; scan all records
                db = new HashMap<>();
                final var files = getDataFiles();
                for (final String file : files) {
                    final var dbToSearch = openDb(file);
                    if (dbToSearch != null) {
                        try {
                            db.putAll(search(dbToSearch, search));
                        } finally {
                            closeDb(file);
                        }
                    }
                }
            } else {
                // Reuse the filtered db for subsequent non-indexed searches
                db = search(db, search);
            }

            if (db.isEmpty()) {
                return Collections.emptyMap(); // Early exit if no matches
            }
        }

        if (db == null) {
            return Collections.emptyMap();
        }

        // Step 5: Return the final results
        return db;
    }

    default Map<String, V> multiSearch(final Set<String> matchingKeys, final List<Search> searches) throws Throwable {
        if (searches == null || searches.isEmpty() || matchingKeys == null || matchingKeys.isEmpty()) {
            return Collections.emptyMap();
        }

        // Step 1: Split searches into indexed and non-indexed
        final List<Search> indexedSearches = new ArrayList<>();
        final List<Search> nonIndexedSearches = new ArrayList<>();
        final Set<String> indexFileNames = indexFileNames();

        for (final Search search : searches) {
            if (indexFileNames.contains(search.field())) {
                indexedSearches.add(search);
            } else {
                nonIndexedSearches.add(search);
            }
        }

        SearchResult<byte[]> searchResult = null;
        if (!indexedSearches.isEmpty()) {
            final var openIndexes = new ArrayList<String>();
            try {
                for (int i = 0; i < indexedSearches.size() && !matchingKeys.isEmpty(); i++) {
                    final Search search = indexedSearches.get(i);
                    final var indexFilePath = getIndexPath(search.field());
                    final var indexDb = MAP_DB.openIndex(indexFilePath);

                    if (indexDb != null) {
                        openIndexes.add(indexFilePath);
                        searchResult = indexedSearch(search, indexDb, matchingKeys);
                    }

                    if (isResultEmpty(searchResult)) {
                        return Collections.emptyMap();
                    }
                }

                if (nonIndexedSearches.isEmpty()) {
                    return get(toStringIterable(searchResult.results()));
                }
            } finally {
                for (final String path : openIndexes) {
                    MAP_DB.closeIndex(path);
                }
            }
        }

        // Step 3: Fetch records for the filtered keys
        Map<String, V> db = searchResult != null ? get(toStringIterable(searchResult.results())) : get(matchingKeys);
        if (db.isEmpty()) {
            return Collections.emptyMap();
        }

        // Step 4: Process non-indexed searches
        for (final var search : nonIndexedSearches) {
            db = search(db, search);
            if (db.isEmpty()) {
                return Collections.emptyMap(); // Early exit if no matches
            }
        }

        // Step 5: Return the final results
        return db;
    }

    default int multiIndexSearchCount(final List<Search> searches) throws Throwable {
        if (searches == null || searches.isEmpty()) {
            return 0;
        }

        final var openIndexes = new ArrayList<String>();
        SearchResult<byte[]> searchResult = null;

        try {
            for (final Search search : searches) {
                final var indexFilePath = getIndexPath(search.field());
                final var indexDb = MAP_DB.openIndex(indexFilePath);
                if (indexDb != null) {
                    openIndexes.add(indexFilePath);
                    searchResult = (searchResult == null)
                            ? indexedSearch(search, indexDb)
                            : indexedSearch(search, indexDb, searchResult);

                    if (isResultEmpty(searchResult)) {
                        return 0;
                    }
                }
            }

            if (searchResult != null) {
                for (@SuppressWarnings("unused")
                final var ignored : searchResult.results()) {
                    // Do nothing — just iterate to trigger `.next()` and count
                }
                return searchResult.size().get();
            }
            return 0;
        } finally {
            for (final String path : openIndexes) {
                MAP_DB.closeIndex(path);
            }
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
    default Map<String, LinkedHashMap<String, Object>> subsetOfValues(final Map<String, V> initialMap,
            final String[] fields) {
        final var map = new HashMap<String, LinkedHashMap<String, Object>>(initialMap.size());

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

        final var dataFiles = getDataFiles();
        if (dataFiles.size() > 1) {
            return getKeyMapSize();
        }

        final var db = openDb();
        if (db != null) {
            try {
                return db.size();
            } finally {
                closeDb();
            }
        }

        return 0;
    }

    default void deleteDataFiles() throws IOException {
        Logger.info("Truncating database at [{}].", dataPath());
        final var files = getDataFiles();
        for (final var file : files) {
            CHRONICLE_UTILS.deleteFileIfExists(dataPath() + DATA_DIR + file);
        }
    }

    default boolean exists(final String key) throws IOException {
        Logger.info("Checking [{}] existence at [{}].", key, dataPath());
        final var dataFiles = getDataFiles();

        if (dataFiles.size() > 1) {
            return getKeyMapExists(key);
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

    default Map<String, Boolean> existsMultiple(final Set<String> keys) throws IOException {
        Logger.info("Checking [{}] key(s) existence at [{}].", keys.size(), dataPath());
        final var dataFiles = getDataFiles();

        if (dataFiles.size() > 1) {
            return getKeyMapExists(keys);
        }

        final var containsMap = new HashMap<String, Boolean>();
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
    default List<String> existsList(final Set<String> keys) throws IOException {
        Logger.info("Checking [{}] key(s) existence at [{}].", keys.size(), dataPath());
        final var dataFiles = getDataFiles();

        if (dataFiles.size() > 1) {
            return getKeyMapExistsList(keys);
        }

        final var list = new ArrayList<String>(keys.size());
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
    default List<String> notExistsList(final Set<String> keys) throws IOException {
        Logger.info("Checking [{}] key(s) existence at [{}].", keys.size(), dataPath());
        final var dataFiles = getDataFiles();

        if (dataFiles.size() > 1) {
            return getKeyMapNotExistsList(keys);
        }

        final var list = new ArrayList<String>(keys.size());
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
