package chronicle.db.dao;

import static chronicle.db.dao.ChronicleUtils.CHRONICLE_UTILS;
import static chronicle.db.service.ChronicleDb.CHRONICLE_DB;
import static chronicle.db.service.MapDb.MAP_DB;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
    int HARD_LIMIT = 100_000;

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
     * If an object needs indexes, use this to declare.
     */
    default Map<String, Set<Object>> indexExclusions() {
        return Collections.emptyMap();
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
                throw new RuntimeException("Failed to initialize data file cache for [{" + dataPath() + "}]", e);
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
        CHRONICLE_UTILS.index(db, name(), fields, dataPath(), indexDirPath, averageValue().getClass(),
                indexExclusions());
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
        final var dataFilePath = Path.of(dataFileStr);

        // 1. Make a backup before recovery
        final var backupPath = Path.of(dataPath() + BACKUP_DIR + CORRUPTED_FILE);
        Files.copy(dataFilePath, backupPath, StandardCopyOption.REPLACE_EXISTING);
        Logger.info("Backed up file to {}", backupPath);
        try (final var db = CHRONICLE_DB.recoverDb(name(), entries(), averageKey(), averageValue(),
                dataFileStr, bloatFactor())) {
            Logger.info("Recovered ChronicleMap [{}] with {} entries", dataFileName, db.size());
        }
    }

    /**
     * Fetches all records in the db
     * 
     * @return Map<String, V>
     * @throws IOException
     */
    default Map<String, V> fetch() throws IOException {
        final Map<String, V> result = new HashMap<>();
        for (final String file : getDataFiles()) {
            if (result.size() >= HARD_LIMIT) {
                break;
            }
            final var db = openDb(file);
            if (db != null) {
                try {
                    db.forEachEntryWhile(entry -> {
                        if (result.size() >= HARD_LIMIT) {
                            return false; // Early exit
                        }
                        final var key = entry.key().get();
                        result.put(key, db.getUsing(key, using()));
                        return true;
                    });
                } finally {
                    closeDb(file);
                }
            }
        }
        Logger.info("Fetched [{}] entries at [{}].", result.size(), dataPath());
        return result;
    }

    /**
     * Fetches all keys in the db, never run directly for huge files
     * 
     * @return Set<String>
     * @throws IOException
     */
    default Set<String> fetchKeys() throws IOException {
        final Set<String> result = new HashSet<>();
        if (getDataFiles().size() > 1) {
            return getKeyMapKeys();
        }
        final var db = openDb();
        if (db != null) {
            try {
                result.addAll(db.keySet());
            } finally {
                closeDb();
            }
        }
        Logger.info("Fetched [{}] keys at [{}].", result.size(), dataPath());
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

    public record GroupedKeys(Map<String, Iterable<String>> fileGroups, AutoCloseable closer) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            if (closer != null) {
                closer.close();
            }
        }
    }

    /**
     * Get a map of file and set of keys in that file from cache.
     * 
     * @throws IOException
     */
    private GroupedKeys getDbFiles(final Iterable<String> keys, final Set<String> dbFiles) throws IOException {
        final var dataFileSize = dbFiles.size();

        if (dataFileSize <= 1) {
            final Set<String> keySet = new HashSet<>();
            final var db = openDb();

            if (db != null) {
                try {
                    for (final String k : keys) {
                        if (db.get(k) != null) {
                            keySet.add(k);
                        }
                    }
                } finally {
                    closeDb();
                }
            }

            final Map<String, Iterable<String>> singleGroup = Map.of(DATA_FILE, keySet);
            return new GroupedKeys(singleGroup, () -> {
            });
        }

        // Multi-file: use lazy grouping based on keyMap
        final HTreeMap<String, String> keyMap = MAP_DB.openMap(getKeyMapPath());

        // Group keys lazily by file using filtering
        final Map<String, Iterable<String>> fileGroups = new HashMap<>();
        for (final String file : dbFiles) {
            final Iterable<String> fileKeys = () -> new Iterator<>() {
                final Iterator<String> it = keys.iterator();
                String nextValid = null;
                boolean computed = false;

                private void findNext() {
                    while (it.hasNext()) {
                        final String k = it.next();
                        final String mappedFile = keyMap.get(k);
                        if (file.equals(mappedFile)) {
                            nextValid = k;
                            return;
                        }
                    }
                    nextValid = null;
                }

                @Override
                public boolean hasNext() {
                    if (!computed) {
                        findNext();
                        computed = true;
                    }
                    return nextValid != null;
                }

                @Override
                public String next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    computed = false;
                    return nextValid;
                }
            };

            fileGroups.put(file, fileKeys);
        }

        return new GroupedKeys(fileGroups, () -> MAP_DB.closeMap(getKeyMapPath()));
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

    private Set<String> getKeyMapKeys() {
        final HTreeMap<String, String> keyMap = MAP_DB.openMap(getKeyMapPath());
        final var keys = new HashSet<String>();
        if (keyMap != null) {
            try {
                keys.addAll(keyMap.keySet());
            } finally {
                MAP_DB.closeMap(getKeyMapPath());
            }
        }

        return keys;
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
    default Map<String, V> get(final Iterable<String> keys) throws Exception {
        if (keys == null || !keys.iterator().hasNext()) {
            return Collections.emptyMap();
        }
        Logger.info("Querying multiple keys at [{}].", dataPath());
        final var map = new HashMap<String, V>(1000);

        if (getDataFiles().size() <= 1) {
            final var db = openDb();
            if (db != null) {
                try {
                    for (final var key : keys) {
                        final var value = db.getUsing(key, using());
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

        try (var keyFiles = getDbFiles(keys, getDataFiles())) {
            for (final var entry : keyFiles.fileGroups().entrySet()) {
                final var file = entry.getKey();
                final var db = openDb(file);
                if (db != null) {
                    try {
                        for (final var key : entry.getValue()) {
                            final var value = db.getUsing(key, using());
                            if (value != null) {
                                map.put(key, value);
                            }
                        }
                    } finally {
                        closeDb(file);
                    }
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
        final var file = getDbFile(key, getDataFiles());
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
        if (getDataFiles().size() > 1) {
            removeFromKeyMap(key);
        }

        final var indexFileNames = indexFileNames();
        CHRONICLE_UTILS.removeFromIndex(name(), dataPath(), indexFileNames, Map.of(key, value),
                averageValue().getClass(), indexExclusions());
        return true;
    }

    private void removeFromIndex(final Map<String, V> deletedMap) throws IOException {
        Logger.info("{} record(s) deleted at [{}].", deletedMap.size(), dataPath());
        CHRONICLE_UTILS.removeFromIndex(name(), dataPath(), indexFileNames(), deletedMap, averageValue().getClass(),
                indexExclusions());
    }

    /**
     * Remove a value using a list of keys
     * 
     * @param keys the keys to remove
     * @return true if updated else false
     * @throws IOException
     */
    default boolean delete(final Set<String> keys) throws Exception {
        if (keys == null || keys.isEmpty()) {
            return false;
        }

        final var deletedMap = new HashMap<String, V>();
        Logger.info("Deleting {} keys at [{}].", keys.size(), dataPath());

        if (getDataFiles().size() <= 1) {
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

        try (var keyFiles = getDbFiles(keys, getDataFiles())) {
            if (keyFiles.fileGroups().isEmpty()) {
                return false;
            }

            for (final var entry : keyFiles.fileGroups().entrySet()) {
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
    private void rotateFile(final ChronicleMap<String, V> db) throws IOException {
        // Reinitialize DATA_FILE_CACHE
        DATA_FILE_CACHE.remove(dataPath()); // Clear existing cache entry
        final var currentFiles = getDataFiles(); // Rebuild cache by scanning DATA_DIR
        final String rotatedFile = "data-" + (currentFiles.size() + 1);
        final var currentPath = Path.of(dataPath() + DATA_DIR + DATA_FILE);
        final var rotatedPath = Path.of(dataPath() + DATA_DIR + rotatedFile);

        // Check if rotated file already exists (extra safety)
        if (Files.exists(rotatedPath)) {
            throw new IOException(
                    "Rotated file [" + rotatedPath + "] already exists, aborting rotation to prevent data loss");
        }

        // Update key map using the provided ChronicleMap
        final HTreeMap<String, String> keyMap = MAP_DB.openMap(getKeyMapPath());
        if (keyMap != null) {
            try {
                db.forEachEntry(entry -> keyMap.put(entry.key().get(), rotatedFile));
            } finally {
                MAP_DB.closeMap(getKeyMapPath());
            }
        }

        // Move file to data-N
        Files.move(currentPath, rotatedPath, REPLACE_EXISTING);

        // Update cache
        currentFiles.add(rotatedFile);
        DATA_FILE_CACHE.put(dataPath(), currentFiles);
        Logger.info("Rotated data file at [{}] to {}.", dataPath(), rotatedFile);
    }

    private ChronicleMap<String, V> checkAndRotate(final ChronicleMap<String, V> db) throws IOException {
        if (db.size() >= entries()) {
            rotateFile(db); // Pass db instead of closing
            closeDb(); // Close after rotation
            return openDb(); // Open new empty data file (already created in rotateFile)
        }
        return db;
    }

    private ChronicleMap<String, V> checkAndRotate(final ChronicleMap<String, V> db,
            final Map<String, String> keyMapUpdate)
            throws IOException {
        if (db.size() >= entries()) {
            rotateFile(db); // Pass db instead of closing
            closeDb(); // Close after rotation
            keyMapUpdate.clear();
            return openDb(); // Open new empty data file (already created in rotateFile)
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
        var file = DATA_FILE;
        synchronized (lock) {
            file = getDbFile(key, getDataFiles());
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
                    Map.of(key, prevValue), averageValue().getClass(), indexExclusions());
            status = PutStatus.UPDATED;
        } else {
            if (getDataFiles().size() > 1) {
                addToKeyMap(key, file);
            }
            CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames, Map.of(key, value),
                    Collections.emptyMap(), averageValue().getClass(), indexExclusions());
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
                    Map.of(key, prevValue), averageValue().getClass(), indexExclusions());
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

        var file = DATA_FILE;
        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            file = getDbFile(key, getDataFiles());
            if (file == null) {
                file = DATA_FILE;
            }
            if (file != null && !DATA_FILE.equals(file)) {
                return PutStatus.FAILED; // already exists
            }

            var db = openDb();
            if (db != null) {
                try {
                    db = checkAndRotate(db);
                    db.put(key, value);
                } finally {
                    closeDb();
                }
            }
        }

        if (getDataFiles().size() > 1) {
            addToKeyMap(key, DATA_FILE);
        }
        CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames, Map.of(key, value), Collections.emptyMap(),
                averageValue().getClass(), indexExclusions());
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
    default PutStatus put(final Map<String, V> map) throws Exception {
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
            try (var grouped = getDbFiles(keysToInsert, getDataFiles())) {
                for (final var entry : grouped.fileGroups().entrySet()) {
                    final var file = entry.getKey();
                    final var db = openDb(file);

                    if (db != null) {
                        try {
                            for (final String key : entry.getValue()) {
                                final V prevValue = db.put(key, map.get(key));
                                if (prevValue != null) {
                                    prevValues.put(key, prevValue);
                                    keysToInsert.remove(key); // Optional: only if needed for downstream
                                }
                            }
                        } finally {
                            closeDb(file);
                        }
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
                            if (getDataFiles().size() > 1) {
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

        CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames(), map, prevValues, averageValue().getClass(),
                indexExclusions());
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
    default PutStatus update(final Map<String, V> map) throws Exception {
        if (map == null || map.isEmpty()) {
            return PutStatus.FAILED;
        }

        var status = PutStatus.UPDATED;
        final var mapSize = map.size();
        final var prevValues = new HashMap<String, V>(mapSize);
        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            try (var grouped = getDbFiles(map.keySet(), getDataFiles())) {
                for (final var entry : grouped.fileGroups().entrySet()) {
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

        CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames(), map, prevValues, averageValue().getClass(),
                indexExclusions());
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
            var db = openDb();
            if (db != null) {
                try {
                    for (final var entry : map.entrySet()) {
                        final String key = entry.getKey();
                        final V value = entry.getValue();
                        db = checkAndRotate(db, keyMapUpdate);
                        db.put(key, value);
                        if (getDataFiles().size() > 1) {
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

        CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames(), map, Collections.emptyMap(),
                averageValue().getClass(), indexExclusions());
        Logger.info("Inserted {} records at [{}].", putSize, dataPath());

        return PutStatus.INSERTED;
    }

    default Map<String, V> search(final Iterable<String> keys, final List<Search> filters) throws Throwable {
        if (keys == null || !keys.iterator().hasNext()) {
            return Collections.emptyMap();
        }

        Logger.info("Querying filtered keys at [{}] with [{}] remaining filters. {}", dataPath(), filters.size(),
                filters);
        final var map = new ConcurrentHashMap<String, V>(10_000);

        // Determine minimum positive limit across all filters
        final int limit = filters.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(HARD_LIMIT);

        if (getDataFiles().size() <= 1) {
            final var db = openDb();
            if (db != null) {
                final AtomicInteger count = new AtomicInteger();
                try {
                    StreamSupport.stream(keys.spliterator(), true)
                            .forEach(key -> {
                                final V value = db.getUsing(key, using());
                                if (value == null)
                                    return;

                                boolean match = true;
                                for (final var search : filters) {
                                    try {
                                        if (!CHRONICLE_UTILS.search(search, key, value)) {
                                            match = false;
                                            break;
                                        }
                                    } catch (final Throwable e) {
                                        Logger.error("Search failed for key [{}]: {}", key);
                                        Logger.error(e);
                                    }
                                }

                                if (match) {
                                    if (count.incrementAndGet() > limit)
                                        return;
                                    map.put(key, value);
                                }
                            });
                } finally {
                    closeDb();
                }
            }
            return map;
        }

        try (var grouped = getDbFiles(keys, getDataFiles())) {
            final AtomicInteger count = new AtomicInteger();

            for (final var entry : grouped.fileGroups().entrySet()) {
                if (count.get() >= limit)
                    break;

                final var file = entry.getKey();
                final var keysForFile = entry.getValue(); // Iterable<String>
                final var db = openDb(file);

                if (db != null) {
                    try {
                        StreamSupport.stream(keysForFile.spliterator(), true) // true = parallel
                                .forEach(key -> {
                                    final V value = db.getUsing(key, using());
                                    if (value == null)
                                        return;

                                    boolean match = true;
                                    for (final var search : filters) {
                                        try {
                                            if (!CHRONICLE_UTILS.search(search, key, value)) {
                                                match = false;
                                                break;
                                            }
                                        } catch (final Throwable e) {
                                            Logger.error("Search failed for key [{}]: {}", key, e.toString());
                                        }
                                    }

                                    if (match) {
                                        if (count.incrementAndGet() > limit)
                                            return;
                                        map.put(key, value);
                                    }
                                });
                    } finally {
                        closeDb(file);
                    }
                }
            }
        }

        return map;
    }

    default Map<String, V> search(final Iterable<String> keys, final List<Search> filters,
            final Set<String> excludedKeys) throws Throwable {
        if (keys == null || !keys.iterator().hasNext()) {
            return Collections.emptyMap();
        }

        Logger.info("Querying filtered keys at [{}] with [{}] remaining filters and [{}] excluded keys. {}", dataPath(),
                filters.size(), excludedKeys.size(), filters);
        final var map = new ConcurrentHashMap<String, V>(10_000);

        // Determine minimum positive limit across all filters
        final int limit = filters.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(HARD_LIMIT);

        if (getDataFiles().size() <= 1) {
            final var db = openDb();
            if (db != null) {
                final AtomicInteger count = new AtomicInteger();
                try {
                    StreamSupport.stream(keys.spliterator(), true)
                            .forEach(key -> {
                                if (!excludedKeys.contains(key)) {
                                    final V value = db.getUsing(key, using());
                                    if (value == null)
                                        return;

                                    boolean match = true;
                                    for (final var search : filters) {
                                        try {
                                            if (!CHRONICLE_UTILS.search(search, key, value)) {
                                                match = false;
                                                break;
                                            }
                                        } catch (final Throwable e) {
                                            Logger.error("Search failed for key [{}]: {}", key);
                                            Logger.error(e);
                                        }
                                    }

                                    if (match) {
                                        if (count.incrementAndGet() > limit)
                                            return;
                                        map.put(key, value);
                                    }
                                }
                            });
                } finally {
                    closeDb();
                }
            }
            return map;
        }

        try (var grouped = getDbFiles(keys, getDataFiles())) {
            final AtomicInteger count = new AtomicInteger();

            for (final var entry : grouped.fileGroups().entrySet()) {
                if (count.get() >= limit)
                    break;

                final var file = entry.getKey();
                final var keysForFile = entry.getValue(); // Iterable<String>
                final var db = openDb(file);

                if (db != null) {
                    try {
                        StreamSupport.stream(keysForFile.spliterator(), true) // true = parallel
                                .forEach(key -> {
                                    if (!excludedKeys.contains(key)) {
                                        final V value = db.getUsing(key, using());
                                        if (value == null)
                                            return;

                                        boolean match = true;
                                        for (final var search : filters) {
                                            try {
                                                if (!CHRONICLE_UTILS.search(search, key, value)) {
                                                    match = false;
                                                    break;
                                                }
                                            } catch (final Throwable e) {
                                                Logger.error("Search failed for key [{}]: {}", key, e.toString());
                                            }
                                        }

                                        if (match) {
                                            if (count.incrementAndGet() > limit)
                                                return;
                                            map.put(key, value);
                                        }
                                    }
                                });
                    } finally {
                        closeDb(file);
                    }
                }
            }
        }

        return map;
    }

    default long searchCount(final Iterable<String> keys, final List<Search> filters) throws Throwable {
        if (keys == null || !keys.iterator().hasNext()) {
            return 0;
        }

        Logger.info("Counting filtered keys at [{}] with [{}] remaining filters. {}", dataPath(), filters.size(),
                filters);

        // Determine minimum positive limit across all filters
        final int limit = filters.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(Integer.MAX_VALUE);
        final var count = new LongAdder();

        if (getDataFiles().size() <= 1) {
            final var db = openDb();
            if (db != null) {
                try {
                    StreamSupport.stream(keys.spliterator(), true)
                            .forEach(key -> {
                                final V value = db.getUsing(key, using());
                                if (value == null)
                                    return;

                                boolean match = true;
                                for (final var search : filters) {
                                    try {
                                        if (!CHRONICLE_UTILS.search(search, key, value)) {
                                            match = false;
                                            break;
                                        }
                                    } catch (final Throwable e) {
                                        Logger.error("Search failed for key [{}]: {}", key);
                                        Logger.error(e);
                                    }
                                }

                                if (match) {
                                    count.increment();
                                    if (count.sum() > limit)
                                        return;
                                }
                            });
                } finally {
                    closeDb();
                }
            }
            return count.sum();
        }

        try (var grouped = getDbFiles(keys, getDataFiles())) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                if (count.sum() > limit)
                    break;

                final var file = entry.getKey();
                final var keysForFile = entry.getValue(); // Iterable<String>
                final var db = openDb(file);

                if (db != null) {
                    try {
                        StreamSupport.stream(keysForFile.spliterator(), true) // true = parallel
                                .forEach(key -> {
                                    final V value = db.getUsing(key, using());
                                    if (value == null)
                                        return;

                                    boolean match = true;
                                    for (final var search : filters) {
                                        try {
                                            if (!CHRONICLE_UTILS.search(search, key, value)) {
                                                match = false;
                                                break;
                                            }
                                        } catch (final Throwable e) {
                                            Logger.error("Search failed for key [{}]: {}", key, e.toString());
                                        }
                                    }

                                    if (match) {
                                        count.increment();
                                        if (count.sum() > limit)
                                            return;
                                    }
                                });
                    } finally {
                        closeDb(file);
                    }
                }
            }
        }

        return count.sum();
    }

    /**
     * When no db provided, use forEachEntry
     */
    private Map<String, V> search(final ChronicleMap<String, V> db, final List<Search> filters) {
        Logger.info("Searching DB at [{}] for {} filters.", dataPath(), filters.size());
        final Map<String, V> result = new HashMap<>(10_000);
        final int limit = filters.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(HARD_LIMIT);

        db.forEachEntryWhile(entry -> {
            try {
                if (result.size() >= limit) {
                    return false; // Early exit
                }

                final String key = entry.key().get();
                final V value = entry.value().get();
                if (value == null) {
                    return true;
                }

                for (final Search search : filters) {
                    if (!CHRONICLE_UTILS.search(search, key, value)) {
                        return true;
                    }
                }

                result.put(key, db.getUsing(key, using()));
            } catch (final Throwable e) {
                Logger.error("Search failed during multi-filter scan.");
                Logger.error(e);
            }
            return true;
        });

        return result;
    }

    private Map<String, V> search(final ChronicleMap<String, V> db, final List<Search> filters, final int limit) {
        Logger.info("Searching DB at [{}] for {} filters.", dataPath(), filters.size());
        final Map<String, V> result = new HashMap<>(10_000);

        db.forEachEntryWhile(entry -> {
            try {
                if (result.size() >= limit) {
                    return false; // Early exit
                }

                final String key = entry.key().get();
                final V value = entry.value().get();
                if (value == null) {
                    return true;
                }

                for (final Search search : filters) {
                    if (!CHRONICLE_UTILS.search(search, key, value)) {
                        return true;
                    }
                }

                result.put(key, db.getUsing(key, using()));
            } catch (final Throwable e) {
                Logger.error("Search failed during multi-filter scan.");
                Logger.error(e);
            }
            return true;
        });

        return result;
    }

    /**
     * Same as above, just has excluded keys
     */
    private Map<String, V> search(final ChronicleMap<String, V> db, final List<Search> filters,
            final Set<String> excludedKeys) {
        Logger.info("Searching DB at [{}] using [{}] filters and [{}] excluded keys. Filters: {}", dataPath(),
                filters.size(), excludedKeys.size(), filters);
        final Map<String, V> result = new HashMap<>(10_000);
        final int limit = filters.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(HARD_LIMIT);

        db.forEachEntryWhile(entry -> {
            try {
                if (result.size() >= limit) {
                    return false; // Early exit
                }

                final String key = entry.key().get();
                if (excludedKeys.contains(key)) {
                    return true;
                }

                final V value = entry.value().get();
                if (value == null) {
                    return true;
                }

                for (final Search search : filters) {
                    if (!CHRONICLE_UTILS.search(search, key, value)) {
                        return true;
                    }
                }

                result.put(key, db.getUsing(key, using()));
            } catch (final Throwable e) {
                Logger.error("Search failed during multi-filter scan.");
                Logger.error(e);
            }
            return true;
        });

        return result;
    }

    private Map<String, V> search(final ChronicleMap<String, V> db, final List<Search> filters,
            final Set<String> excludedKeys, final int limit) {
        Logger.info("Searching DB at [{}] using [{}] filters and [{}] excluded keys. Filters: {}, Limit [{}]",
                dataPath(),
                filters.size(), excludedKeys.size(), filters, limit);
        final Map<String, V> result = new HashMap<>(10_000);

        db.forEachEntryWhile(entry -> {
            try {
                if (result.size() >= limit) {
                    return false; // Early exit
                }

                final String key = entry.key().get();
                if (excludedKeys.contains(key)) {
                    return true;
                }

                final V value = entry.value().get();
                if (value == null) {
                    return true;
                }

                for (final Search search : filters) {
                    if (!CHRONICLE_UTILS.search(search, key, value)) {
                        return true;
                    }
                }

                result.put(key, db.getUsing(key, using()));
            } catch (final Throwable e) {
                Logger.error("Search failed during multi-filter scan.");
                Logger.error(e);
            }
            return true;
        });

        return result;
    }

    private int searchCount(final ChronicleMap<String, V> db, final List<Search> filters) {
        Logger.info("Counting DB at [{}] for {} filters.", dataPath(), filters.size());
        final int limit = filters.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(Integer.MAX_VALUE);
        final var count = new AtomicInteger();

        db.forEachEntryWhile(entry -> {
            try {
                final String key = entry.key().get();
                final V value = entry.value().get();
                if (value == null) {
                    return true;
                }

                for (final Search search : filters) {
                    if (!CHRONICLE_UTILS.search(search, key, value)) {
                        return true;
                    }
                }

                if (count.incrementAndGet() > limit) {
                    return false; // Strict limit enforcement
                }
            } catch (final Throwable e) {
                Logger.error("Search failed during multi-filter scan.");
                Logger.error(e);
            }
            return true;
        });

        return count.get();
    }

    private Map<String, V> search(final Map<String, V> db, final List<Search> filters) {
        Logger.info("Searching in-memory map with [{}] filters.", filters.size());
        final Map<String, V> result = new HashMap<>(Math.min(db.size(), 10_000));

        for (final Map.Entry<String, V> entry : db.entrySet()) {
            final String key = entry.getKey();
            final V value = entry.getValue();

            if (value == null) {
                continue;
            }

            boolean match = true;
            for (final Search search : filters) {
                try {
                    if (!CHRONICLE_UTILS.search(search, key, value)) {
                        match = false;
                        break;
                    }
                } catch (final Throwable e) {
                    Logger.error("Search failed on key [{}] during filter scan.", key);
                    Logger.error(e);
                    match = false;
                    break;
                }
            }

            if (match) {
                result.put(key, value);
            }
        }

        return result;
    }

    /**
     * Searches the objects using an index, without needed to loop over every record
     * Only useful for @code SearchType.EQUAL and @code SearchType.NOT_EQUAL
     * 
     * @param search the Search object
     * @param db
     * @param index
     */
    default SearchResult indexedSearch(final Search search, final NavigableSet<byte[]> index) {
        Logger.info("Index searching at [{}] for {}.", dataPath(), search);
        if (index == null || index.isEmpty()) {
            return new SearchResult(Collections.emptyList());
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
                final var keysAfter = MAP_DB.getAfterIndexSearch(index, searchTerm, search.limit());
                return new SearchResult(
                        CHRONICLE_UTILS.concatIterable(keysBefore.results(), keysAfter.results(), search.limit()));
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

    default SearchResult indexedSearch(final Search search, final NavigableSet<byte[]> index,
            final Set<String> excludedKeys) {
        Logger.info("Index searching at [{}] with [{}] excluded keys for {}.", dataPath(), excludedKeys.size(), search);
        if (index == null || index.isEmpty()) {
            return new SearchResult(Collections.emptyList());
        }

        final SearchType searchType = search.searchType();
        final String searchTerm = String.valueOf(search.searchTerm());
        final Set<String> searchTermSet = (searchType == SearchType.IN || searchType == SearchType.NOT_IN)
                ? new HashSet<>((List<String>) search.searchTerm())
                : null;
        final var searchTermBetween = searchType == SearchType.BETWEEN ? (List<Object>) search.searchTerm() : null;

        switch (searchType) {
            case EQUAL -> {
                return MAP_DB.getEqualIndexSearch(index, searchTerm, search.limit(), excludedKeys);
            }
            case NOT_EQUAL -> {
                final var keysBefore = MAP_DB.getBeforeIndexSearch(index, searchTerm, search.limit(), excludedKeys);
                final var keysAfter = MAP_DB.getAfterIndexSearch(index, searchTerm, search.limit(), excludedKeys);
                return new SearchResult(
                        CHRONICLE_UTILS.concatIterable(keysBefore.results(), keysAfter.results(), search.limit()));
            }
            case LESS -> {
                return MAP_DB.getLessThanIndexSearch(index, searchTerm, search.limit(), excludedKeys);
            }
            case LESS_OR_EQUAL -> {
                return MAP_DB.getLessThanOrEqualIndexSearch(index, searchTerm, search.limit(), excludedKeys);
            }
            case GREATER -> {
                return MAP_DB.getGreaterThanIndexSearch(index, searchTerm, search.limit(), excludedKeys);
            }
            case GREATER_OR_EQUAL -> {
                return MAP_DB.getGreaterThanOrEqualIndexSearch(index, searchTerm, search.limit(), excludedKeys);
            }
            case LIKE -> {
                return MAP_DB.getLikeIndexSearch(index, searchTerm, search.limit(), excludedKeys);
            }
            case NOT_LIKE -> {
                return MAP_DB.getNotLikeIndexSearch(index, searchTerm, search.limit(), excludedKeys);
            }
            case STARTS_WITH -> {
                return MAP_DB.getStartsWithIndexSearch(index, searchTerm, search.limit(), excludedKeys);
            }
            case ENDS_WITH -> {
                return MAP_DB.getEndsWithIndexSearch(index, searchTerm, search.limit(), excludedKeys);
            }
            case IN -> {
                return MAP_DB.getInIndexSearch(index, searchTermSet, search.limit(), excludedKeys);
            }
            case NOT_IN -> {
                return MAP_DB.getNotInIndexSearch(index, searchTermSet, search.limit(), excludedKeys);
            }
            case BETWEEN -> {
                return MAP_DB.getBetweenIndexSearch(index, searchTermBetween.get(0).toString(),
                        searchTermBetween.get(1).toString(), search.limit(), excludedKeys);
            }
            default -> throw new UnsupportedOperationException("Search type not supported: " + searchType);
        }
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

    private <T> boolean isResultEmpty(final Iterable<T> result) {
        return result == null || !result.iterator().hasNext();
    }

    default Map<String, V> indexedSearch(final Search search) throws Exception {
        final String indexPath = getIndexPath(search.field());
        final var indexDb = MAP_DB.openIndex(indexPath);
        if (indexDb != null) {
            try {
                final var searchResult = indexedSearch(search, indexDb);
                if (isResultEmpty(searchResult.results())) {
                    return Collections.emptyMap();
                }

                return get(toStringIterable(searchResult.results()));
            } finally {
                MAP_DB.closeIndex(indexPath);
            }
        }
        return Collections.emptyMap();
    }

    default Map<String, V> indexedSearch(final Search search, final Set<String> excludedKeys) throws Exception {
        final String indexPath = getIndexPath(search.field());
        final var indexDb = MAP_DB.openIndex(indexPath);
        if (indexDb != null) {
            try {
                final var searchResult = indexedSearch(search, indexDb, excludedKeys);
                if (isResultEmpty(searchResult.results())) {
                    return Collections.emptyMap();
                }

                return get(toStringIterable(searchResult.results()));
            } finally {
                MAP_DB.closeIndex(indexPath);
            }
        }
        return Collections.emptyMap();
    }

    default Map<String, V> indexedSearch(final Set<String> matchingKeys, final Search search) throws Throwable {
        if (matchingKeys == null || matchingKeys.isEmpty()) {
            return Collections.emptyMap();
        }

        return search(matchingKeys, List.of(search));
    }

    default Map<String, V> search(final Map<String, V> db, final Search search) throws IOException {
        if (db == null || db.isEmpty()) {
            return Collections.emptyMap();
        }
        return search(db, List.of(search));
    }

    default Map<String, V> search(final Search search) throws IOException {
        final Map<String, V> result = new HashMap<>();

        for (final String file : getDataFiles()) {
            if (result.size() >= search.limit()) {
                break;
            }
            final var db = openDb(file);
            if (db != null) {
                try {
                    result.putAll(search(db, List.of(search), search.limit()));
                } finally {
                    closeDb(file);
                }
            }
        }

        return result;
    }

    default Map<String, V> search(final Search search, final Set<String> excludedKeys) throws IOException {
        final Map<String, V> result = new HashMap<>();

        for (final String file : getDataFiles()) {
            if (result.size() >= search.limit()) {
                break;
            }
            final var db = openDb(file);
            if (db != null) {
                try {
                    result.putAll(search(db, List.of(search), excludedKeys, search.limit()));
                } finally {
                    closeDb(file);
                }
            }
        }

        return result;
    }

    default Map<String, V> multiSearch(final List<Search> searches) throws Throwable {
        if (searches == null || searches.isEmpty()) {
            return Collections.emptyMap();
        }

        final Set<String> indexFileNames = indexFileNames();

        // Step 1: Separate searches into first-indexed and remaining
        Search indexedSearch = null;
        final List<Search> remainingSearches = new ArrayList<>();

        for (final Search s : searches) {
            if (!s.skipIndex() && indexedSearch == null && indexFileNames.contains(s.field())) {
                indexedSearch = s;
            } else {
                remainingSearches.add(s);
            }
        }

        SearchResult searchResult = null;
        String indexPath = null;
        NavigableSet<byte[]> indexDb = null;
        // Step 2: Perform indexed search (only if indexedSearch != null)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            indexDb = MAP_DB.openIndex(indexPath);

            if (indexDb != null) {
                searchResult = indexedSearch(indexedSearch, indexDb);
                if (isResultEmpty(searchResult.results())) {
                    MAP_DB.closeIndex(indexPath);
                    return Collections.emptyMap();
                }

                if (remainingSearches.isEmpty()) {
                    try {
                        return get(toStringIterable(searchResult.results()));
                    } finally {
                        MAP_DB.closeIndex(indexPath);
                    }
                }
            }
        }

        // Step 3: Manual search
        try {
            if (searchResult == null) {
                final Map<String, V> result = new ConcurrentHashMap<>();
                final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min()
                        .orElse(HARD_LIMIT);
                final var count = new AtomicInteger(0);

                getDataFiles().parallelStream().forEach(file -> {
                    if (count.get() >= limit)
                        return;
                    try {
                        final var db = openDb(file);
                        if (db != null) {
                            try {
                                final Map<String, V> partial = search(db, searches);
                                for (final Map.Entry<String, V> entry : partial.entrySet()) {
                                    if (count.incrementAndGet() > limit)
                                        break;
                                    result.put(entry.getKey(), entry.getValue());
                                }
                            } finally {
                                closeDb(file);
                            }
                        }
                    } catch (final IOException e) {
                        Logger.error("Count not search db file at [{}], file [{}]", dataPath(), file);
                    }
                });

                return result;
            } else {
                return search(toStringIterable(searchResult.results()), remainingSearches);
            }
        } finally {
            if (indexDb != null) {
                MAP_DB.closeIndex(indexPath);
            }
        }
    }

    default Map<String, V> multiSearch(final List<Search> searches, final Set<String> excludedKeys) throws Throwable {
        if (searches == null || searches.isEmpty()) {
            return Collections.emptyMap();
        }

        final Set<String> indexFileNames = indexFileNames();

        // Step 1: Separate searches into first-indexed and remaining
        Search indexedSearch = null;
        final List<Search> remainingSearches = new ArrayList<>();

        for (final Search s : searches) {
            if (!s.skipIndex() && indexedSearch == null && indexFileNames.contains(s.field())) {
                indexedSearch = s;
            } else {
                remainingSearches.add(s);
            }
        }

        SearchResult searchResult = null;
        String indexPath = null;
        NavigableSet<byte[]> indexDb = null;
        // Step 2: Perform indexed search (only if indexedSearch != null)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            indexDb = MAP_DB.openIndex(indexPath);

            if (indexDb != null) {
                searchResult = indexedSearch(indexedSearch, indexDb, excludedKeys);
                if (isResultEmpty(searchResult.results())) {
                    MAP_DB.closeIndex(indexPath);
                    return Collections.emptyMap();
                }

                if (remainingSearches.isEmpty()) {
                    try {
                        return get(toStringIterable(searchResult.results()));
                    } finally {
                        MAP_DB.closeIndex(indexPath);
                    }
                }
            }
        }

        // Step 3: Manual search
        try {
            if (searchResult == null) {
                final Map<String, V> result = new ConcurrentHashMap<>();
                final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min()
                        .orElse(HARD_LIMIT);
                final var count = new AtomicInteger(0);

                getDataFiles().parallelStream().forEach(file -> {
                    if (count.get() >= limit)
                        return;
                    try {
                        final var db = openDb(file);
                        if (db != null) {
                            try {
                                final Map<String, V> partial = search(db, searches, excludedKeys);
                                for (final Map.Entry<String, V> entry : partial.entrySet()) {
                                    if (count.incrementAndGet() > limit)
                                        break;
                                    result.put(entry.getKey(), entry.getValue());
                                }
                            } finally {
                                closeDb(file);
                            }
                        }
                    } catch (final IOException e) {
                        Logger.error("Count not search db file at [{}], file [{}]", dataPath(), file);
                    }
                });

                return result;
            } else {
                return search(toStringIterable(searchResult.results()), remainingSearches, excludedKeys);
            }
        } finally {
            if (indexDb != null) {
                MAP_DB.closeIndex(indexPath);
            }
        }
    }

    default Map<String, V> multiSearch(final Set<String> matchingKeys, final List<Search> searches) throws Throwable {
        if (searches == null || searches.isEmpty() || matchingKeys == null || matchingKeys.isEmpty()) {
            return Collections.emptyMap();
        }

        return search(matchingKeys, searches);
    }

    default long multiSearchCount(final List<Search> searches) throws Throwable {
        if (searches == null || searches.isEmpty()) {
            return 0;
        }

        final Set<String> indexFileNames = indexFileNames();

        // Step 1: Separate searches into one indexed + remaining
        Search indexedSearch = null;
        final List<Search> remainingSearches = new ArrayList<>();

        for (final Search s : searches) {
            if (indexedSearch == null && indexFileNames.contains(s.field())) {
                indexedSearch = s;
            } else {
                remainingSearches.add(s);
            }
        }

        String indexPath = null;
        NavigableSet<byte[]> indexDb = null;
        // Step 2: Indexed search (only first index used)
        SearchResult searchResult = null;
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            indexDb = MAP_DB.openIndex(indexPath);
            if (indexDb != null) {
                searchResult = indexedSearch(indexedSearch, indexDb);
                if (isResultEmpty(searchResult.results())) {
                    MAP_DB.closeIndex(indexPath);
                    return 0;
                }
                if (remainingSearches.isEmpty()) {
                    try {
                        return MAP_DB.fastCount(searchResult.results());
                    } finally {
                        MAP_DB.closeIndex(indexPath);
                    }
                }
            }
        }

        try {
            if (searchResult == null) {
                return getDataFiles().parallelStream()
                        .mapToInt(file -> {
                            try {
                                final var db = openDb(file);
                                if (db != null) {
                                    try {
                                        return searchCount(db, searches);
                                    } finally {
                                        closeDb(file);
                                    }
                                }
                            } catch (final IOException e) {
                                Logger.error("Could not count db file at [{}], file [{}]", dataPath(), file);
                            }
                            return 0;
                        })
                        .sum();
            } else {
                return searchCount(toStringIterable(searchResult.results()), remainingSearches);
            }
        } finally {
            if (indexDb != null) {
                MAP_DB.closeIndex(indexPath);
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

        if (getDataFiles().size() > 1) {
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
        for (final var file : getDataFiles()) {
            CHRONICLE_UTILS.deleteFileIfExists(dataPath() + DATA_DIR + file);
        }
    }

    default boolean exists(final String key) throws IOException {
        Logger.info("Checking [{}] existence at [{}].", key, dataPath());

        if (getDataFiles().size() > 1) {
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

        if (getDataFiles().size() > 1) {
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

        if (getDataFiles().size() > 1) {
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

        if (getDataFiles().size() > 1) {
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
