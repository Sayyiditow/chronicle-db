package chronicle.db.dao;

import static chronicle.db.dao.ChronicleUtils.CHRONICLE_UTILS;
import static chronicle.db.service.ChronicleDb.CHRONICLE_DB;
import static chronicle.db.service.MapDb.MAP_DB;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.mapdb.HTreeMap;
import org.tinylog.Logger;

import com.jsoniter.spi.TypeLiteral;

import chronicle.db.entity.CsvObject;
import chronicle.db.entity.PutStatus;
import chronicle.db.entity.Search;
import chronicle.db.entity.Search.SearchType;
import chronicle.db.service.ChronicleDb.SharedChronicleMap;
import chronicle.db.service.MapDb.SearchResult;
import chronicle.db.service.MapDb.SharedIndexMap;
import net.openhft.chronicle.bytes.UTFDataFormatRuntimeException;
import net.openhft.chronicle.map.ChronicleMap;

/**
 * High-performance, disk-persisted data access object built on ChronicleMap and
 * MapDB.
 * <p>
 * ChronicleDao provides a complete CRUD interface with advanced features
 * including:
 * <ul>
 * <li><b>File Rotation (Sharding):</b> Automatically splits data across
 * multiple files when size limits are reached</li>
 * <li><b>Secondary Indexes:</b> Fast lookups on any field using MapDB
 * indexes</li>
 * <li><b>Advanced Search:</b> Support for EQUAL, RANGE, LIKE, IN, BETWEEN, and
 * more</li>
 * <li><b>CSV Export:</b> Query results can be returned in CSV format</li>
 * <li><b>Subset Queries:</b> Retrieve only specific fields to reduce memory
 * usage</li>
 * <li><b>Batch Operations:</b> Efficient bulk insert/update/delete</li>
 * <li><b>Backup & Recovery:</b> Built-in backup and data recovery
 * mechanisms</li>
 * <li><b>Vacuum:</b> Reclaim disk space from deleted records</li>
 * </ul>
 * </p>
 * 
 * <p>
 * <b>Implementation:</b> To use ChronicleDao, create an interface that extends
 * this interface
 * and implement the required configuration methods. The interface uses default
 * methods to
 * provide all CRUD functionality.
 * </p>
 * 
 * <p>
 * <b>Example:</b>
 * 
 * <pre>
 * {
 *     &#64;code
 *     public interface UserDao extends ChronicleDao<User> {
 *         UserDao USER_DAO = new UserDao() {
 *         };
 * 
 *         &#64;Override
 *         default String dataPath() {
 *             return "users";
 *         }
 * 
 *         &#64;Override
 *         default long entries() {
 *             return 10000;
 *         }
 * 
 *         &#64;Override
 *         default String averageKey() {
 *             return "user123";
 *         }
 * 
 *         &#64;Override
 *         default User averageValue() {
 *             return new User();
 *         }
 * 
 *         &#64;Override
 *         default User using() {
 *             return new User();
 *         }
 * 
 *         @Override
 *         default TypeLiteral<User> jsonType() {
 *             return new TypeLiteral<User>() {
 *             };
 *         }
 *     }
 * 
 *     // Usage
 *     User user = new User("user123", "John Doe");
 *     USER_DAO.put("user123", user);
 *     User retrieved = USER_DAO.get("user123");
 * }
 * </pre>
 * </p>
 * 
 * <p>
 * <b>Thread Safety:</b> All operations are thread-safe. Write operations use
 * synchronized
 * locks per data path to prevent conflicts during file rotation and index
 * updates.
 * </p>
 * 
 * <p>
 * <b>File Structure:</b>
 * <ul>
 * <li>{@code /data/} - ChronicleMap data files (sharded)</li>
 * <li>{@code /indexes/} - MapDB secondary indexes</li>
 * <li>{@code /files/} - Metadata files (keys, entry sizes)</li>
 * <li>{@code /backup/} - Backup files</li>
 * </ul>
 * </p>
 * 
 * @param <V> The entity type stored in this DAO
 */
@SuppressWarnings("unchecked")
public interface ChronicleDao<V> {
    /**
     * Represents the state of data files for a DAO instance.
     * <p>
     * Tracks all data file names (shards) and the current active file for writes.
     * Used internally for file rotation management.
     * </p>
     * 
     * @param fileNames   Set of all data file names for this DAO
     * @param currentFile The currently active file for write operations
     */
    static record DataFileState(Set<String> fileNames, String currentFile) {
        /**
         * Creates a new DataFileState with an updated current file.
         * 
         * @param currentFile The new current file name
         * @return A new DataFileState instance
         */
        public DataFileState withCurrentFile(final String currentFile) {
            return new DataFileState(this.fileNames, currentFile);
        }
    }

    ConcurrentMap<String, Object> WRITE_LOCKS = new ConcurrentHashMap<>();
    ConcurrentMap<String, DataFileState> DATA_FILE_CACHE = new ConcurrentHashMap<>();
    String DATA_DIR = "/data/", INDEX_DIR = "/indexes/", FILES_DIR = "/files/", BACKUP_DIR = "/backup/",
            DATA_FILE = "data", CORRUPTED_FILE = "corrupted", RECOVER_FILE = "recovery", ENTRY_SIZE_FILE = "entrySize",
            KEY_FILE = "keys";
    String[] DB_DIRS = { DATA_DIR, INDEX_DIR, FILES_DIR, BACKUP_DIR };
    int HARD_LIMIT = 100_000;

    /**
     * Returns the name of this DAO for logging and identification purposes.
     * <p>
     * Defaults to the simple class name of the entity type.
     * Override to provide a custom name.
     * </p>
     * 
     * @return The DAO name (defaults to entity class simple name)
     */
    default String name() {
        return averageValue().getClass().getSimpleName();
    }

    /**
     * Returns the expected number of entries per data file (shard).
     * <p>
     * This value is used to pre-allocate ChronicleMap capacity. When a file
     * reaches this limit, a new file is created (file rotation). Set this to
     * a reasonable estimate of your data size to optimize performance.
     * </p>
     * 
     * @return Expected entries per file
     */
    long entries();

    /**
     * Returns a representative average key for sizing calculations.
     * <p>
     * ChronicleMap uses this to estimate memory requirements. Provide a
     * typical key that represents the average length and structure.
     * </p>
     * 
     * @return A sample key (e.g., "user12345")
     */
    String averageKey();

    /**
     * Returns a representative average entity instance for sizing calculations.
     * <p>
     * ChronicleMap uses this to estimate memory requirements. Provide a
     * typical entity that represents the average size and structure.
     * </p>
     * 
     * @return A sample entity instance
     */
    V averageValue();

    /**
     * Returns the base directory path where all DAO files will be stored.
     * <p>
     * This path is relative to the system's chronicle.db.path property.
     * All data files, indexes, and metadata will be created under this path.
     * </p>
     * 
     * @return The data path (e.g., "users", "transactions")
     */
    String dataPath();

    /**
     * Returns a reusable entity instance for deserialization.
     * <p>
     * This instance is used internally for JSON deserialization to avoid
     * repeated object allocation. Should return a new instance each time.
     * </p>
     * 
     * @return A new entity instance
     */
    V using();

    /**
     * Returns the TypeLiteral for JSON deserialization.
     * <p>
     * Used by JsonIter to deserialize JSON strings back to entity objects.
     * Typically implemented as: {@code return new TypeLiteral<MyEntity>() {};}
     * </p>
     * 
     * @return TypeLiteral for the entity type
     */
    TypeLiteral<V> jsonType();

    /**
     * Returns the bloat factor for ChronicleMap capacity planning.
     * <p>
     * Use values > 1.0 when entities can grow significantly larger than the
     * average value. For example, if your average entity is 1KB but some can
     * be 5KB, use a bloat factor of 5.0. Defaults to 1.0.
     * </p>
     * 
     * @return The bloat factor (default: 1.0)
     */
    default double bloatFactor() {
        return 1;
    }

    /**
     * Returns the set of field names that should have secondary indexes.
     * <p>
     * Indexed fields enable fast lookups using the search methods. Only index
     * fields that are frequently queried. Each index adds overhead to write
     * operations.
     * </p>
     * 
     * @return Set of field names to index (default: empty)
     */
    default Set<String> indexFileNames() {
        return Collections.emptySet();
    }

    /**
     * Returns a map of index exclusions for specific field values.
     * <p>
     * Use this to exclude certain values from being indexed. For example,
     * you might exclude null or empty values from a status index.
     * Map key is the field name, value is the set of excluded values.
     * </p>
     * 
     * @return Map of field names to excluded values (default: empty)
     */
    default Map<String, Set<String>> indexExclusions() {
        return Collections.emptyMap();
    }

    default SharedChronicleMap<String, V> openDb(final String dataDir, final String fileName) {
        return CHRONICLE_DB.open(name(), entries(), averageKey(), averageValue(), dataPath() + dataDir + fileName,
                bloatFactor());
    }

    default SharedChronicleMap<String, V> openDb(final String dataDir, final String fileName, final long entries) {
        return CHRONICLE_DB.open(name(), entries, averageKey(), averageValue(), dataPath() + dataDir + fileName,
                bloatFactor());
    }

    default SharedChronicleMap<String, V> openDb() {
        return openDb(DATA_DIR, getDataFileState().currentFile());
    }

    default SharedChronicleMap<String, V> openDb(final String fileName) {
        return openDb(DATA_DIR, fileName);
    }

    default SharedChronicleMap<String, V> openDb(final String fileName, final long entries) {
        return openDb(DATA_DIR, fileName, entries);
    }

    private String getKeyMapPath() {
        return dataPath() + DATA_DIR + KEY_FILE;
    }

    private void populateKeyMap(final Set<String> dataFiles, final HTreeMap<String, String> keyMap) {
        dataFiles.parallelStream().forEach(file -> {
            try (final var shared = openDb(file)) {
                shared.map.forEachEntry(entry -> keyMap.put(entry.key().get(), file));
            }
        });
    }

    default void rebuildKeyMap() {
        final Object lock = WRITE_LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            CHRONICLE_UTILS.deleteFileIfExists(getKeyMapPath());
            final var dataFileState = getDataFileState();
            if (dataFileState.fileNames().size() > 1) {
                try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
                    populateKeyMap(dataFileState.fileNames(), sharedKeyMap.map);
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

        final Object lock = WRITE_LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            final var dataFileState = getDataFileState();
            if (dataFileState.fileNames().size() > 1 && !Files.exists(Path.of(getKeyMapPath()))) {
                Logger.info("Populating KeyMap at [{}]", dataPath());
                try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
                    if (sharedKeyMap.map.isEmpty()) {
                        populateKeyMap(dataFileState.fileNames(), sharedKeyMap.map);
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

        // cleanup the old backup files first
        for (final var file : CHRONICLE_UTILS.getFileList(backupPath)) {
            CHRONICLE_UTILS.deleteFileIfExists(backupPath + file);
        }

        for (final var file : CHRONICLE_UTILS.getFileList(dataPath)) {
            Files.copy(Path.of(dataPath + file), Path.of(backupPath + file), REPLACE_EXISTING);
        }
    }

    default void deleteDataFiles() {
        for (final var file : getDataFileState().fileNames()) {
            CHRONICLE_UTILS.deleteFileIfExists(dataPath() + DATA_DIR + file);
        }
    }

    /**
     * Delete and rerun all indexes. Faster when inserting a lot of records.
     * 
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
    default DataFileState getDataFileState() {
        return DATA_FILE_CACHE.computeIfAbsent(dataPath(), k -> {
            try (final var stream = Files.list(Path.of(dataPath() + DATA_DIR))) {
                final var dataFiles = stream.map(Path::getFileName).map(Path::toString)
                        .filter(file -> file.startsWith("data"))
                        .collect(Collectors.toCollection(HashSet::new));

                final int size = dataFiles.size();
                String currentFile = DATA_FILE;
                if (size <= 1) {
                    dataFiles.add(DATA_FILE);
                } else {
                    currentFile = DATA_FILE + "-" + size;
                }

                return new DataFileState(dataFiles, currentFile);
            } catch (final IOException e) {
                // should never happen
                throw new UncheckedIOException("Failed to initialize data file cache for [{" + dataPath() + "}]", e);
            }
        });
    }

    /**
     * Only runs to initialize an index on the field first time
     * 
     * @param field the field of the V value object
     * 
     */
    private void initIndex(final ChronicleMap<String, V> db, final Set<String> fields) {
        CHRONICLE_UTILS.index(db, name(), fields, dataPath(), averageValue().getClass(), indexExclusions());
    }

    /**
     * Only runs to initialize an index on the field first time
     * 
     * @param field the field of the V value object
     * 
     */
    private void initIndex(final Set<String> fields) {
        getDataFileState().fileNames().parallelStream().forEach(file -> {
            try (final var shared = openDb(file)) {
                initIndex(shared.map, fields);
            }
        });
        Logger.info("Indexing {} at [{}] complete.", fields, dataPath());
    }

    /**
     * Delete and rerun all indexes. Faster when inserting a lot of records.
     * 
     */
    default void refreshIndexes() {
        final var indexFiles = indexFileNames();
        if (!indexFiles.isEmpty()) {
            final Object lock = WRITE_LOCKS.computeIfAbsent(dataPath(), k -> new Object());
            synchronized (lock) {
                Logger.info("Re-initializing indexes at [{}].", dataPath());
                for (final String field : indexFiles) {
                    final var indexPath = getIndexPath(field);
                    MAP_DB.closeIndex(indexPath);
                    CHRONICLE_UTILS.deleteFileIfExists(indexPath);
                }
                initIndex(indexFiles);
            }
        }
    }

    default void refreshKeyMap() {
        final Object lock = WRITE_LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            final var dataFileState = getDataFileState();
            if (dataFileState.fileNames().size() > 1) {
                Logger.info("Refreshing KeyMap at [{}]", dataPath());
                final var keyMapPath = getKeyMapPath();
                MAP_DB.closeMap(keyMapPath);
                CHRONICLE_UTILS.deleteFileIfExists(keyMapPath);
                try (final var sharedKeyMap = MAP_DB.openMap(keyMapPath)) {
                    populateKeyMap(dataFileState.fileNames(), sharedKeyMap.map);
                }
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
        if (!getDataFileState().fileNames().isEmpty()) {
            final Object lock = WRITE_LOCKS.computeIfAbsent(dataPath(), k -> new Object());

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
     * In cases of data corruption, we can recover the db using this method
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
     * In cases of UTFDataFormatRuntimeException, we can recover the db using this
     * method
     */
    default void checkUtf8Exceptions(final String dataFileName) {
        final var deleteKeys = new HashSet<String>();
        try (final var shared = openDb(dataFileName)) {
            Logger.info("Total db size [{}]", shared.map.size());
            shared.map.forEachEntry(entry -> {
                final var key = entry.key().get();
                try {
                    shared.map.getUsing(key, using());
                } catch (final UTFDataFormatRuntimeException e) {
                    deleteKeys.add(key);
                }
            });

            Logger.info("Found [{}] entries with malformed data at [{}] and file [{}]", deleteKeys.size(), dataPath(),
                    dataFileName);
        }
    }

    /**
     * Get the db file for this key from cache or default file is new key
     * if cache is empty, use default file
     */
    private String getDbFile(final String key, final Set<String> dbFiles) {
        if (dbFiles.size() <= 1) {
            return DATA_FILE;
        }

        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            final var file = sharedKeyMap.map.get(key);
            if (file == null) {
                return null;
            }
            return file;
        }
    }

    /**
     * Get a map of file and set of keys in that file from cache.
     * Only run for multi file cases
     * 
     */
    private Map<String, Set<String>> getDbFiles(final Iterable<String> keys, final Set<String> dbFiles) {
        // Multi-file: eagerly group keys by file
        final Map<String, Set<String>> fileGroups = new HashMap<>();
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            for (final String k : keys) {
                final String mappedFile = sharedKeyMap.map.get(k);
                if (mappedFile != null) {
                    fileGroups.computeIfAbsent(mappedFile, file -> new HashSet<>()).add(k);
                }
            }
        }

        return fileGroups;
    }

    private void removeFromKeyMap(final String key) {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            sharedKeyMap.map.remove(key);
        }
        Logger.info("Deleted [{}] from KeyMap at [{}].", key, dataPath());
    }

    private void removeAllFromKeyMap(final Set<String> keys) {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            sharedKeyMap.map.keySet().removeAll(keys);
        }
        Logger.info("Deleted [{}] keys from KeyMap at [{}].", keys.size(), dataPath());
    }

    private void addToKeyMap(final String key, final String file) {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            sharedKeyMap.map.put(key, file);
        }
        Logger.info("Inserted [{}] to KeyMap at [{}].", key, dataPath());
    }

    private void addToKeyMap(final Map<String, String> map) {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            sharedKeyMap.map.putAll(map);
        }
        Logger.info("Inserted [{}] keys to KeyMap at [{}].", map.size(), dataPath());
    }

    private int getKeyMapSize() {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            return sharedKeyMap.map.size();
        }
    }

    private boolean getKeyMapExists(final String key) {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            return sharedKeyMap.map.containsKey(key);
        }
    }

    private Set<String> getKeyMapKeys() {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            return new HashSet<>(sharedKeyMap.map.keySet());
        }
    }

    private List<String> getKeyMapKeysList() {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            return new ArrayList<>(sharedKeyMap.map.keySet());
        }
    }

    private Map<String, Boolean> getKeyMapExists(final Collection<String> keys) {
        final var containsMap = new HashMap<String, Boolean>(keys.size());
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            for (final String k : keys) {
                containsMap.put(k, sharedKeyMap.map.containsKey(k));
            }
        }

        return containsMap;
    }

    private List<String> getKeyMapExistsList(final Collection<String> keys) {
        final var containsList = new ArrayList<String>(keys.size());
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            for (final String k : keys) {
                if (sharedKeyMap.map.containsKey(k)) {
                    containsList.add(k);
                }
            }
        }

        return containsList;
    }

    private List<String> getKeyMapNotExistsList(final Collection<String> keys) {
        final var containsList = new ArrayList<String>(keys.size());
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            for (final String k : keys) {
                if (!sharedKeyMap.map.containsKey(k)) {
                    containsList.add(k);
                }
            }
        }

        return containsList;
    }

    default void resizeDb(final String fileName, final long newSize) throws IOException {
        final Object lock = WRITE_LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            final var dataFilePath = dataPath() + DATA_DIR + fileName;
            final var backupDataFilePath = dataPath() + BACKUP_DIR + fileName;
            final var tempFileName = "data.tmp";
            final var tempFilePath = dataPath() + DATA_DIR + tempFileName;
            long currentEntrySize = 0;
            boolean success = false;

            try (final var shared = openDb()) {
                currentEntrySize = shared.map.size();
                if (newSize <= currentEntrySize) {
                    Logger.warn("New size {} is not larger than current size {} at [{}]. Skipping resize.",
                            newSize, currentEntrySize, dataPath());
                    return;
                }

                try (final var newShared = openDb(tempFileName, newSize)) {
                    newShared.map.putAll(shared.map);
                    success = true;
                }
            }

            if (success) {
                Files.move(Path.of(dataFilePath), Path.of(backupDataFilePath), REPLACE_EXISTING);
                Files.move(Path.of(tempFilePath), Path.of(dataFilePath), REPLACE_EXISTING);
                Logger.info("Resized DB at [{}] from {} to {}.", dataPath(), currentEntrySize, newSize);
            }
        }
    }

    default void vacuum() throws IOException {
        // backup all files then read from these files and insert afresh
        final Object lock = WRITE_LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        Logger.info("Vacuuming database at [{}]", dataPath());

        synchronized (lock) {
            backup();
            deleteDataFiles();
            deleteIndexes();
            CHRONICLE_UTILS.deleteFileIfExists(getKeyMapPath());
            DATA_FILE_CACHE.remove(dataPath());

            for (final String file : CHRONICLE_UTILS.getFileList(dataPath() + BACKUP_DIR)) {
                try (final var shared = openDb(BACKUP_DIR, file)) {
                    insert(shared.map);
                }
            }
        }
    }

    /**
     * Fetches all records in the db
     * 
     * @return Map<String, V>
     */
    default Map<String, V> fetch(final int limit) {
        final Map<String, V> result = new HashMap<>(limit);
        for (final String file : getDataFileState().fileNames()) {
            if (result.size() >= limit) {
                break;
            }
            try (final var shared = openDb(file)) {
                shared.map.forEachEntryWhile(entry -> {
                    final var key = entry.key().get();
                    result.put(key, entry.value().getUsing(using()));
                    return result.size() < limit;
                });
            }
        }
        Logger.info("Fetched [{}] entries at [{}].", result.size(), dataPath());
        return result;
    }

    default CsvObject fetchCsv(final int limit) {
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());
        final AtomicReference<String[]> headers = new AtomicReference<>(null);

        for (final String file : getDataFileState().fileNames()) {
            if (rowQueue.size() >= limit) {
                break;
            }
            try (final var shared = openDb(file)) {
                shared.map.forEachEntryWhile(entry -> {
                    final var key = entry.key().get();
                    final var value = entry.value().getUsing(using());
                    headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                    rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
                    return rowQueue.size() < limit;
                });
            }
        }
        Logger.info("Fetched [{}] CSV entries at [{}].", rowQueue.size(), dataPath());

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    default Map<String, Map<String, Object>> fetchSubset(final String[] fields, final int limit) {
        final var result = new ConcurrentHashMap<String, Map<String, Object>>(limit);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());
        for (final String file : getDataFileState().fileNames()) {
            if (result.size() >= limit) {
                break;
            }
            try (final var shared = openDb(file)) {
                shared.map.forEachEntryWhile(entry -> {
                    result.put(entry.key().get(),
                            CHRONICLE_UTILS.getSubsetFromObject(classData, fields, entry.value().getUsing(using())));
                    return result.size() < limit;
                });
            }
        }
        Logger.info("Fetched [{}] entries at [{}].", result.size(), dataPath());
        return result;
    }

    default CsvObject fetchSubsetCsv(final String[] fields, final int limit) {
        final String[] headers = CHRONICLE_UTILS.getCsvHeaders(fields);
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        for (final String file : getDataFileState().fileNames()) {
            if (rowQueue.size() >= limit) {
                break;
            }
            try (final var shared = openDb(file)) {
                shared.map.forEachEntryWhile(entry -> {
                    rowQueue.add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, entry.key().get(), fields,
                            entry.value().getUsing(using())));
                    return rowQueue.size() < limit;
                });
            }
        }
        Logger.info("Fetched subset CSV [{}] entries at [{}].", rowQueue.size(), dataPath());
        return new CsvObject(headers, new ArrayList<>(rowQueue));
    }

    default Map<String, V> fetch() {
        return fetch(HARD_LIMIT);
    }

    default CsvObject fetchCsv() {
        return fetchCsv(HARD_LIMIT);
    }

    default Map<String, Map<String, Object>> fetchSubset(final String[] fields) {
        return fetchSubset(fields, HARD_LIMIT);
    }

    default CsvObject fetchSubsetCsv(final String[] fields) {
        return fetchSubsetCsv(fields, HARD_LIMIT);
    }

    /**
     * Fetches all keys in the db, never run directly for huge files
     * 
     * @return Set<String>
     */
    default Set<String> fetchKeys() {
        if (getDataFileState().fileNames().size() > 1) {
            return getKeyMapKeys();
        }
        try (final var shared = openDb()) {
            Logger.info("Fetching [{}] keys at [{}].", shared.map.size(), dataPath());
            return new HashSet<>(shared.map.keySet());
        }
    }

    default List<String> fetchKeysList() {
        if (getDataFileState().fileNames().size() > 1) {
            return getKeyMapKeysList();
        }
        try (final var shared = openDb()) {
            Logger.info("Fetching [{}] keys at [{}].", shared.map.size(), dataPath());
            return new ArrayList<>(shared.map.keySet());
        }
    }

    /**
     * Get a value using key
     * 
     * @param key the key to search in the db
     * @return V value
     */
    default V get(final String key) {
        if (key == null) {
            return null;
        }

        Logger.info("Querying key [{}] at [{}].", key, dataPath());
        final var file = getDbFile(key, getDataFileState().fileNames());
        if (file == null) {
            return null;
        }

        try (final var shared = openDb(file)) {
            return shared.map.getUsing(key, using());
        }
    }

    private void processKeysByFile(final Iterable<String> keys, final BiConsumer<String, V> valueConsumer) {
        final var keyFiles = getDbFiles(keys, getDataFileState().fileNames());
        keyFiles.entrySet().parallelStream().forEach(entry -> {
            final var file = entry.getKey();
            try (final var shared = openDb(file)) {
                for (final var key : entry.getValue()) {
                    final var value = shared.map.getUsing(key, using());
                    if (value != null) {
                        valueConsumer.accept(key, value);
                    }
                }
            }
        });
    }

    private void processKeysByFile(final Iterable<String> keys, final int limit,
            final BiConsumer<String, V> valueConsumer) {
        final var keyFiles = getDbFiles(keys, getDataFileState().fileNames());
        int count = 0;
        outer: for (final var entry : keyFiles.entrySet()) {
            final var file = entry.getKey();
            try (final var shared = openDb(file)) {
                for (final var key : entry.getValue()) {
                    final var value = shared.map.getUsing(key, using());
                    if (value != null) {
                        valueConsumer.accept(key, value);
                        if (++count >= limit) {
                            break outer;
                        }
                    }
                }
            }
        }
    }

    private void processKeysByFile(final Iterable<String> keys, final int limit,
            final Set<String> excludedKeys, final BiConsumer<String, V> valueConsumer) {
        final var keyFiles = getDbFiles(keys, getDataFileState().fileNames());
        int count = 0;
        outer: for (final var entry : keyFiles.entrySet()) {
            final var file = entry.getKey();
            try (final var shared = openDb(file)) {
                for (final var key : entry.getValue()) {
                    if (!excludedKeys.contains(key)) {
                        final var value = shared.map.getUsing(key, using());
                        if (value != null) {
                            valueConsumer.accept(key, value);
                            if (++count >= limit) {
                                break outer;
                            }
                        }
                    }
                }
            }
        }
    }

    private void processKeys(final Iterable<String> keys, final BiConsumer<String, V> valueConsumer) {
        try (final var shared = openDb()) {
            for (final var key : keys) {
                final var value = shared.map.getUsing(key, using());
                if (value != null) {
                    valueConsumer.accept(key, value);
                }
            }
        }
    }

    private void processKeys(final Iterable<String> keys, final int limit, final BiConsumer<String, V> valueConsumer) {
        var count = 0;
        try (final var shared = openDb()) {
            for (final var key : keys) {
                final var value = shared.map.getUsing(key, using());
                if (value != null) {
                    valueConsumer.accept(key, value);
                    if (++count >= limit)
                        break;
                }
            }
        }
    }

    private void processKeys(final Iterable<String> keys, final int limit, final Set<String> excludedKeys,
            final BiConsumer<String, V> valueConsumer) {
        var count = 0;
        try (final var shared = openDb()) {
            for (final var key : keys) {
                if (!excludedKeys.contains(key)) {
                    final var value = shared.map.getUsing(key, using());
                    if (value != null) {
                        valueConsumer.accept(key, value);
                        if (++count >= limit)
                            break;
                    }
                }
            }
        }
    }

    /**
     * Get all the values for a list of keys
     * 
     * @param keys list of keys
     * @return Map<String, V> values
     */
    default Map<String, V> get(final Iterable<String> keys) {
        if (keys == null || !keys.iterator().hasNext()) {
            return Collections.emptyMap();
        }

        Logger.info("Querying multiple keys at [{}].", dataPath());
        final var map = new ConcurrentHashMap<String, V>(10_000);

        if (getDataFileState().fileNames().size() <= 1) {
            processKeys(keys, (key, value) -> map.put(key, value));
            return map;
        }

        processKeysByFile(keys, (key, value) -> map.put(key, value));

        return map;
    }

    default CsvObject getCsv(final Iterable<String> keys) {
        if (keys == null || !keys.iterator().hasNext()) {
            return CsvObject.empty();
        }

        Logger.info("Querying multiple keys for CSV at [{}].", dataPath());
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final AtomicReference<String[]> headers = new AtomicReference<>(null);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        if (getDataFileState().fileNames().size() <= 1) {
            processKeys(keys, (key, value) -> {
                headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
            });

            return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
        }

        processKeysByFile(keys, (key, value) -> {
            headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(classData, value));
            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
        });

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    default Map<String, Map<String, Object>> getSubset(final Iterable<String> keys, final String[] fields) {
        if (keys == null || !keys.iterator().hasNext()) {
            return Collections.emptyMap();
        }

        Logger.info("Querying subset multiple keys at [{}].", dataPath());
        final var map = new ConcurrentHashMap<String, Map<String, Object>>(10_000);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        if (getDataFileState().fileNames().size() <= 1) {
            processKeys(keys,
                    (key, value) -> map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value)));

            return map;
        }

        processKeysByFile(keys,
                (key, value) -> map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value)));

        return map;
    }

    default CsvObject getSubsetCsv(final Iterable<String> keys, final String[] fields) {
        if (keys == null || !keys.iterator().hasNext()) {
            return CsvObject.empty();
        }

        Logger.info("Querying subset CSV multiple keys at [{}].", dataPath());
        final String[] headers = CHRONICLE_UTILS.getCsvHeaders(fields);
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        if (getDataFileState().fileNames().size() <= 1) {
            processKeys(keys, (key, value) -> rowQueue
                    .add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value)));

            return new CsvObject(headers, new ArrayList<>(rowQueue));
        }

        processKeysByFile(keys,
                (key, value) -> rowQueue.add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value)));

        return new CsvObject(headers, new ArrayList<>(rowQueue));
    }

    default Map<String, V> get(final Iterable<String> keys, final int limit) {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return Collections.emptyMap();
        }

        Logger.info("Querying multiple keys at [{}] with limit [{}].", dataPath(), limit);
        final var map = new ConcurrentHashMap<String, V>(limit);

        if (getDataFileState().fileNames().size() <= 1) {
            processKeys(keys, limit, (key, value) -> map.put(key, value));
            return map;
        }

        processKeysByFile(keys, limit, (key, value) -> map.put(key, value));

        return map;
    }

    default CsvObject getCsv(final Iterable<String> keys, final int limit) {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.info("Querying multiple keys at [{}] with limit [{}].", dataPath(), limit);
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());
        final AtomicReference<String[]> headers = new AtomicReference<>(null);

        if (getDataFileState().fileNames().size() <= 1) {
            processKeys(keys, limit, (key, value) -> {
                headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
            });

            return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
        }

        processKeysByFile(keys, limit, (key, value) -> {
            headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(classData, value));
            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
        });

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    default Map<String, Map<String, Object>> getSubset(final Iterable<String> keys, final String[] fields,
            final int limit) {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return Collections.emptyMap();
        }

        Logger.info("Querying subset multiple keys at [{}] with limit [{}].", dataPath(), limit);
        final var map = new ConcurrentHashMap<String, Map<String, Object>>(limit);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        if (getDataFileState().fileNames().size() <= 1) {
            processKeys(keys, limit,
                    (key, value) -> map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value)));

            return map;
        }

        processKeysByFile(keys, limit,
                (key, value) -> map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value)));

        return map;
    }

    default CsvObject getSubsetCsv(final Iterable<String> keys, final String[] fields, final int limit) {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.info("Querying subset CSV multiple keys at [{}] with limit [{}].", dataPath(), limit);
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var headers = CHRONICLE_UTILS.getCsvHeaders(fields);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        if (getDataFileState().fileNames().size() <= 1) {
            processKeys(keys, limit, (key, value) -> rowQueue
                    .add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value)));

            return new CsvObject(headers, new ArrayList<>(rowQueue));
        }

        processKeysByFile(keys, limit,
                (key, value) -> rowQueue.add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value)));

        return new CsvObject(headers, new ArrayList<>(rowQueue));
    }

    default Map<String, V> get(final Iterable<String> keys, final Set<String> excludedKeys, final int limit) {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return Collections.emptyMap();
        }

        Logger.info("Querying multiple keys at [{}] with limit [{}] and excluded keys.", dataPath(), limit);
        final var map = new ConcurrentHashMap<String, V>(limit);

        if (getDataFileState().fileNames().size() <= 1) {
            processKeys(keys, limit, excludedKeys, (key, value) -> map.put(key, value));

            return map;
        }

        processKeysByFile(keys, limit, excludedKeys, (key, value) -> map.put(key, value));

        return map;
    }

    default CsvObject getCsv(final Iterable<String> keys, final Set<String> excludedKeys, final int limit) {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.info("Querying multiple keys at [{}] with limit [{}] and excluded keys.", dataPath(), limit);
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());
        final AtomicReference<String[]> headers = new AtomicReference<>(null);

        if (getDataFileState().fileNames().size() <= 1) {
            processKeys(keys, limit, excludedKeys, (key, value) -> {
                headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
            });

            return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
        }

        processKeysByFile(keys, limit, excludedKeys, (key, value) -> {
            headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(classData, value));
            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
        });

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    default Map<String, Map<String, Object>> getSubset(final Iterable<String> keys, final Set<String> excludedKeys,
            final String[] fields, final int limit) {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return Collections.emptyMap();
        }

        Logger.info("Querying subset multiple keys at [{}] with limit [{}] and excluded keys.", dataPath(), limit);
        final var map = new ConcurrentHashMap<String, Map<String, Object>>(limit);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        if (getDataFileState().fileNames().size() <= 1) {
            processKeys(keys, limit, excludedKeys,
                    (key, value) -> map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value)));

            return map;
        }

        processKeysByFile(keys, limit, excludedKeys,
                (key, value) -> map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value)));

        return map;
    }

    default CsvObject getSubsetCsv(final Iterable<String> keys, final Set<String> excludedKeys, final String[] fields,
            final int limit) {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.info("Querying subset CSV multiple keys at [{}] with limit [{}] and excluded keys.", dataPath(), limit);
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var headers = CHRONICLE_UTILS.getCsvHeaders(fields);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        if (getDataFileState().fileNames().size() <= 1) {
            processKeys(keys, limit, excludedKeys, (key, value) -> rowQueue
                    .add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value)));

            return new CsvObject(headers, new ArrayList<>(rowQueue));
        }

        processKeysByFile(keys, limit, excludedKeys,
                (key, value) -> rowQueue.add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value)));

        return new CsvObject(headers, new ArrayList<>(rowQueue));
    }

    /**
     * Remove a value using key
     * 
     * @param key the key to remove
     * @return true if updated else false
     */
    default boolean delete(final String key) {
        if (key == null) {
            return false;
        }

        final Object lock = WRITE_LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            final var file = getDbFile(key, getDataFileState().fileNames());
            if (file == null) {
                return false;
            }

            final V value;
            try (final var shared = openDb(file)) {
                value = shared.map.remove(key);
            }

            if (value == null) {
                return false;
            }

            final var tasks = new ArrayList<Runnable>();
            if (getDataFileState().fileNames().size() > 1) {
                tasks.add(() -> removeFromKeyMap(key));
            }

            tasks.add(() -> CHRONICLE_UTILS.removeFromIndex(name(), dataPath(), indexFileNames(), Map.of(key, value),
                    averageValue().getClass()));

            CHRONICLE_UTILS.processInParallel(tasks);

            Logger.info("Deleted using key [{}] at [{}].", key, dataPath());
            return true;
        }
    }

    private void removeFromIndex(final Map<String, V> deletedMap) {
        Logger.info("Deleted [{}] record(s) at [{}].", deletedMap.size(), dataPath());
        CHRONICLE_UTILS.removeFromIndex(name(), dataPath(), indexFileNames(), deletedMap, averageValue().getClass());
    }

    /**
     * Remove a value using a list of keys
     * 
     * @param keys the keys to remove
     * @return true if updated else false
     */
    default boolean delete(final Iterable<String> keys) {
        if (keys == null || !keys.iterator().hasNext()) {
            return false;
        }

        final var deletedMap = new ConcurrentHashMap<String, V>(1000);
        final Object lock = WRITE_LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            if (getDataFileState().fileNames().size() <= 1) {
                try (final var shared = openDb()) {
                    for (final String key : keys) {
                        final var deleted = shared.map.remove(key);
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

            final var keyFiles = getDbFiles(keys, getDataFileState().fileNames());
            if (keyFiles.isEmpty()) {
                return false;
            }

            keyFiles.entrySet().parallelStream().forEach(entry -> {
                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    for (final String key : entry.getValue()) {
                        deletedMap.put(key, shared.map.remove(key));
                    }
                }
            });

            if (deletedMap.isEmpty()) {
                return false;
            }

            final var tasks = new ArrayList<Runnable>();
            tasks.add(() -> removeAllFromKeyMap(deletedMap.keySet()));
            tasks.add(() -> removeFromIndex(deletedMap));
            CHRONICLE_UTILS.processInParallel(tasks);

            return true;
        }
    }

    private SharedChronicleMap<String, V> checkAndRotate(final SharedChronicleMap<String, V> shared) {
        if (shared.map.size() >= entries()) {
            final var dataFileState = getDataFileState();
            final String newFile = "data-" + (dataFileState.fileNames().size() + 1);
            // Update key map using the provided ChronicleMap
            try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
                shared.map.forEachEntry(entry -> sharedKeyMap.map.put(entry.key().get(), dataFileState.currentFile()));
            }
            shared.close(); // close after rotation
            dataFileState.fileNames().add(newFile);
            Logger.info("Rotated file [{}] at [{}].", dataFileState.currentFile(), dataPath());
            final var updateFileState = dataFileState.withCurrentFile(newFile);
            DATA_FILE_CACHE.put(dataPath(), updateFileState);
            return openDb(newFile); // open new db
        }
        return shared;
    }

    private SharedChronicleMap<String, V> checkAndRotate(final SharedChronicleMap<String, V> shared,
            final Map<String, String> keyMapUpdate) {
        if (shared.map.size() >= entries()) {
            final var dataFileState = getDataFileState();
            final var currentSize = dataFileState.fileNames().size();
            final String newFile = "data-" + (currentSize + 1);

            if (currentSize == 1) {
                // For first rotation since no keys will be in keyMapUpdate
                final int firstFileSize = shared.map.size();
                try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
                    shared.map.forEachEntry(
                            entry -> sharedKeyMap.map.put(entry.key().get(), dataFileState.currentFile()));
                }
                Logger.info("Inserted [{}] keys to KeyMap at [{}].", firstFileSize, dataPath());
            } else {
                addToKeyMap(keyMapUpdate);
            }
            shared.close();

            dataFileState.fileNames().add(newFile);
            Logger.info("Rotated file [{}] at [{}].", dataFileState.currentFile(), dataPath());
            final var updateFileState = dataFileState.withCurrentFile(newFile);
            DATA_FILE_CACHE.put(dataPath(), updateFileState);
            keyMapUpdate.clear();
            return openDb(newFile); // open new db
        }
        return shared;
    }

    /**
     * Add/Update a value
     * 
     * @param key   the key
     * @param value the value
     * @return true if updated else false
     */
    default PutStatus put(final String key, final V value, final Set<String> indexFileNames) {
        if (key == null) {
            return PutStatus.FAILED;
        }

        V prevValue = null;
        var file = DATA_FILE;
        final Object lock = WRITE_LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            final var dataFileState = getDataFileState();
            file = getDbFile(key, dataFileState.fileNames());
            if (file == null) {
                file = dataFileState.currentFile();
            }

            var shared = openDb(file); // no try with resource here for rotation
            // only rotate if current file is full and insert mode
            try {
                if (dataFileState.currentFile().equals(file) && !shared.map.containsKey(key)) {
                    shared = checkAndRotate(shared);
                }
                prevValue = shared.map.put(key, value);
            } finally {
                shared.close();
            }

            var status = PutStatus.INSERTED;
            if (prevValue != null) {
                CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames, Map.of(key, value),
                        Map.of(key, prevValue), averageValue().getClass(), indexExclusions());
                status = PutStatus.UPDATED;
                Logger.info("Updated using key [{}] at [{}].", key, dataPath());
            } else {
                if (getDataFileState().fileNames().size() > 1) {
                    addToKeyMap(key, getDataFileState().currentFile());
                }
                CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames, Map.of(key, value),
                        Collections.emptyMap(), averageValue().getClass(), indexExclusions());
                Logger.info("Inserted using key [{}] at [{}].", key, dataPath());
            }

            return status;
        }
    }

    /**
     * Refer to method above
     */
    default PutStatus put(final String key, final V value) {
        return put(key, value, indexFileNames());
    }

    /**
     * Update a value without bothering about db creation, only use for updates
     * 
     * @param key   the key
     * @param value the value
     * @return true if updated else false
     */
    default PutStatus update(final String key, final V value, final Set<String> indexFileNames) {
        if (key == null || !exists(key)) {
            Logger.error("Key [{}] does not exist during update at [{}].", key, dataPath());
            return PutStatus.FAILED;
        }

        V prevValue = null;
        var status = PutStatus.FAILED;

        final Object lock = WRITE_LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            final var file = getDbFile(key, getDataFileState().fileNames());
            try (final var shared = openDb(file)) {
                if (shared.map.containsKey(key)) {
                    status = PutStatus.UPDATED;
                    prevValue = shared.map.put(key, value);
                }
            }

            if (status == PutStatus.UPDATED) {
                CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames, Map.of(key, value),
                        Map.of(key, prevValue), averageValue().getClass(), indexExclusions());
                Logger.info("Updated using key [{}] at [{}].", key, dataPath());
            }

            return status;
        }
    }

    /**
     * Refer to method above
     */
    default PutStatus update(final String key, final V value) {
        return update(key, value, indexFileNames());
    }

    /**
     * Add a value, will not return failed if it exists
     * 
     * @param key   the key
     * @param value the value
     * @return true if updated else false
     */
    default PutStatus insert(final String key, final V value, final Set<String> indexFileNames) {
        if (key == null || exists(key)) {
            Logger.error("Key [{}] already exists during insert at [{}].", key, dataPath());
            return PutStatus.FAILED;
        }

        final Object lock = WRITE_LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            var shared = openDb();
            try {
                shared = checkAndRotate(shared);
                shared.map.put(key, value);
            } finally {
                shared.close();
            }

            if (getDataFileState().fileNames().size() > 1) {
                addToKeyMap(key, getDataFileState().currentFile());
            }

            CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames, Map.of(key, value), Collections.emptyMap(),
                    averageValue().getClass(), indexExclusions());
            Logger.info("Inserted using key [{}] at [{}].", key, dataPath());

            return PutStatus.INSERTED;
        }
    }

    /**
     * Refer to method above
     */
    default PutStatus insert(final String key, final V value) {
        return insert(key, value, indexFileNames());
    }

    /**
     * Add/Update multiple values into the db, then update all indexes related
     * 
     * @param map the map to add
     */
    default PutStatus put(final Map<String, V> map) {
        if (map == null || map.isEmpty()) {
            return PutStatus.FAILED;
        }

        final int putSize = map.size();
        final var prevValues = new ConcurrentHashMap<String, V>(putSize);
        final Set<String> keysToInsert = new HashSet<>(map.keySet());
        final var keyMapUpdate = new HashMap<String, String>(putSize);
        var status = PutStatus.INSERTED;

        final Object lock = WRITE_LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            // update old records first then only move to new record inserts.
            if (getDataFileState().fileNames().size() <= 1) {
                // Single file
                try (final var shared = openDb()) {
                    for (final String key : map.keySet()) {
                        if (shared.map.containsKey(key)) {
                            prevValues.put(key, shared.map.put(key, map.get(key)));
                        }
                    }
                }
            } else {
                final var keyFiles = getDbFiles(keysToInsert, getDataFileState().fileNames());
                keyFiles.entrySet().parallelStream().forEach(entry -> {
                    final var file = entry.getKey();
                    try (final var shared = openDb(file)) {
                        for (final String key : entry.getValue()) {
                            final V prevValue = shared.map.put(key, map.get(key));
                            if (prevValue != null) {
                                prevValues.put(key, prevValue);
                            }
                        }
                    }
                });
            }

            if (!prevValues.isEmpty()) {
                status = PutStatus.UPDATED;
                Logger.info("Updated [{}] record(s) at [{}].", prevValues.size(), dataPath());
                keysToInsert.removeAll(prevValues.keySet());
            }

            if (!keysToInsert.isEmpty()) {
                // Insert new records (only keys in keysToInsert) in batches
                var shared = openDb();
                try {
                    final var batches = new ArrayList<>(keysToInsert);
                    int startIndex = 0;

                    while (startIndex < batches.size()) {
                        final long remainingEntries = entries() - shared.map.size();
                        final int endIndex = (int) Math.min(startIndex + remainingEntries, batches.size());

                        // Process this batch (sublist)
                        final var batch = batches.subList(startIndex, endIndex);
                        for (final String key : batch) {
                            final V value = map.get(key);
                            shared.map.put(key, value);

                            if (getDataFileState().fileNames().size() > 1) {
                                keyMapUpdate.put(key, getDataFileState().currentFile());
                            }
                        }

                        startIndex = endIndex;

                        // More to insert? Rotate and continue
                        if (startIndex < batches.size()) {
                            shared = checkAndRotate(shared, keyMapUpdate);
                        }
                    }
                    Logger.info("Inserted [{}] record(s) at [{}].", keysToInsert.size(), dataPath());
                } finally {
                    shared.close();
                }
            }

            final var tasks = new ArrayList<Runnable>();
            if (!keyMapUpdate.isEmpty()) {
                tasks.add(() -> addToKeyMap(keyMapUpdate));
            }
            tasks.add(() -> CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames(), map, prevValues,
                    averageValue().getClass(), indexExclusions()));
            CHRONICLE_UTILS.processInParallel(tasks);

            return status;
        }
    }

    /**
     * Update multiple values into the db, then update all indexes related
     * This is useful as it does not increase db size. Never run with non existent
     * keys, it wont insert
     * 
     * @param map the map to add
     */
    default PutStatus update(final Map<String, V> map) {
        if (map == null || map.isEmpty()) {
            return PutStatus.FAILED;
        }

        var status = PutStatus.UPDATED;
        final var mapSize = map.size();
        final var prevValues = new ConcurrentHashMap<String, V>(mapSize);

        final Object lock = WRITE_LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            if (getDataFileState().fileNames().size() <= 1) {
                // Single file
                try (final var shared = openDb()) {
                    for (final String key : map.keySet()) {
                        if (shared.map.containsKey(key)) {
                            prevValues.put(key, shared.map.put(key, map.get(key)));
                        }
                    }
                }
            } else {
                // Multi-file
                final var keyFiles = getDbFiles(map.keySet(), getDataFileState().fileNames());
                keyFiles.entrySet().parallelStream().forEach(entry -> {
                    final var file = entry.getKey();
                    try (final var shared = openDb(file)) {
                        for (final String key : entry.getValue()) {
                            prevValues.put(key, shared.map.put(key, map.get(key)));
                        }
                    }
                });
            }

            final var prevValueSize = prevValues.size();
            if (prevValueSize == 0) {
                Logger.error("All [{}] values given do not exist during update at [{}", mapSize, dataPath());
                return PutStatus.FAILED;
            }

            if (prevValueSize != mapSize) {
                final var mapKeySet = map.keySet();
                mapKeySet.removeAll(prevValues.keySet());
                final var extraSize = mapKeySet.size();

                Logger.error("{} extra keys found during update at [{}]. New keys: [{}].", extraSize, dataPath(),
                        mapKeySet);
                status = PutStatus.PARTIAL;
            }

            CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames(), map, prevValues,
                    averageValue().getClass(), indexExclusions());
            Logger.info("Updated {} records at [{}].", prevValueSize, dataPath());

            return status;
        }
    }

    /**
     * Add multiple values into the db, this will skip existing values
     * 
     * @param map the map to add
     */
    default PutStatus insert(final Map<String, V> map) {
        if (map == null || map.isEmpty()) {
            return PutStatus.FAILED;
        }

        // only work with new keys
        final var existingKeys = existsList(map.keySet());
        map.keySet().removeAll(existingKeys);

        if (map.isEmpty()) {
            Logger.error("No new records found during insert at [{}].", dataPath());
            return PutStatus.FAILED;
        }

        final var keyMapUpdate = new HashMap<String, String>();
        final Object lock = WRITE_LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            // insert in batches by checking remaining entries
            var shared = openDb();
            try {
                final var batches = new ArrayList<>(map.entrySet());
                int startIndex = 0;

                while (startIndex < batches.size()) {
                    final long remainingEntries = entries() - shared.map.size();
                    final int endIndex = (int) Math.min(startIndex + remainingEntries, batches.size());

                    // Process this batch (sublist)
                    final var batch = batches.subList(startIndex, endIndex);
                    for (final var entry : batch) {
                        shared.map.put(entry.getKey(), entry.getValue());

                        if (getDataFileState().fileNames().size() > 1) {
                            keyMapUpdate.put(entry.getKey(), getDataFileState().currentFile());
                        }
                    }

                    startIndex = endIndex;

                    // More to insert? Rotate and continue
                    if (startIndex < batches.size()) {
                        shared = checkAndRotate(shared, keyMapUpdate);
                    }
                }
            } finally {
                shared.close();
            }

            final var tasks = new ArrayList<Runnable>();
            if (!keyMapUpdate.isEmpty()) {
                tasks.add(() -> addToKeyMap(keyMapUpdate));
            }
            tasks.add(
                    () -> CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames(), map, Collections.emptyMap(),
                            averageValue().getClass(), indexExclusions()));
            CHRONICLE_UTILS.processInParallel(tasks);

            Logger.info("Inserted {} records at [{}].", map.size(), dataPath());

            return PutStatus.INSERTED;
        }
    }

    default Map<String, V> search(final Iterable<String> keys, final List<Search> filters, final int limit)
            throws Throwable {
        if (keys == null || !keys.iterator().hasNext() || filters.isEmpty()) {
            return Collections.emptyMap();
        }

        Logger.info("Querying filtered keys at [{}] with [{}] remaining filters.", dataPath(), filters.size());
        final var map = new ConcurrentHashMap<String, V>(limit);
        final var averageValueClass = averageValue().getClass();

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, limit,
                        key -> {
                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : filters) {
                                if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                    return false;
                                }
                            }

                            map.put(key, value);
                            return true;
                        });
            }
            return map;
        }

        final AtomicInteger count = new AtomicInteger();
        final var keyFiles = getDbFiles(keys, getDataFileState().fileNames());
        for (final var entry : keyFiles.entrySet()) {
            if (count.get() >= limit)
                break;

            final var file = entry.getKey();
            final var keysForFile = entry.getValue(); // Iterable<String>

            try (final var shared = openDb(file)) {
                CHRONICLE_UTILS.parallelIterable(keysForFile, limit - count.get(), count, key -> {
                    final V value = shared.map.getUsing(key, using());
                    if (value == null)
                        return false;

                    for (final var search : filters) {
                        if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                            return false;
                        }
                    }

                    map.put(key, value);
                    return true;
                });
            }
        }

        return map;
    }

    default CsvObject searchCsv(final Iterable<String> keys, final List<Search> filters, final int limit)
            throws Throwable {
        if (keys == null || !keys.iterator().hasNext() || filters.isEmpty()) {
            return CsvObject.empty();
        }

        Logger.info("Querying CSV filtered keys at [{}] with [{}] remaining filters.", dataPath(), filters.size());
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var averageValueClass = averageValue().getClass();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);
        final AtomicReference<String[]> headers = new AtomicReference<>(null);

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, limit,
                        key -> {
                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : filters) {
                                if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                    return false;
                                }
                            }

                            headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
                            return true;
                        });
            }
            return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
        }

        final AtomicInteger count = new AtomicInteger();
        final var keyFiles = getDbFiles(keys, getDataFileState().fileNames());
        for (final var entry : keyFiles.entrySet()) {
            if (count.get() >= limit)
                break;

            final var file = entry.getKey();
            final var keysForFile = entry.getValue(); // Iterable<String>

            try (final var shared = openDb(file)) {
                CHRONICLE_UTILS.parallelIterable(keysForFile, limit - count.get(), count, key -> {
                    final V value = shared.map.getUsing(key, using());
                    if (value == null)
                        return false;

                    for (final var search : filters) {
                        if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                            return false;
                        }
                    }

                    headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                    rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
                    return true;
                });
            }
        }

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    default Map<String, Map<String, Object>> searchSubset(final Iterable<String> keys,
            final List<Search> filters, final String[] fields, final int limit) throws Throwable {
        if (keys == null || !keys.iterator().hasNext()) {
            return Collections.emptyMap();
        }

        Logger.info("Querying subset filtered keys at [{}] with [{}] remaining filters.", dataPath(), filters.size());
        final var map = new ConcurrentHashMap<String, Map<String, Object>>(limit);
        final var averageValueClass = averageValue().getClass();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, limit,
                        key -> {
                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : filters) {
                                if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                    return false;
                                }
                            }

                            map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value));
                            return true;
                        });
            }
            return map;
        }

        final AtomicInteger count = new AtomicInteger();
        final var keyFiles = getDbFiles(keys, getDataFileState().fileNames());
        for (final var entry : keyFiles.entrySet()) {
            if (count.get() >= limit)
                break;

            final var file = entry.getKey();
            final var keysForFile = entry.getValue(); // Iterable<String>

            try (final var shared = openDb(file)) {
                CHRONICLE_UTILS.parallelIterable(keysForFile, limit - count.get(), count, key -> {
                    final V value = shared.map.getUsing(key, using());
                    if (value == null)
                        return false;

                    for (final var search : filters) {
                        if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                            return false;
                        }
                    }

                    map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value));
                    return true;
                });
            }
        }

        return map;
    }

    default CsvObject searchSubsetCsv(final Iterable<String> keys, final List<Search> filters, final String[] fields,
            final int limit) throws Throwable {
        if (keys == null || !keys.iterator().hasNext()) {
            return CsvObject.empty();
        }

        Logger.info("Querying subset CSV filtered keys at [{}] with [{}] remaining filters.", dataPath(),
                filters.size());
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var headers = CHRONICLE_UTILS.getCsvHeaders(fields);
        final var averageValueClass = averageValue().getClass();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, limit,
                        key -> {
                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : filters) {
                                if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                    return false;
                                }
                            }

                            rowQueue.add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value));
                            return true;
                        });
            }
            return new CsvObject(headers, new ArrayList<>(rowQueue));
        }

        final AtomicInteger count = new AtomicInteger();
        final var keyFiles = getDbFiles(keys, getDataFileState().fileNames());
        for (final var entry : keyFiles.entrySet()) {
            if (count.get() >= limit)
                break;

            final var file = entry.getKey();
            final var keysForFile = entry.getValue(); // Iterable<String>

            try (final var shared = openDb(file)) {
                CHRONICLE_UTILS.parallelIterable(keysForFile, limit - count.get(), count, key -> {
                    final V value = shared.map.getUsing(key, using());
                    if (value == null)
                        return false;

                    for (final var search : filters) {
                        if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                            return false;
                        }
                    }

                    rowQueue.add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value));
                    return true;
                });
            }
        }

        return new CsvObject(headers, new ArrayList<>(rowQueue));
    }

    default void searchKeys(final Iterable<String> keys, final List<Search> filters, final Collection<String> results)
            throws Throwable {
        if (keys == null || !keys.iterator().hasNext()) {
            return;
        }

        Logger.info("Querying filtered keys at [{}] with [{}] remaining filters.", dataPath(), filters.size());
        final var averageValueClass = averageValue().getClass();

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, Integer.MAX_VALUE,
                        key -> {
                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : filters) {
                                if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                    return false;
                                }
                            }

                            results.add(key);
                            return true;
                        });
            }
            return;
        }

        final var keyFiles = getDbFiles(keys, getDataFileState().fileNames());
        for (final var entry : keyFiles.entrySet()) {
            final var file = entry.getKey();
            final var keysForFile = entry.getValue(); // Iterable<String>

            try (final var shared = openDb(file)) {
                CHRONICLE_UTILS.parallelIterable(keysForFile, Integer.MAX_VALUE, key -> {
                    final V value = shared.map.getUsing(key, using());
                    if (value == null)
                        return false;

                    for (final var search : filters) {
                        if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                            return false;
                        }
                    }

                    results.add(key);
                    return true;
                });
            }
        }
    }

    default Map<String, V> search(final Iterable<String> keys, final List<Search> filters,
            final Set<String> excludedKeys, final int limit) throws Throwable {
        if (keys == null || !keys.iterator().hasNext()) {
            return Collections.emptyMap();
        }

        Logger.info("Querying filtered keys at [{}] with [{}] remaining filters and [{}] excluded keys.", dataPath(),
                filters.size(), excludedKeys.size());
        final var map = new ConcurrentHashMap<String, V>(limit);
        final var averageValueClass = averageValue().getClass();

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, limit,
                        key -> {
                            if (excludedKeys.contains(key))
                                return false;

                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : filters) {
                                if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                    return false;
                                }
                            }

                            map.put(key, value);
                            return true;
                        });
            }
            return map;
        }

        final AtomicInteger count = new AtomicInteger();
        final var keyFiles = getDbFiles(keys, getDataFileState().fileNames());
        for (final var entry : keyFiles.entrySet()) {
            if (count.get() >= limit)
                break;

            final var file = entry.getKey();
            final var keysForFile = entry.getValue(); // Iterable<String>

            try (final var shared = openDb(file)) {
                CHRONICLE_UTILS.parallelIterable(keysForFile, limit - count.get(), count,
                        key -> {
                            if (excludedKeys.contains(key))
                                return false;

                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : filters) {
                                if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                    return false;
                                }
                            }

                            map.put(key, value);
                            return true;
                        });
            }
        }

        return map;
    }

    default CsvObject searchCsv(final Iterable<String> keys, final List<Search> filters,
            final Set<String> excludedKeys, final int limit) throws Throwable {
        if (keys == null || !keys.iterator().hasNext()) {
            return CsvObject.empty();
        }

        Logger.info("Querying filtered keys at [{}] with [{}] remaining filters and [{}] excluded keys.", dataPath(),
                filters.size(), excludedKeys.size());
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var averageValueClass = averageValue().getClass();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);
        final AtomicReference<String[]> headers = new AtomicReference<>(null);

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, limit,
                        key -> {
                            if (excludedKeys.contains(key))
                                return false;

                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : filters) {
                                if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                    return false;
                                }
                            }

                            headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
                            return true;
                        });
            }
            return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
        }

        final AtomicInteger count = new AtomicInteger();
        final var keyFiles = getDbFiles(keys, getDataFileState().fileNames());
        for (final var entry : keyFiles.entrySet()) {
            if (count.get() >= limit)
                break;

            final var file = entry.getKey();
            final var keysForFile = entry.getValue(); // Iterable<String>

            try (final var shared = openDb(file)) {
                CHRONICLE_UTILS.parallelIterable(keysForFile, limit - count.get(), count,
                        key -> {
                            if (excludedKeys.contains(key))
                                return false;

                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : filters) {
                                if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                    return false;
                                }
                            }

                            headers.compareAndSet(null,
                                    CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
                            return true;
                        });
            }
        }

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    default Map<String, Map<String, Object>> searchSubset(final Iterable<String> keys,
            final List<Search> filters, final Set<String> excludedKeys, final int limit, final String[] fields)
            throws Throwable {
        if (keys == null || !keys.iterator().hasNext()) {
            return Collections.emptyMap();
        }

        Logger.info("Querying subset filtered keys at [{}] with [{}] remaining filters and [{}] excluded keys.",
                dataPath(), filters.size(), excludedKeys.size());
        final var map = new ConcurrentHashMap<String, Map<String, Object>>(limit);
        final var averageValueClass = averageValue().getClass();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, limit,
                        key -> {
                            if (excludedKeys.contains(key))
                                return false;

                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : filters) {
                                if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                    return false;
                                }
                            }

                            map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value));
                            return true;
                        });
            }
            return map;
        }

        final AtomicInteger count = new AtomicInteger();
        final var keyFiles = getDbFiles(keys, getDataFileState().fileNames());
        for (final var entry : keyFiles.entrySet()) {
            if (count.get() >= limit)
                break;

            final var file = entry.getKey();
            final var keysForFile = entry.getValue(); // Iterable<String>

            try (final var shared = openDb(file)) {
                CHRONICLE_UTILS.parallelIterable(keysForFile, limit - count.get(), count,
                        key -> {
                            if (excludedKeys.contains(key))
                                return false;

                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : filters) {
                                if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                    return false;
                                }
                            }

                            map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value));
                            return true;
                        });
            }
        }

        return map;
    }

    default CsvObject searchSubsetCsv(final Iterable<String> keys, final List<Search> filters,
            final Set<String> excludedKeys, final String[] fields, final int limit) throws Throwable {
        if (keys == null || !keys.iterator().hasNext()) {
            return CsvObject.empty();
        }

        Logger.info("Querying subset CSV filtered keys at [{}] with [{}] remaining filters and [{}] excluded keys.",
                dataPath(), filters.size(), excludedKeys.size());
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var headers = CHRONICLE_UTILS.getCsvHeaders(fields);
        final var averageValueClass = averageValue().getClass();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, limit,
                        key -> {
                            if (excludedKeys.contains(key))
                                return false;

                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : filters) {
                                if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                    return false;
                                }
                            }

                            rowQueue.add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value));
                            return true;
                        });
            }
            return new CsvObject(headers, new ArrayList<>(rowQueue));
        }

        final AtomicInteger count = new AtomicInteger();
        final var keyFiles = getDbFiles(keys, getDataFileState().fileNames());
        for (final var entry : keyFiles.entrySet()) {
            if (count.get() >= limit)
                break;

            final var file = entry.getKey();
            final var keysForFile = entry.getValue(); // Iterable<String>

            try (final var shared = openDb(file)) {
                CHRONICLE_UTILS.parallelIterable(keysForFile, limit - count.get(), count,
                        key -> {
                            if (excludedKeys.contains(key))
                                return false;

                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : filters) {
                                if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                    return false;
                                }
                            }

                            rowQueue.add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value));
                            return true;
                        });
            }
        }

        return new CsvObject(headers, new ArrayList<>(rowQueue));
    }

    default long searchCount(final Iterable<String> keys, final List<Search> filters)
            throws Throwable {
        if (keys == null || !keys.iterator().hasNext()) {
            return 0;
        }

        Logger.info("Counting filtered keys at [{}] with [{}] remaining filters.", dataPath(), filters.size());
        final var limit = Integer.MAX_VALUE;

        // Determine minimum positive limit across all filters
        final var count = new LongAdder();
        final var averageValueClass = averageValue().getClass();

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, limit,
                        key -> {
                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : filters) {
                                if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                    return false;
                                }
                            }

                            count.increment();
                            return true;
                        });
            }
            return count.sum();
        }

        final AtomicInteger counter = new AtomicInteger(0);
        final var keyFiles = getDbFiles(keys, getDataFileState().fileNames());
        for (final var entry : keyFiles.entrySet()) {
            final var file = entry.getKey();
            final var keysForFile = entry.getValue(); // Iterable<String>

            try (final var shared = openDb(file)) {
                CHRONICLE_UTILS.parallelIterable(keysForFile, limit - counter.get(), counter,
                        key -> {
                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : filters) {
                                if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                    return false;
                                }
                            }

                            count.increment();
                            return true;
                        });
            }
        }

        return count.sum();
    }

    private void search(final ChronicleMap<String, V> db, final List<Search> filters, final int limit,
            final Map<String, V> result, final AtomicInteger counter) {
        Logger.info("Searching DB at [{}] for {} filters.", dataPath(), filters.size());
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        db.forEachEntryWhile(entry -> {
            final String key = entry.key().get();
            final V value = entry.value().getUsing(using());

            for (final var search : preparedFilters) {
                if (!CHRONICLE_UTILS.search(search, key, value)) {
                    return true;
                }
            }

            // Add first, then check
            result.put(key, value);
            return counter.incrementAndGet() < limit;
        });
    }

    private void searchCsv(final ChronicleMap<String, V> db, final List<Search> filters, final int limit,
            final ConcurrentLinkedQueue<Object[]> rowQueue, final AtomicInteger counter,
            final AtomicReference<String[]> headers) {
        Logger.info("Searching DB at [{}] for {} filters.", dataPath(), filters.size());
        final var averageValueClass = averageValue().getClass();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        db.forEachEntryWhile(entry -> {
            final String key = entry.key().get();
            final V value = entry.value().getUsing(using());
            for (final var search : preparedFilters) {
                if (!CHRONICLE_UTILS.search(search, key, value)) {
                    return true;
                }
            }

            headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(classData, value));
            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
            return counter.incrementAndGet() < limit;
        });
    }

    private void searchSubset(final ChronicleMap<String, V> db, final List<Search> filters, final int limit,
            final Map<String, Map<String, Object>> result, final AtomicInteger counter,
            final String[] fields) {
        Logger.info("Subset searching DB at [{}] for {} filters.", dataPath(), filters.size());
        final var averageValueClass = averageValue().getClass();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);

        db.forEachEntryWhile(entry -> {
            final String key = entry.key().get();
            final V value = entry.value().getUsing(using());
            for (final Search search : filters) {
                if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                    return true;
                }
            }

            result.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value));
            return counter.incrementAndGet() < limit;
        });
    }

    private void searchSubsetCsv(final ChronicleMap<String, V> db, final List<Search> filters, final int limit,
            final ConcurrentLinkedQueue<Object[]> rowQueue, final AtomicInteger counter, final String[] fields) {
        Logger.info("Subset CSV searching DB at [{}] for {} filters.", dataPath(), filters.size());
        final var averageValueClass = averageValue().getClass();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);

        db.forEachEntryWhile(entry -> {
            final String key = entry.key().get();
            final V value = entry.value().getUsing(using());

            for (final Search search : filters) {
                if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                    return true;
                }
            }

            rowQueue.add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value));
            return counter.incrementAndGet() < limit;
        });
    }

    private void searchKeys(final ChronicleMap<String, V> db, final List<Search> filters,
            final Collection<String> results) {
        Logger.info("Searching DB keys at [{}] for {} filters.", dataPath(), filters.size());
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        db.forEachEntryWhile(entry -> {
            final String key = entry.key().get();
            final V value = entry.value().get();
            for (final var search : preparedFilters) {
                if (!CHRONICLE_UTILS.search(search, key, value)) {
                    return true;
                }
            }

            results.add(key);
            return true;
        });
    }

    private void search(final ChronicleMap<String, V> db, final List<Search> filters, final Set<String> excludedKeys,
            final int limit, final Map<String, V> result, final AtomicInteger counter) {
        Logger.info("Searching DB at [{}] using [{}] filters and [{}] excluded keys. Limit [{}]",
                dataPath(), filters.size(), excludedKeys.size(), limit);
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        db.forEachEntryWhile(entry -> {
            final String key = entry.key().get();
            if (excludedKeys.contains(key)) {
                return true;
            }

            final V value = entry.value().getUsing(using());
            for (final var search : preparedFilters) {
                if (!CHRONICLE_UTILS.search(search, key, value)) {
                    return true;
                }
            }

            result.put(key, value);
            return counter.incrementAndGet() < limit;
        });
    }

    private void searchCsv(final ChronicleMap<String, V> db, final List<Search> filters, final Set<String> excludedKeys,
            final int limit, final ConcurrentLinkedQueue<Object[]> rowQueue, final AtomicInteger counter,
            final AtomicReference<String[]> headers) {
        Logger.info("Searching DB at [{}] using [{}] filters and [{}] excluded keys. Limit [{}]",
                dataPath(), filters.size(), excludedKeys.size(), limit);
        final var averageValueClass = averageValue().getClass();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        db.forEachEntryWhile(entry -> {
            final String key = entry.key().get();
            if (excludedKeys.contains(key)) {
                return true;
            }

            final V value = entry.value().getUsing(using());
            for (final var search : preparedFilters) {
                if (!CHRONICLE_UTILS.search(search, key, value)) {
                    return true;
                }
            }

            headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(classData, value));
            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
            return counter.incrementAndGet() < limit;
        });
    }

    private void searchSubset(final ChronicleMap<String, V> db, final List<Search> filters,
            final Set<String> excludedKeys, final int limit, final Map<String, Map<String, Object>> result,
            final AtomicInteger counter, final String[] fields) {
        Logger.info("Searching DB at [{}] using [{}] filters and [{}] excluded keys. Limit [{}]",
                dataPath(), filters.size(), excludedKeys.size(), limit);
        final var averageValueClass = averageValue().getClass();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        db.forEachEntryWhile(entry -> {
            final String key = entry.key().get();
            if (excludedKeys.contains(key)) {
                return true;
            }

            final V value = entry.value().getUsing(using());
            for (final var search : preparedFilters) {
                if (!CHRONICLE_UTILS.search(search, key, value)) {
                    return true;
                }
            }

            result.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value));
            return counter.incrementAndGet() < limit;
        });
    }

    private void searchSubsetCsv(final ChronicleMap<String, V> db, final List<Search> filters,
            final Set<String> excludedKeys, final int limit, final ConcurrentLinkedQueue<Object[]> rowQueue,
            final AtomicInteger counter, final String[] fields) {
        Logger.info("Subset CSV searching DB at [{}] using [{}] filters and [{}] excluded keys. Limit [{}]",
                dataPath(), filters.size(), excludedKeys.size(), limit);
        final var averageValueClass = averageValue().getClass();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        db.forEachEntryWhile(entry -> {
            final String key = entry.key().get();
            if (excludedKeys.contains(key)) {
                return true;
            }

            final V value = entry.value().getUsing(using());
            for (final var search : preparedFilters) {
                if (!CHRONICLE_UTILS.search(search, key, value)) {
                    return true;
                }
            }

            rowQueue.add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value));
            return counter.incrementAndGet() < limit;
        });
    }

    private void searchKeys(final ChronicleMap<String, V> db, final List<Search> filters,
            final Set<String> excludedKeys, final Collection<String> results) {
        Logger.info("Searching DB keys at [{}] using [{}] filters and [{}] excluded keys.",
                dataPath(), filters.size(), excludedKeys.size());
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        db.forEachEntryWhile(entry -> {
            final String key = entry.key().get();
            if (excludedKeys.contains(key)) {
                return true;
            }

            final V value = entry.value().get();
            for (final var search : preparedFilters) {
                if (!CHRONICLE_UTILS.search(search, key, value)) {
                    return true;
                }
            }

            results.add(key);
            return true;
        });
    }

    private int searchCount(final ChronicleMap<String, V> db, final List<Search> filters) {
        Logger.info("Counting DB at [{}] for {} filters.", dataPath(), filters.size());
        final var count = new AtomicInteger();
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        db.forEachEntryWhile(entry -> {
            final String key = entry.key().get();
            final V value = entry.value().get();
            for (final var search : preparedFilters) {
                if (!CHRONICLE_UTILS.search(search, key, value)) {
                    return true;
                }
            }
            count.incrementAndGet();
            return true;
        });

        return count.get();
    }

    private Map<String, V> search(final Map<String, V> db, final List<Search> filters) {
        if (filters.isEmpty() || db.isEmpty()) {
            return Collections.emptyMap();
        }

        final var size = db.size();
        Logger.info("Searching in-memory map of [{}] with [{}] filters.", size, filters.size());
        final Map<String, V> result = new ConcurrentHashMap<>(Math.min(size, 10_000));
        final var averageValueClass = averageValue().getClass();

        db.entrySet().parallelStream().forEach(entry -> {
            final String key = entry.getKey();
            final V value = entry.getValue();

            if (value == null) {
                return;
            }

            boolean match = true;
            for (final Search search : filters) {
                if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                    match = false;
                    break;
                }
            }

            if (match) {
                result.put(key, value);
            }
        });

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
        Logger.info("Index searching at [{}] for {}.", dataPath(), search.field());
        if (index == null || index.isEmpty()) {
            return new SearchResult(Collections.emptyList());
        }

        final SearchType searchType = search.searchType();
        final String searchTerm = String.valueOf(search.searchTerm());
        final Set<String> searchTermSet = (searchType == SearchType.IN || searchType == SearchType.NOT_IN)
                ? new HashSet<>((List<String>) search.searchTerm())
                : null;
        final var searchTermBetween = searchType == SearchType.BETWEEN ? (List<Object>) search.searchTerm() : null;

        return switch (searchType) {
            case EQUAL -> MAP_DB.getEqualIndexSearch(index, searchTerm, search.limit());
            case NOT_EQUAL -> {
                final var keysBefore = MAP_DB.getBeforeIndexSearch(index, searchTerm, search.limit());
                final var keysAfter = MAP_DB.getAfterIndexSearch(index, searchTerm, search.limit());
                yield new SearchResult(
                        CHRONICLE_UTILS.concatIterable(keysBefore.results(), keysAfter.results(), search.limit()));
            }
            case LESS -> MAP_DB.getLessThanIndexSearch(index, searchTerm, search.limit());
            case LESS_OR_EQUAL -> MAP_DB.getLessThanOrEqualIndexSearch(index, searchTerm, search.limit());
            case GREATER -> MAP_DB.getGreaterThanIndexSearch(index, searchTerm, search.limit());
            case GREATER_OR_EQUAL -> MAP_DB.getGreaterThanOrEqualIndexSearch(index, searchTerm, search.limit());
            case LIKE -> MAP_DB.getLikeIndexSearch(index, searchTerm, search.limit());
            case NOT_LIKE -> MAP_DB.getNotLikeIndexSearch(index, searchTerm, search.limit());
            case STARTS_WITH -> MAP_DB.getStartsWithIndexSearch(index, searchTerm, search.limit());
            case ENDS_WITH -> MAP_DB.getEndsWithIndexSearch(index, searchTerm, search.limit());
            case IN -> MAP_DB.getInIndexSearch(index, searchTermSet, search.limit());
            case NOT_IN -> MAP_DB.getNotInIndexSearch(index, searchTermSet, search.limit());
            case BETWEEN -> MAP_DB.getBetweenIndexSearch(index, searchTermBetween.get(0).toString(),
                    searchTermBetween.get(1).toString(), search.limit());
            default -> throw new UnsupportedOperationException("Search type not supported: " + searchType);
        };
    }

    default SearchResult indexedSearch(final Search search, final NavigableSet<byte[]> index,
            final Set<String> excludedKeys) {
        Logger.info("Index searching at [{}] with [{}] excluded keys for {}.", dataPath(), excludedKeys.size(),
                search.field());
        if (index == null || index.isEmpty()) {
            return new SearchResult(Collections.emptyList());
        }

        final SearchType searchType = search.searchType();
        final String searchTerm = String.valueOf(search.searchTerm());
        final Set<String> searchTermSet = (searchType == SearchType.IN || searchType == SearchType.NOT_IN)
                ? new HashSet<>((List<String>) search.searchTerm())
                : null;
        final var searchTermBetween = searchType == SearchType.BETWEEN ? (List<Object>) search.searchTerm() : null;

        return switch (searchType) {
            case EQUAL -> MAP_DB.getEqualIndexSearch(index, searchTerm, search.limit(), excludedKeys);
            case NOT_EQUAL -> {
                final var keysBefore = MAP_DB.getBeforeIndexSearch(index, searchTerm, search.limit(), excludedKeys);
                final var keysAfter = MAP_DB.getAfterIndexSearch(index, searchTerm, search.limit(), excludedKeys);
                yield new SearchResult(
                        CHRONICLE_UTILS.concatIterable(keysBefore.results(), keysAfter.results(), search.limit()));
            }
            case LESS -> MAP_DB.getLessThanIndexSearch(index, searchTerm, search.limit(), excludedKeys);
            case LESS_OR_EQUAL -> MAP_DB.getLessThanOrEqualIndexSearch(index, searchTerm, search.limit(), excludedKeys);
            case GREATER -> MAP_DB.getGreaterThanIndexSearch(index, searchTerm, search.limit(), excludedKeys);
            case GREATER_OR_EQUAL ->
                MAP_DB.getGreaterThanOrEqualIndexSearch(index, searchTerm, search.limit(), excludedKeys);
            case LIKE -> MAP_DB.getLikeIndexSearch(index, searchTerm, search.limit(), excludedKeys);
            case NOT_LIKE -> MAP_DB.getNotLikeIndexSearch(index, searchTerm, search.limit(), excludedKeys);
            case STARTS_WITH -> MAP_DB.getStartsWithIndexSearch(index, searchTerm, search.limit(), excludedKeys);
            case ENDS_WITH -> MAP_DB.getEndsWithIndexSearch(index, searchTerm, search.limit(), excludedKeys);
            case IN -> MAP_DB.getInIndexSearch(index, searchTermSet, search.limit(), excludedKeys);
            case NOT_IN -> MAP_DB.getNotInIndexSearch(index, searchTermSet, search.limit(), excludedKeys);
            case BETWEEN -> MAP_DB.getBetweenIndexSearch(index, searchTermBetween.get(0).toString(),
                    searchTermBetween.get(1).toString(), search.limit(), excludedKeys);
            default -> throw new UnsupportedOperationException("Search type not supported: " + searchType);
        };
    }

    default List<String> toListOfKeys(final Iterable<String> keys) {
        return StreamSupport.stream(keys.spliterator(), true)
                .collect(Collectors.toList());
    }

    default Set<String> toSetOfKeys(final Iterable<String> keys) {
        return StreamSupport.stream(keys.spliterator(), true)
                .collect(Collectors.toSet());
    }

    private <T> boolean isResultEmpty(final Iterable<T> result) {
        return result == null || !result.iterator().hasNext();
    }

    default Map<String, V> indexedSearch(final Search search) {
        final String indexPath = getIndexPath(search.field());
        final int limit = search.limit() > 0 ? search.limit() : HARD_LIMIT;
        try (final var sharedIndexMap = MAP_DB.openIndex(indexPath)) {
            final var searchResult = indexedSearch(search, sharedIndexMap.index);
            if (isResultEmpty(searchResult.results())) {
                return Collections.emptyMap();
            }
            return get(searchResult.results(), limit);
        }
    }

    default Set<String> indexedSearchKeys(final Search search) {
        final String indexPath = getIndexPath(search.field());
        try (final var sharedIndexMap = MAP_DB.openIndex(indexPath)) {
            final var searchResult = indexedSearch(search, sharedIndexMap.index);
            if (isResultEmpty(searchResult.results())) {
                return Collections.emptySet();
            }
            return toSetOfKeys(searchResult.results());
        }
    }

    default List<String> indexedSearchKeysList(final Search search) {
        final String indexPath = getIndexPath(search.field());
        try (final var sharedIndexMap = MAP_DB.openIndex(indexPath)) {
            final var searchResult = indexedSearch(search, sharedIndexMap.index);
            if (isResultEmpty(searchResult.results())) {
                return Collections.emptyList();
            }
            return toListOfKeys(searchResult.results());
        }
    }

    default Map<String, V> indexedSearch(final Search search, final Set<String> excludedKeys) {
        final String indexPath = getIndexPath(search.field());
        final int limit = search.limit() > 0 ? search.limit() : HARD_LIMIT;
        try (final var sharedIndexMap = MAP_DB.openIndex(indexPath)) {
            final var searchResult = indexedSearch(search, sharedIndexMap.index, excludedKeys);
            if (isResultEmpty(searchResult.results())) {
                return Collections.emptyMap();
            }
            return get(searchResult.results(), limit);
        }
    }

    default Set<String> indexedSearchKeys(final Search search, final Set<String> excludedKeys) {
        final String indexPath = getIndexPath(search.field());
        try (final var sharedIndexMap = MAP_DB.openIndex(indexPath)) {
            final var searchResult = indexedSearch(search, sharedIndexMap.index, excludedKeys);
            if (isResultEmpty(searchResult.results())) {
                return Collections.emptySet();
            }
            return toSetOfKeys(searchResult.results());
        }
    }

    default List<String> indexedSearchKeysList(final Search search, final Set<String> excludedKeys) {
        final String indexPath = getIndexPath(search.field());
        try (final var sharedIndexMap = MAP_DB.openIndex(indexPath)) {
            final var searchResult = indexedSearch(search, sharedIndexMap.index, excludedKeys);
            if (isResultEmpty(searchResult.results())) {
                return Collections.emptyList();
            }
            return toListOfKeys(searchResult.results());
        }
    }

    default Map<String, V> searchMatchingKeys(final Set<String> matchingKeys, final Search search) throws Throwable {
        if (matchingKeys == null || matchingKeys.isEmpty()) {
            return Collections.emptyMap();
        }

        return search(matchingKeys, List.of(search), matchingKeys.size());
    }

    default Map<String, V> search(final Map<String, V> db, final Search search) {
        if (db == null || db.isEmpty()) {
            return Collections.emptyMap();
        }
        return search(db, List.of(search));
    }

    default Map<String, V> search(final Search search) {
        final int limit = search.limit() == -1 ? HARD_LIMIT : search.limit();
        final Map<String, V> result = new ConcurrentHashMap<>(limit);
        final AtomicInteger counter = new AtomicInteger(0);

        getDataFileState().fileNames().parallelStream().forEach(file -> {
            if (counter.get() >= limit) {
                return;
            }
            try (final var shared = openDb(file)) {
                search(shared.map, List.of(search), limit, result, counter);
            }
        });

        return result;
    }

    default Set<String> searchKeys(final List<Search> searches) {
        final var results = ConcurrentHashMap.<String>newKeySet();
        getDataFileState().fileNames().parallelStream()
                .forEach(file -> {
                    try (final var shared = openDb(file)) {
                        searchKeys(shared.map, searches, results);
                    }
                });
        return results;
    }

    default List<String> searchKeysList(final List<Search> searches) {
        final var result = new ConcurrentLinkedQueue<String>();
        getDataFileState().fileNames().parallelStream()
                .forEach(file -> {
                    try (final var shared = openDb(file)) {
                        searchKeys(shared.map, searches, result);
                    }
                });
        return new ArrayList<>(result);
    }

    default Map<String, V> search(final Search search, final Set<String> excludedKeys) {
        final int limit = search.limit() == -1 ? HARD_LIMIT : search.limit();
        final Map<String, V> result = new ConcurrentHashMap<>(limit);
        final AtomicInteger counter = new AtomicInteger(0);

        getDataFileState().fileNames().parallelStream().forEach(file -> {
            if (counter.get() >= limit) {
                return;
            }
            try (final var shared = openDb(file)) {
                search(shared.map, List.of(search), excludedKeys, limit, result, counter);
            }
        });

        return result;
    }

    default Set<String> searchKeys(final List<Search> searches, final Set<String> excludedKeys) {
        final var results = ConcurrentHashMap.<String>newKeySet();
        getDataFileState().fileNames().parallelStream()
                .forEach(file -> {
                    try (final var shared = openDb(file)) {
                        searchKeys(shared.map, searches, excludedKeys, results);
                    }
                });
        return results;
    }

    default List<String> searchKeysList(final List<Search> searches, final Set<String> excludedKeys) {
        final var result = new ConcurrentLinkedQueue<String>();
        getDataFileState().fileNames().parallelStream().forEach(file -> {
            try (final var shared = openDb(file)) {
                searchKeys(shared.map, searches, excludedKeys, result);
            }
        });
        return new ArrayList<>(result);
    }

    default Map<String, Map<String, Object>> multiSearchSubset(final List<Search> searches,
            final String[] fields) throws Throwable {
        if (searches == null || searches.isEmpty()) {
            return Collections.emptyMap();
        }

        final Set<String> indexFileNames = indexFileNames();

        // Step 1: Separate searches into first-indexed and remaining
        Search indexedSearch = null;
        final List<Search> remainingSearches = new ArrayList<>(searches.size());
        int minLimit = Integer.MAX_VALUE;

        for (final Search s : searches) {
            // Compute limit
            if (s.limit() > 0 && s.limit() < minLimit) {
                minLimit = s.limit();
            }

            // separate searches
            final var exclusions = indexExclusions().get(s.field());
            if (indexedSearch == null && indexFileNames.contains(s.field()) && !s.skipIndex()
                    && (exclusions == null || !exclusions.contains(s.searchTerm()))) {
                indexedSearch = s;
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = (minLimit == Integer.MAX_VALUE) ? HARD_LIMIT : minLimit;

        SearchResult searchResult = null;
        String indexPath = null;
        SharedIndexMap sharedIndexMap = null;
        // Step 2: Perform indexed search (only if indexedSearch != null)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            sharedIndexMap = MAP_DB.openIndex(indexPath);
            searchResult = indexedSearch(indexedSearch, sharedIndexMap.index);
            if (isResultEmpty(searchResult.results())) {
                sharedIndexMap.close();
                return Collections.emptyMap();
            }

            if (remainingSearches.isEmpty()) {
                try {
                    return getSubset(searchResult.results(), fields, limit);
                } finally {
                    sharedIndexMap.close();
                }
            }
        }

        // Step 3: Manual search
        try {
            if (searchResult == null) {
                final var result = new ConcurrentHashMap<String, Map<String, Object>>(limit);
                final var counter = new AtomicInteger();
                getDataFileState().fileNames().parallelStream().forEach(file -> {
                    if (counter.get() >= limit) {
                        return;
                    }
                    try (final var shared = openDb(file)) {
                        searchSubset(shared.map, searches, limit, result, counter, fields);
                    }
                });

                return result;
            } else {
                return searchSubset(searchResult.results(), remainingSearches, fields, limit);
            }
        } finally {
            if (sharedIndexMap != null) {
                sharedIndexMap.close();
            }
        }
    }

    default CsvObject multiSearchSubsetCsv(final List<Search> searches, final String[] fields) throws Throwable {
        if (searches == null || searches.isEmpty()) {
            return CsvObject.empty();
        }

        final Set<String> indexFileNames = indexFileNames();

        // Step 1: Separate searches into first-indexed and remaining
        Search indexedSearch = null;
        final List<Search> remainingSearches = new ArrayList<>(searches.size());
        int minLimit = Integer.MAX_VALUE;

        for (final Search s : searches) {
            // Compute limit
            if (s.limit() > 0 && s.limit() < minLimit) {
                minLimit = s.limit();
            }

            // separate searches
            final var exclusions = indexExclusions().get(s.field());
            if (indexedSearch == null && indexFileNames.contains(s.field()) && !s.skipIndex()
                    && (exclusions == null || !exclusions.contains(s.searchTerm()))) {
                indexedSearch = s;
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = (minLimit == Integer.MAX_VALUE) ? HARD_LIMIT : minLimit;

        SearchResult searchResult = null;
        String indexPath = null;
        SharedIndexMap sharedIndexMap = null;
        // Step 2: Perform indexed search (only if indexedSearch != null)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            sharedIndexMap = MAP_DB.openIndex(indexPath);
            searchResult = indexedSearch(indexedSearch, sharedIndexMap.index);
            if (isResultEmpty(searchResult.results())) {
                sharedIndexMap.close();
                return CsvObject.empty();
            }

            if (remainingSearches.isEmpty()) {
                try {
                    return getSubsetCsv(searchResult.results(), fields, limit);
                } finally {
                    sharedIndexMap.close();
                }
            }
        }

        // Step 3: Manual search
        try {
            if (searchResult == null) {
                final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
                final var counter = new AtomicInteger();
                getDataFileState().fileNames().parallelStream().forEach(file -> {
                    if (counter.get() >= limit) {
                        return;
                    }
                    try (final var shared = openDb(file)) {
                        searchSubsetCsv(shared.map, searches, limit, rowQueue, counter, fields);
                    }
                });

                final var headers = CHRONICLE_UTILS.getCsvHeaders(fields);
                return new CsvObject(headers, new ArrayList<>(rowQueue));
            } else {
                return searchSubsetCsv(searchResult.results(), remainingSearches, fields, limit);
            }
        } finally {
            if (sharedIndexMap != null) {
                sharedIndexMap.close();
            }
        }
    }

    default Map<String, V> multiSearch(final List<Search> searches) throws Throwable {
        if (searches == null || searches.isEmpty()) {
            return Collections.emptyMap();
        }

        final Set<String> indexFileNames = indexFileNames();

        // Step 1: Separate searches into first-indexed and remaining
        Search indexedSearch = null;
        final List<Search> remainingSearches = new ArrayList<>(searches.size());
        int minLimit = Integer.MAX_VALUE;

        for (final Search s : searches) {
            // Compute limit
            if (s.limit() > 0 && s.limit() < minLimit) {
                minLimit = s.limit();
            }

            // separate searches
            final var exclusions = indexExclusions().get(s.field());
            if (indexedSearch == null && indexFileNames.contains(s.field()) && !s.skipIndex()
                    && (exclusions == null || !exclusions.contains(s.searchTerm()))) {
                indexedSearch = s;
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = (minLimit == Integer.MAX_VALUE) ? HARD_LIMIT : minLimit;

        SearchResult searchResult = null;
        String indexPath = null;
        SharedIndexMap sharedIndexMap = null;
        // Step 2: Perform indexed search (only if indexedSearch != null)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            sharedIndexMap = MAP_DB.openIndex(indexPath);
            searchResult = indexedSearch(indexedSearch, sharedIndexMap.index);
            if (isResultEmpty(searchResult.results())) {
                sharedIndexMap.close();
                return Collections.emptyMap();
            }

            if (remainingSearches.isEmpty()) {
                try {
                    return get(searchResult.results(), limit);
                } finally {
                    sharedIndexMap.close();
                }
            }
        }

        // Step 3: Manual search
        try {
            if (searchResult == null) {
                final Map<String, V> result = new ConcurrentHashMap<>(limit);
                final var counter = new AtomicInteger();
                getDataFileState().fileNames().parallelStream().forEach(file -> {
                    if (counter.get() >= limit) {
                        return;
                    }
                    try (final var shared = openDb(file)) {
                        search(shared.map, searches, limit, result, counter);
                    }
                });

                return result;
            } else {
                return search(searchResult.results(), remainingSearches, limit);
            }
        } finally {
            if (sharedIndexMap != null) {
                sharedIndexMap.close();
            }
        }
    }

    default CsvObject multiSearchCsv(final List<Search> searches) throws Throwable {
        if (searches == null || searches.isEmpty()) {
            return CsvObject.empty();
        }

        final Set<String> indexFileNames = indexFileNames();

        // Step 1: Separate searches into first-indexed and remaining
        Search indexedSearch = null;
        final List<Search> remainingSearches = new ArrayList<>(searches.size());
        int minLimit = Integer.MAX_VALUE;

        for (final Search s : searches) {
            // Compute limit
            if (s.limit() > 0 && s.limit() < minLimit) {
                minLimit = s.limit();
            }

            // separate searches
            final var exclusions = indexExclusions().get(s.field());
            if (indexedSearch == null && indexFileNames.contains(s.field()) && !s.skipIndex()
                    && (exclusions == null || !exclusions.contains(s.searchTerm()))) {
                indexedSearch = s;
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = (minLimit == Integer.MAX_VALUE) ? HARD_LIMIT : minLimit;

        SearchResult searchResult = null;
        String indexPath = null;
        SharedIndexMap sharedIndexMap = null;
        // Step 2: Perform indexed search (only if indexedSearch != null)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            sharedIndexMap = MAP_DB.openIndex(indexPath);
            searchResult = indexedSearch(indexedSearch, sharedIndexMap.index);
            if (isResultEmpty(searchResult.results())) {
                sharedIndexMap.close();
                return CsvObject.empty();
            }

            if (remainingSearches.isEmpty()) {
                try {
                    return getCsv(searchResult.results(), limit);
                } finally {
                    sharedIndexMap.close();
                }
            }
        }

        // Step 3: Manual search
        try {
            if (searchResult == null) {
                final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
                final AtomicReference<String[]> headers = new AtomicReference<>(null);
                final var counter = new AtomicInteger();
                getDataFileState().fileNames().parallelStream().forEach(file -> {
                    if (counter.get() >= limit) {
                        return;
                    }
                    try (final var shared = openDb(file)) {
                        searchCsv(shared.map, searches, limit, rowQueue, counter, headers);
                    }
                });

                return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
            } else {
                return searchCsv(searchResult.results(), remainingSearches, limit);
            }
        } finally {
            if (sharedIndexMap != null) {
                sharedIndexMap.close();
            }
        }
    }

    default Set<String> multiSearchKeys(final List<Search> searches) throws Throwable {
        if (searches == null || searches.isEmpty()) {
            return Collections.emptySet();
        }

        final Set<String> indexFileNames = indexFileNames();

        // Step 1: Separate searches into first-indexed and remaining
        Search indexedSearch = null;
        final List<Search> remainingSearches = new ArrayList<>(searches.size());

        for (final Search s : searches) {
            if (!s.skipIndex() && indexedSearch == null && indexFileNames.contains(s.field())) {
                indexedSearch = s;
            } else {
                remainingSearches.add(s);
            }
        }

        SearchResult searchResult = null;
        String indexPath = null;
        SharedIndexMap sharedIndexMap = null;
        // Step 2: Perform indexed search (only if indexedSearch != null)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            sharedIndexMap = MAP_DB.openIndex(indexPath);
            searchResult = indexedSearch(indexedSearch, sharedIndexMap.index);
            if (isResultEmpty(searchResult.results())) {
                sharedIndexMap.close();
                return Collections.emptySet();
            }

            if (remainingSearches.isEmpty()) {
                try {
                    return toSetOfKeys(searchResult.results());
                } finally {
                    sharedIndexMap.close();
                }
            }
        }

        // Step 3: Manual search
        try {
            if (searchResult == null) {
                return searchKeys(searches);
            } else {
                final var results = ConcurrentHashMap.<String>newKeySet();
                searchKeys(searchResult.results(), remainingSearches, results);
                return results;
            }
        } finally {
            if (sharedIndexMap != null) {
                sharedIndexMap.close();
            }
        }
    }

    default List<String> multiSearchKeysList(final List<Search> searches) throws Throwable {
        if (searches == null || searches.isEmpty()) {
            return Collections.emptyList();
        }

        final Set<String> indexFileNames = indexFileNames();

        // Step 1: Separate searches into first-indexed and remaining
        Search indexedSearch = null;
        final List<Search> remainingSearches = new ArrayList<>(searches.size());

        for (final Search s : searches) {
            if (!s.skipIndex() && indexedSearch == null && indexFileNames.contains(s.field())) {
                indexedSearch = s;
            } else {
                remainingSearches.add(s);
            }
        }

        SearchResult searchResult = null;
        String indexPath = null;
        SharedIndexMap sharedIndexMap = null;
        // Step 2: Perform indexed search (only if indexedSearch != null)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            sharedIndexMap = MAP_DB.openIndex(indexPath);
            searchResult = indexedSearch(indexedSearch, sharedIndexMap.index);
            if (isResultEmpty(searchResult.results())) {
                sharedIndexMap.close();
                return Collections.emptyList();
            }

            if (remainingSearches.isEmpty()) {
                try {
                    return toListOfKeys(searchResult.results());
                } finally {
                    sharedIndexMap.close();
                }
            }
        }

        // Step 3: Manual search
        try {
            if (searchResult == null) {
                return searchKeysList(searches);
            } else {
                final var results = new ConcurrentLinkedQueue<String>();
                searchKeys(searchResult.results(), remainingSearches, results);
                return new ArrayList<>(results);
            }
        } finally {
            if (sharedIndexMap != null) {
                sharedIndexMap.close();
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
        final List<Search> remainingSearches = new ArrayList<>(searches.size());
        int minLimit = Integer.MAX_VALUE;

        for (final Search s : searches) {
            // Compute limit
            if (s.limit() > 0 && s.limit() < minLimit) {
                minLimit = s.limit();
            }

            // separate searches
            final var exclusions = indexExclusions().get(s.field());
            if (indexedSearch == null && indexFileNames.contains(s.field()) && !s.skipIndex()
                    && (exclusions == null || !exclusions.contains(s.searchTerm()))) {
                indexedSearch = s;
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = (minLimit == Integer.MAX_VALUE) ? HARD_LIMIT : minLimit;

        SearchResult searchResult = null;
        String indexPath = null;
        SharedIndexMap sharedIndexMap = null;
        // Step 2: Perform indexed search (only if indexedSearch != null)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            sharedIndexMap = MAP_DB.openIndex(indexPath);
            searchResult = indexedSearch(indexedSearch, sharedIndexMap.index);
            if (isResultEmpty(searchResult.results())) {
                sharedIndexMap.close();
                return Collections.emptyMap();
            }

            if (remainingSearches.isEmpty()) {
                try {
                    return get(searchResult.results(), excludedKeys, limit);
                } finally {
                    sharedIndexMap.close();
                }
            }
        }

        // Step 3: Manual search
        try {
            if (searchResult == null) {
                final Map<String, V> result = new ConcurrentHashMap<>(limit);
                final var counter = new AtomicInteger(0);
                getDataFileState().fileNames().parallelStream().forEach(file -> {
                    if (counter.get() >= limit) {
                        return;
                    }
                    try (final var shared = openDb(file)) {
                        search(shared.map, searches, excludedKeys, limit, result, counter);
                    }
                });

                return result;
            } else {
                return search(searchResult.results(), remainingSearches, excludedKeys, limit);
            }
        } finally {
            if (sharedIndexMap != null) {
                sharedIndexMap.close();
            }
        }
    }

    default CsvObject multiSearchCsv(final List<Search> searches, final Set<String> excludedKeys) throws Throwable {
        if (searches == null || searches.isEmpty()) {
            return CsvObject.empty();
        }

        final Set<String> indexFileNames = indexFileNames();

        // Step 1: Separate searches into first-indexed and remaining
        Search indexedSearch = null;
        final List<Search> remainingSearches = new ArrayList<>(searches.size());
        int minLimit = Integer.MAX_VALUE;

        for (final Search s : searches) {
            // Compute limit
            if (s.limit() > 0 && s.limit() < minLimit) {
                minLimit = s.limit();
            }

            // separate searches
            final var exclusions = indexExclusions().get(s.field());
            if (indexedSearch == null && indexFileNames.contains(s.field()) && !s.skipIndex()
                    && (exclusions == null || !exclusions.contains(s.searchTerm()))) {
                indexedSearch = s;
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = (minLimit == Integer.MAX_VALUE) ? HARD_LIMIT : minLimit;

        SearchResult searchResult = null;
        String indexPath = null;
        SharedIndexMap sharedIndexMap = null;
        // Step 2: Perform indexed search (only if indexedSearch != null)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            sharedIndexMap = MAP_DB.openIndex(indexPath);
            searchResult = indexedSearch(indexedSearch, sharedIndexMap.index);
            if (isResultEmpty(searchResult.results())) {
                sharedIndexMap.close();
                return CsvObject.empty();
            }

            if (remainingSearches.isEmpty()) {
                try {
                    return getCsv(searchResult.results(), excludedKeys, limit);
                } finally {
                    sharedIndexMap.close();
                }
            }
        }

        // Step 3: Manual search
        try {
            if (searchResult == null) {
                final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
                final AtomicReference<String[]> headers = new AtomicReference<>(null);
                final var counter = new AtomicInteger(0);
                getDataFileState().fileNames().parallelStream().forEach(file -> {
                    if (counter.get() >= limit) {
                        return;
                    }
                    try (final var shared = openDb(file)) {
                        searchCsv(shared.map, searches, excludedKeys, limit, rowQueue, counter, headers);
                    }
                });

                return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
            } else {
                return searchCsv(searchResult.results(), remainingSearches, excludedKeys, limit);
            }
        } finally {
            if (sharedIndexMap != null) {
                sharedIndexMap.close();
            }
        }
    }

    default Map<String, Map<String, Object>> multiSearchSubset(final List<Search> searches,
            final Set<String> excludedKeys, final String[] fields) throws Throwable {
        if (searches == null || searches.isEmpty()) {
            return Collections.emptyMap();
        }

        final Set<String> indexFileNames = indexFileNames();

        // Step 1: Separate searches into first-indexed and remaining
        Search indexedSearch = null;
        final List<Search> remainingSearches = new ArrayList<>(searches.size());
        int minLimit = Integer.MAX_VALUE;

        for (final Search s : searches) {
            // Compute limit
            if (s.limit() > 0 && s.limit() < minLimit) {
                minLimit = s.limit();
            }

            // separate searches
            final var exclusions = indexExclusions().get(s.field());
            if (indexedSearch == null && indexFileNames.contains(s.field()) && !s.skipIndex()
                    && (exclusions == null || !exclusions.contains(s.searchTerm()))) {
                indexedSearch = s;
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = (minLimit == Integer.MAX_VALUE) ? HARD_LIMIT : minLimit;

        SearchResult searchResult = null;
        String indexPath = null;
        SharedIndexMap sharedIndexMap = null;
        // Step 2: Perform indexed search (only if indexedSearch != null)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            sharedIndexMap = MAP_DB.openIndex(indexPath);
            searchResult = indexedSearch(indexedSearch, sharedIndexMap.index);
            if (isResultEmpty(searchResult.results())) {
                sharedIndexMap.close();
                return Collections.emptyMap();
            }

            if (remainingSearches.isEmpty()) {
                try {
                    return getSubset(searchResult.results(), excludedKeys, fields, limit);
                } finally {
                    sharedIndexMap.close();
                }
            }
        }

        // Step 3: Manual search
        try {
            if (searchResult == null) {
                final var result = new ConcurrentHashMap<String, Map<String, Object>>(limit);
                final var counter = new AtomicInteger(0);
                getDataFileState().fileNames().parallelStream().forEach(file -> {
                    if (counter.get() >= limit) {
                        return;
                    }
                    try (final var shared = openDb(file)) {
                        searchSubset(shared.map, searches, excludedKeys, limit, result, counter, fields);
                    }
                });

                return result;
            } else {
                return searchSubset(searchResult.results(), remainingSearches, excludedKeys, limit, fields);
            }
        } finally {
            if (sharedIndexMap != null) {
                sharedIndexMap.close();
            }
        }
    }

    default CsvObject multiSearchSubsetCsv(final List<Search> searches, final Set<String> excludedKeys,
            final String[] fields) throws Throwable {
        if (searches == null || searches.isEmpty()) {
            return CsvObject.empty();
        }

        final Set<String> indexFileNames = indexFileNames();

        // Step 1: Separate searches into first-indexed and remaining
        Search indexedSearch = null;
        final List<Search> remainingSearches = new ArrayList<>(searches.size());
        int minLimit = Integer.MAX_VALUE;

        for (final Search s : searches) {
            // Compute limit
            if (s.limit() > 0 && s.limit() < minLimit) {
                minLimit = s.limit();
            }

            // separate searches
            final var exclusions = indexExclusions().get(s.field());
            if (indexedSearch == null && indexFileNames.contains(s.field()) && !s.skipIndex()
                    && (exclusions == null || !exclusions.contains(s.searchTerm()))) {
                indexedSearch = s;
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = (minLimit == Integer.MAX_VALUE) ? HARD_LIMIT : minLimit;

        SearchResult searchResult = null;
        String indexPath = null;
        SharedIndexMap sharedIndexMap = null;
        // Step 2: Perform indexed search (only if indexedSearch != null)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            sharedIndexMap = MAP_DB.openIndex(indexPath);
            searchResult = indexedSearch(indexedSearch, sharedIndexMap.index);
            if (isResultEmpty(searchResult.results())) {
                sharedIndexMap.close();
                return CsvObject.empty();
            }

            if (remainingSearches.isEmpty()) {
                try {
                    return getSubsetCsv(searchResult.results(), excludedKeys, fields, limit);
                } finally {
                    sharedIndexMap.close();
                }
            }
        }

        // Step 3: Manual search
        try {
            if (searchResult == null) {
                final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
                final var counter = new AtomicInteger(0);
                getDataFileState().fileNames().parallelStream().forEach(file -> {
                    if (counter.get() >= limit) {
                        return;
                    }
                    try (final var shared = openDb(file)) {
                        searchSubsetCsv(shared.map, searches, excludedKeys, limit, rowQueue, counter, fields);
                    }
                });

                final var headers = CHRONICLE_UTILS.getCsvHeaders(fields);
                return new CsvObject(headers, new ArrayList<>(rowQueue));
            } else {
                return searchSubsetCsv(searchResult.results(), remainingSearches, excludedKeys, fields, limit);
            }
        } finally {
            if (sharedIndexMap != null) {
                sharedIndexMap.close();
            }
        }
    }

    default Map<String, V> multiSearch(final Set<String> matchingKeys, final List<Search> searches) throws Throwable {
        final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(HARD_LIMIT);

        return search(matchingKeys, searches, limit);
    }

    default CsvObject multiSearchCsv(final Set<String> matchingKeys, final List<Search> searches) throws Throwable {
        final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(HARD_LIMIT);

        return searchCsv(matchingKeys, searches, limit);
    }

    default Map<String, Map<String, Object>> multiSearchSubset(final Set<String> matchingKeys,
            final List<Search> searches, final String[] fields) throws Throwable {
        final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(HARD_LIMIT);

        return searchSubset(matchingKeys, searches, fields, limit);
    }

    default CsvObject multiSearchSubsetCsv(final Set<String> matchingKeys,
            final List<Search> searches, final String[] fields) throws Throwable {
        final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(HARD_LIMIT);

        return searchSubsetCsv(matchingKeys, searches, fields, limit);
    }

    default long multiSearchCount(final List<Search> searches) throws Throwable {
        if (searches == null || searches.isEmpty()) {
            return 0;
        }

        final Set<String> indexFileNames = indexFileNames();

        // Step 1: Separate searches into one indexed + remaining
        Search indexedSearch = null;
        final List<Search> remainingSearches = new ArrayList<>(searches.size());

        for (final Search s : searches) {
            // separate searches
            final var exclusions = indexExclusions().get(s.field());
            if (indexedSearch == null && indexFileNames.contains(s.field()) && !s.skipIndex()
                    && (exclusions == null || !exclusions.contains(s.searchTerm()))) {
                indexedSearch = s;
            } else {
                remainingSearches.add(s);
            }
        }

        String indexPath = null;
        SearchResult searchResult = null;
        SharedIndexMap sharedIndexMap = null;
        // Step 2: Indexed search (only first index used)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            sharedIndexMap = MAP_DB.openIndex(indexPath);
            searchResult = indexedSearch(indexedSearch, sharedIndexMap.index);
            if (isResultEmpty(searchResult.results())) {
                sharedIndexMap.close();
                return 0;
            }
            if (remainingSearches.isEmpty()) {
                try {
                    return MAP_DB.fastCount(searchResult.results());
                } finally {
                    sharedIndexMap.close();
                }
            }
        }

        try {
            if (searchResult == null) {
                return getDataFileState().fileNames().parallelStream()
                        .mapToInt(file -> {
                            try (final var shared = openDb(file)) {
                                return searchCount(shared.map, searches);
                            }
                        })
                        .sum();
            } else {
                return searchCount(searchResult.results(), remainingSearches);
            }
        } finally {
            if (sharedIndexMap != null) {
                sharedIndexMap.close();
            }
        }
    }

    /**
     * Current size of the data
     * 
     * @return int size
     */
    default int size() {
        Logger.info("Getting DB size at [{}].", dataPath());

        if (getDataFileState().fileNames().size() > 1) {
            return getKeyMapSize();
        }

        try (var shared = openDb()) {
            return shared.map.size();
        }
    }

    default void truncate() throws IOException {
        Logger.info("Dropping database at [{}].", dataPath());
        backup();
        deleteDataFiles();
        deleteIndexes();
        CHRONICLE_UTILS.deleteFileIfExists(getKeyMapPath());
        DATA_FILE_CACHE.remove(dataPath());
    }

    default boolean exists(final String key) {
        Logger.info("Checking [{}] existence at [{}].", key, dataPath());

        if (getDataFileState().fileNames().size() > 1) {
            return getKeyMapExists(key);
        }

        try (var shared = openDb()) {
            return shared.map.containsKey(key);
        }
    }

    default Map<String, Boolean> existsMultiple(final Collection<String> keys) {
        Logger.info("Checking [{}] keys existence at [{}].", keys.size(), dataPath());

        if (getDataFileState().fileNames().size() > 1) {
            return getKeyMapExists(keys);
        }

        final var containsMap = new HashMap<String, Boolean>(keys.size());
        try (var shared = openDb()) {
            if (shared.map.size() == 0) {
                return containsMap;
            }

            for (final var key : keys) {
                containsMap.put(key, shared.map.containsKey(key));
            }
        }

        return containsMap;
    }

    /**
     * Returns only the keys that exist
     */
    default List<String> existsList(final Collection<String> keys) {
        Logger.info("Checking [{}] keys existence at [{}].", keys.size(), dataPath());

        if (getDataFileState().fileNames().size() > 1) {
            return getKeyMapExistsList(keys);
        }

        final var list = new ArrayList<String>(keys.size());
        try (var shared = openDb()) {
            if (shared.map.size() == 0) {
                return list;
            }

            for (final var key : keys) {
                if (shared.map.containsKey(key)) {
                    list.add(key);
                }
            }
        }

        return list;
    }

    /**
     * Returns only the keys that dont exist
     */
    default List<String> notExistsList(final Collection<String> keys) {
        Logger.info("Checking [{}] keys non-existence at [{}].", keys.size(), dataPath());

        if (getDataFileState().fileNames().size() > 1) {
            return getKeyMapNotExistsList(keys);
        }

        final var list = new ArrayList<String>(keys.size());
        try (var shared = openDb()) {
            if (shared.map.size() == 0) {
                return list;
            }

            for (final var key : keys) {
                if (!shared.map.containsKey(key)) {
                    list.add(key);
                }
            }
        }

        return list;
    }
}
