package chronicle.db.dao;

import static chronicle.db.service.ChronicleDb.CHRONICLE_DB;
import static chronicle.db.service.MapDb.MAP_DB;
import static chronicle.db.utils.ChronicleUtils.CHRONICLE_UTILS;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.mapdb.HTreeMap;
import org.tinylog.Logger;

import com.jsoniter.spi.TypeLiteral;

import chronicle.db.entity.CsvObject;
import chronicle.db.entity.PutStatus;
import chronicle.db.entity.Search;
import chronicle.db.entity.Search.SearchType;
import chronicle.db.service.ChronicleDb.SharedChronicleMap;
import chronicle.db.service.MapDb.KeyMapValue;
import chronicle.db.service.MapDb.SearchResult;
import chronicle.db.service.MapDb.SharedIndexSet;
import chronicle.db.utils.SafeRunnable;
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
 * reentrant locks per data path to prevent conflicts during file rotation and
 * index
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

    ConcurrentMap<String, ReentrantLock> WRITE_LOCKS = new ConcurrentHashMap<>();
    ConcurrentMap<String, DataFileState> DATA_FILE_CACHE = new ConcurrentHashMap<>();
    ConcurrentMap<String, String> KEY_MAP_PATH_CACHE = new ConcurrentHashMap<>();
    String DATA_DIR = "/data/", INDEX_DIR = "/indexes/", FILES_DIR = "/files/", BACKUP_DIR = "/backup/",
            DATA_FILE = "data", CORRUPTED_FILE = "corrupted", RECOVER_FILE = "recovery", ENTRY_SIZE_FILE = "entrySize",
            KEY_FILE = "keys";
    String[] DB_DIRS = { DATA_DIR, INDEX_DIR, FILES_DIR, BACKUP_DIR };
    int HARD_LIMIT = 100_000;
    String RECOVERY_MODE_PROPERTY = "chronicle.recovery.mode";
    boolean IN_RECOVERY = Boolean.getBoolean(RECOVERY_MODE_PROPERTY);

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
    default String averageKey() {
        return "k".repeat(Integer.getInteger("chronicle." + name() + ".key.size", 36));
    }

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

    default String getKeyMapPath() {
        return KEY_MAP_PATH_CACHE.computeIfAbsent(dataPath(), k -> k + DATA_DIR + KEY_FILE);
    }

    private void populateKeyMap(final Set<String> dataFiles, final HTreeMap<byte[], KeyMapValue> keyMap) {
        CHRONICLE_UTILS.processInParallel(dataFiles, file -> {
            try (final var shared = openDb(file)) {
                // Collect keys first (forEachEntry doesn't support parallel)
                final var keys = new ArrayList<String>((int) shared.map.size());
                CHRONICLE_UTILS.safeForEachEntry(shared, entry -> keys.add(entry.key().get()));

                // Parallel: compute hash + insert (HTreeMap is thread-safe)
                keys.parallelStream().forEach(primaryKey -> keyMap.put(CHRONICLE_UTILS.to128BitHash(primaryKey),
                        new KeyMapValue(primaryKey, file)));
            }
        });
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

        // Skip keymap initialization in recovery mode to avoid deadlocks
        if (IN_RECOVERY) {
            return;
        }

        CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
            final var dataFileState = getDataFileState();
            final var keyMapPath = getKeyMapPath();
            if (!Files.exists(Path.of(keyMapPath))) {
                try (final var sharedKeyMap = MAP_DB.openMap(keyMapPath, entries())) {
                    populateKeyMap(dataFileState.fileNames(), sharedKeyMap.map);
                } catch (final Exception e) {
                    // Delete corrupt keyMap so it rebuilds on next startup
                    // (try-with-resources already closed the map)
                    Logger.error("Failed to populate KeyMap at [{}]. Deleting for rebuild.", keyMapPath);
                    CHRONICLE_UTILS.deleteFileIfExists(keyMapPath);
                    throw e;
                }
                Logger.info("Initialized KeyMap at [{}]", dataPath());
            }
        });
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
     * Checks if this DAO needs vacuuming based on file fragmentation.
     * <p>
     * A DAO needs vacuuming when the actual number of data files exceeds the
     * expected
     * number based on the record count and configured entries per file.
     * <p>
     * Expected files = ceil(size() / entries()) or 1 if empty
     *
     * @return VacuumInfo if vacuum is needed, null otherwise
     */
    default VacuumInfo needsVacuum() {
        final int size = size();
        final long entriesPerFile = entries();
        final int actualFiles = getDataFileState().fileNames().size();
        final int expectedFiles = size == 0 ? 1 : (int) Math.ceil((double) size / entriesPerFile);

        if (actualFiles > expectedFiles) {
            return new VacuumInfo(dataPath(), actualFiles, expectedFiles, size, entriesPerFile);
        }
        return null;
    }

    /**
     * Information about vacuum requirements for a DAO.
     */
    record VacuumInfo(String dataPath, int actualFiles, int expectedFiles, int recordCount, long entriesPerFile) {
        public int excessFiles() {
            return actualFiles - expectedFiles;
        }

        @Override
        public String toString() {
            return String.format("%s: %d files (expected %d) with %d records (%d entries/file)",
                    dataPath, actualFiles, expectedFiles, recordCount, entriesPerFile);
        }
    }

    /**
     * Only runs to initialize an index on the field first time
     * 
     * @param field the field of the V value object
     * 
     */
    private void initIndex(final SharedChronicleMap<String, V> db, final Set<String> fields) {
        CHRONICLE_UTILS.index(db, name(), fields, dataPath(), averageValue().getClass(), indexExclusions(), entries());
    }

    /**
     * Only runs to initialize an index on the field first time
     * 
     * @param field the field of the V value object
     * 
     */
    private void initIndex(final Set<String> fields) {
        CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
            try (final var shared = openDb(file)) {
                initIndex(shared, fields);
            }
        });
    }

    /**
     * Delete and rerun all indexes. Faster when inserting a lot of records.
     * 
     */
    default void refreshIndexes() {
        final var indexFiles = indexFileNames();
        if (!indexFiles.isEmpty()) {
            CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
                for (final String field : indexFiles) {
                    final var indexPath = getIndexPath(field);
                    MAP_DB.closeIndex(indexPath);
                    CHRONICLE_UTILS.deleteFileIfExists(indexPath);
                }
                initIndex(indexFiles);
            });
            Logger.info("Refreshed indexes at [{}].", dataPath());
        }
    }

    default void refreshKeyMap() {
        CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
            final var keyMapPath = getKeyMapPath();
            MAP_DB.closeMap(keyMapPath);
            CHRONICLE_UTILS.deleteFileIfExists(keyMapPath);
            try (final var sharedKeyMap = MAP_DB.openMap(keyMapPath, entries())) {
                populateKeyMap(getDataFileState().fileNames(), sharedKeyMap.map);
            } catch (final Exception e) {
                // Delete corrupt keyMap so it rebuilds on next startup
                // (try-with-resources already closed the map)
                Logger.error("Failed to refresh KeyMap at [{}]. Deleting for rebuild.", keyMapPath);
                CHRONICLE_UTILS.deleteFileIfExists(keyMapPath);
                throw e;
            }
            Logger.info("Refreshed KeyMap at [{}]", dataPath());
        });
    }

    default List<String> availableIndexes() throws IOException {
        return CHRONICLE_UTILS.getFileList(dataPath() + INDEX_DIR);
    }

    /**
     * Initialize indexes at dao creation
     * 
     * @param fields
     */
    default void initDefaultIndexes() {
        // Skip index initialization in recovery mode to avoid deadlocks
        if (IN_RECOVERY) {
            return;
        }

        if (!getDataFileState().fileNames().isEmpty()) {
            CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), new SafeRunnable(() -> {
                final var availableIndexes = availableIndexes();
                final var indexFileNames = indexFileNames();
                if (availableIndexes.size() != indexFileNames.size()) {
                    // Find items in indexFileNames not in availableIndexes
                    final Set<String> missingIndexes = new HashSet<>(indexFileNames);
                    missingIndexes.removeAll(availableIndexes);

                    if (!missingIndexes.isEmpty()) {
                        initIndex(missingIndexes);
                        Logger.info("Initialized {} indexes at [{}]", missingIndexes, dataPath());
                    }
                }
            }, "Init Indexes - " + dataPath()));
        }
    }

    /**
     * In cases of data corruption, we can recover the db using this method
     * 
     * @param dataFileName the data- file name to recover
     * @throws IOException
     */
    default void recoverData(final String dataFileName) throws IOException {
        final var dataFileStr = dataPath() + DATA_DIR + dataFileName;
        final var dataFilePath = Path.of(dataFileStr);

        // 1. Make a backup before recovery
        final var backupPath = Path.of(dataPath() + BACKUP_DIR + CORRUPTED_FILE + dataFileName);
        Files.copy(dataFilePath, backupPath, StandardCopyOption.REPLACE_EXISTING);
        Logger.info("Backed up file to {}", backupPath);
        try (final var db = CHRONICLE_DB.recoverDb(name(), entries(), averageKey(), averageValue(),
                dataFileStr, bloatFactor())) {
            Logger.info("Recovered ChronicleMap [{}] with {} entries", dataFileName, db.size());
        }
    }

    /**
     * Looks up the database file name for a given primary key.
     * Calculates the 128-bit hash internally.
     *
     * @param key the primary key to look up
     * @return the database file name containing this key, or null if not found
     */
    private String getDbFile(final String key) {
        return getDbFileFromHash(CHRONICLE_UTILS.to128BitHash(key));
    }

    /**
     * Looks up the database file name using a pre-calculated key hash.
     * Avoids re-hashing when the hash is already available.
     *
     * @param keyHash the 16-byte hash of the primary key
     * @return the database file name containing this key, or null if not found
     */
    private String getDbFileFromHash(final byte[] keyHash) {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            final var keyMapValue = sharedKeyMap.map.get(keyHash);
            if (keyMapValue == null) {
                return null;
            }
            return keyMapValue.fileName();
        }
    }

    /**
     * Result of grouping keys by their database file location.
     * Contains a map of file names to iterables of primary keys, plus an
     * AutoCloseable
     * for the underlying KeyMap resource. Callers must use try-with-resources.
     *
     * @param fileGroups map of database file name to iterable of primary keys in
     *                   that file
     * @param closer     the AutoCloseable to release the KeyMap resource
     */
    public record GroupedKeys(Map<String, Iterable<String>> fileGroups, AutoCloseable closer) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            closer.close();
        }

        public static final AutoCloseable NOOP = () -> {
        };
    }

    /**
     * Groups keys by their database file using lazy, batched lookups.
     * Calculates key hashes internally. Thread-safe with demand-driven batch
     * processing.
     *
     * @param keys the primary keys to group
     * @return a {@link GroupedKeys} record (must be closed via try-with-resources)
     */
    private GroupedKeys getDbFiles(final Iterable<String> keys) {
        return getDbFilesFromHashes(keys, CHRONICLE_UTILS.preCalculateKeyHashes(keys));
    }

    /**
     * Groups keys by their database file using pre-calculated hashes.
     * Uses lazy, batched lookups with demand-driven processing. Thread-safe.
     * <p>
     * The returned {@link GroupedKeys} holds an open KeyMap resource and must be
     * closed via try-with-resources.
     *
     * @param keys       the primary keys to group
     * @param keyHashMap pre-calculated map of primary key to 16-byte hash
     * @return a {@link GroupedKeys} record (must be closed via try-with-resources)
     */
    private GroupedKeys getDbFilesFromHashes(final Iterable<String> keys, final Map<String, byte[]> keyHashMap) {
        // 1. Open the Key-to-File Index Map once.
        final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath());
        final Iterator<String> sourceIterator = keys.iterator();
        final var dbFiles = getDataFileState().fileNames();

        // 2. Setup dynamic, thread-safe buffers.
        final Map<String, ConcurrentLinkedQueue<String>> fileBuffers = new ConcurrentHashMap<>();

        final int batchSize = 25_000;
        final AtomicBoolean sourceExhausted = new AtomicBoolean(false);

        // CRITICAL: Lock for the entire refill process.
        final ReentrantLock refillLock = new ReentrantLock();

        final Runnable refillBuffers = () -> {
            refillLock.lock();
            try {
                if (sourceExhausted.get())
                    return;

                final List<String> keyBatch = new ArrayList<>(batchSize);

                // --- PHASE 1: KEY FETCH ---
                int count = 0;
                while (sourceIterator.hasNext() && count < batchSize) {
                    keyBatch.add(sourceIterator.next());
                    count++;
                }
                if (!sourceIterator.hasNext()) {
                    sourceExhausted.set(true);
                }

                // --- PHASE 2: KEY MAPPING ---
                keyBatch.parallelStream().forEach(key -> {
                    final var keyMapValue = sharedKeyMap.map.get(keyHashMap.get(key));
                    if (keyMapValue != null) {
                        fileBuffers.computeIfAbsent(keyMapValue.fileName(), k -> new ConcurrentLinkedQueue<>())
                                .add(key);
                    }
                });
            } finally {
                refillLock.unlock();
            }
        };

        // 3. Pre-fill buffers to see if we can optimize
        refillBuffers.run();

        // 4. Create Iterators
        final Map<String, Iterable<String>> fileGroups = new HashMap<>();

        // OPTIMIZATION: If source is exhausted after first refill, we know EXACTLY
        // which files are needed.
        // We can return ONLY the discovered files, avoiding iteration over empty files.
        // This makes update() extremely fast.
        final Set<String> filesToIterate = sourceExhausted.get() ? fileBuffers.keySet() : dbFiles;

        for (final String file : filesToIterate) {
            fileGroups.put(file, () -> new Iterator<>() {
                @Override
                public boolean hasNext() {
                    var buffer = fileBuffers.get(file);
                    // While buffer is missing or empty and we still have source data, try to refill
                    while ((buffer == null || buffer.isEmpty()) && !sourceExhausted.get()) {
                        refillBuffers.run();
                        buffer = fileBuffers.get(file); // Re-fetch buffer as it might have been created
                    }
                    return buffer != null && !buffer.isEmpty();
                }

                @Override
                public String next() {
                    if (!hasNext())
                        throw new NoSuchElementException();
                    // hasNext() guarantees buffer exists and is not empty
                    return fileBuffers.get(file).poll();
                }
            });
        }

        return new GroupedKeys(fileGroups, sharedKeyMap::close);
    }

    /**
     * Groups keys by their database file with a limit on total keys processed.
     * Calculates key hashes internally.
     *
     * @param keys  the primary keys to group
     * @param limit maximum number of keys to process
     * @return a {@link GroupedKeys} record (must be closed via try-with-resources)
     */
    private GroupedKeys getDbFiles(final Iterable<String> keys, final int limit) {
        return getDbFilesFromHashes(keys, limit, CHRONICLE_UTILS.preCalculateKeyHashes(keys));
    }

    /**
     * Groups keys by their database file using pre-calculated hashes, with a limit.
     * Uses lazy, batched lookups with demand-driven processing. Thread-safe.
     *
     * @param keys       the primary keys to group
     * @param limit      maximum number of keys to process
     * @param keyHashMap pre-calculated map of primary key to 16-byte hash
     * @return a {@link GroupedKeys} record (must be closed via try-with-resources)
     */
    private GroupedKeys getDbFilesFromHashes(final Iterable<String> keys, final int limit,
            final Map<String, byte[]> keyHashMap) {
        // 1. Open the Key-to-File Index Map once.
        final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath());
        final Iterator<String> sourceIterator = keys.iterator();
        final var dbFiles = getDataFileState().fileNames();

        // 2. Setup dynamic, thread-safe buffers.
        final Map<String, ConcurrentLinkedQueue<String>> fileBuffers = new ConcurrentHashMap<>();

        final int standardBatchSize = 25_000;
        final AtomicInteger totalFilled = new AtomicInteger();
        final AtomicInteger dynamicBatchSize = new AtomicInteger(Math.min(limit, standardBatchSize));
        final AtomicBoolean sourceExhausted = new AtomicBoolean(false);

        // CRITICAL: Lock for the entire refill process.
        final ReentrantLock refillLock = new ReentrantLock();

        final Runnable refillBuffers = () -> {
            refillLock.lock();
            try {
                if (sourceExhausted.get())
                    return;

                final var batchSize = dynamicBatchSize.get();
                final List<String> keyBatch = new ArrayList<>(batchSize);

                // --- PHASE 1: KEY FETCH ---
                int count = 0;
                while (sourceIterator.hasNext() && count < batchSize) {
                    keyBatch.add(sourceIterator.next());
                    count++;
                }

                if (!sourceIterator.hasNext()) {
                    sourceExhausted.set(true);
                }

                // --- PHASE 2: KEY MAPPING ---
                keyBatch.parallelStream().forEach(key -> {
                    final var keyMapValue = sharedKeyMap.map.get(keyHashMap.get(key));
                    if (keyMapValue != null) {
                        totalFilled.incrementAndGet();
                        fileBuffers.computeIfAbsent(keyMapValue.fileName(), k -> new ConcurrentLinkedQueue<>())
                                .add(key);
                    }
                });

                final int remainingToFetch = limit - totalFilled.get();
                if (remainingToFetch > 0) {
                    dynamicBatchSize.set(Math.min(remainingToFetch, standardBatchSize));
                } else {
                    sourceExhausted.set(true);
                }
            } finally {
                refillLock.unlock();
            }
        };

        // 3. Pre-fill buffers to see if we can optimize
        refillBuffers.run();

        // 4. Create Iterators
        final Map<String, Iterable<String>> fileGroups = new HashMap<>();

        // OPTIMIZATION: If source is exhausted after first refill, we know EXACTLY
        // which files are needed.
        // We can return ONLY the discovered files, avoiding iteration over empty files.
        // This makes update() extremely fast.
        final Set<String> filesToIterate = sourceExhausted.get() ? fileBuffers.keySet() : dbFiles;

        for (final String file : filesToIterate) {
            fileGroups.put(file, () -> new Iterator<>() {
                @Override
                public boolean hasNext() {
                    var buffer = fileBuffers.get(file);
                    // While buffer is missing or empty and we still have source data, try to refill
                    while ((buffer == null || buffer.isEmpty()) && !sourceExhausted.get()) {
                        refillBuffers.run();
                        buffer = fileBuffers.get(file); // Re-fetch buffer as it might have been created
                    }
                    return buffer != null && !buffer.isEmpty();
                }

                @Override
                public String next() {
                    if (!hasNext())
                        throw new NoSuchElementException();
                    // hasNext() guarantees buffer exists and is not empty
                    return fileBuffers.get(file).poll();
                }
            });
        }

        return new GroupedKeys(fileGroups, sharedKeyMap::close);
    }

    /**
     * Groups keys by their database file, excluding specified keys.
     * Filters and hashes in one pass for efficiency.
     *
     * @param keys         the primary keys to group
     * @param limit        maximum number of keys to process
     * @param excludedKeys keys to exclude from processing
     * @return a {@link GroupedKeys} record (must be closed via try-with-resources)
     */
    private GroupedKeys getDbFiles(final Iterable<String> keys, final int limit, final Set<String> excludedKeys) {
        return getDbFilesFromHashes(keys, limit, CHRONICLE_UTILS.preCalculateKeyHashes(keys, excludedKeys));
    }

    /**
     * Groups pre-calculated hashes by their file location using direct KeyMap
     * lookup.
     * No re-hashing required. Returns primary keys grouped by file.
     *
     * @param hashes the 16-byte hashes to look up
     * @return a {@link GroupedKeys} record (must be closed via try-with-resources)
     */
    private GroupedKeys getDbFilesFromHashes(final Iterable<byte[]> hashes) {
        // 1. Open the Key-to-File Index Map once.
        final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath());
        final Iterator<byte[]> sourceIterator = hashes.iterator();
        final var dbFiles = getDataFileState().fileNames();

        // 2. Setup dynamic, thread-safe buffers.
        final Map<String, ConcurrentLinkedQueue<String>> fileBuffers = new ConcurrentHashMap<>();

        final int batchSize = 25_000;
        final AtomicBoolean sourceExhausted = new AtomicBoolean(false);

        // CRITICAL: Lock for the entire refill process.
        final ReentrantLock refillLock = new ReentrantLock();

        final Runnable refillBuffers = () -> {
            refillLock.lock();
            try {
                if (sourceExhausted.get())
                    return;

                final List<byte[]> hashBatch = new ArrayList<>(batchSize);

                // --- PHASE 1: HASH FETCH ---
                int count = 0;
                while (sourceIterator.hasNext() && count < batchSize) {
                    hashBatch.add(sourceIterator.next());
                    count++;
                }
                if (!sourceIterator.hasNext()) {
                    sourceExhausted.set(true);
                }

                // --- PHASE 2: DIRECT KEY MAPPING (no re-hashing!) ---
                for (final var hash : hashBatch) {
                    final var keyMapValue = sharedKeyMap.map.get(hash);
                    if (keyMapValue != null) {
                        fileBuffers.computeIfAbsent(keyMapValue.fileName(), k -> new ConcurrentLinkedQueue<>())
                                .add(keyMapValue.primaryKey());
                    }
                }
            } finally {
                refillLock.unlock();
            }
        };

        // 3. Pre-fill buffers to see if we can optimize
        refillBuffers.run();

        // 4. Create Iterators
        final Map<String, Iterable<String>> fileGroups = new HashMap<>();

        // OPTIMIZATION: If source is exhausted after first refill, we know EXACTLY
        // which files are needed.
        final Set<String> filesToIterate = sourceExhausted.get() ? fileBuffers.keySet() : dbFiles;

        for (final String file : filesToIterate) {
            fileGroups.put(file, () -> new Iterator<>() {
                @Override
                public boolean hasNext() {
                    var buffer = fileBuffers.get(file);
                    while ((buffer == null || buffer.isEmpty()) && !sourceExhausted.get()) {
                        refillBuffers.run();
                        buffer = fileBuffers.get(file);
                    }
                    return buffer != null && !buffer.isEmpty();
                }

                @Override
                public String next() {
                    if (!hasNext())
                        throw new NoSuchElementException();
                    return fileBuffers.get(file).poll();
                }
            });
        }

        return new GroupedKeys(fileGroups, sharedKeyMap::close);
    }

    /**
     * Groups pre-calculated hashes by their file location with a limit.
     * No re-hashing required. Returns primary keys grouped by file.
     *
     * @param hashes the 16-byte hashes to look up
     * @param limit  maximum number of keys to process
     * @return a {@link GroupedKeys} record (must be closed via try-with-resources)
     */
    private GroupedKeys getDbFilesFromHashes(final Iterable<byte[]> hashes, final int limit) {
        final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath());
        final Iterator<byte[]> sourceIterator = hashes.iterator();
        final var dbFiles = getDataFileState().fileNames();

        final Map<String, ConcurrentLinkedQueue<String>> fileBuffers = new ConcurrentHashMap<>();

        final int standardBatchSize = 25_000;
        final AtomicInteger totalFilled = new AtomicInteger();
        final AtomicInteger dynamicBatchSize = new AtomicInteger(Math.min(limit, standardBatchSize));
        final AtomicBoolean sourceExhausted = new AtomicBoolean(false);

        final ReentrantLock refillLock = new ReentrantLock();

        final Runnable refillBuffers = () -> {
            refillLock.lock();
            try {
                if (sourceExhausted.get() || totalFilled.get() >= limit)
                    return;

                final List<byte[]> hashBatch = new ArrayList<>(dynamicBatchSize.get());

                int count = 0;
                while (sourceIterator.hasNext() && count < dynamicBatchSize.get()
                        && totalFilled.get() + count < limit) {
                    hashBatch.add(sourceIterator.next());
                    count++;
                }
                if (!sourceIterator.hasNext()) {
                    sourceExhausted.set(true);
                }

                for (final var hash : hashBatch) {
                    final var keyMapValue = sharedKeyMap.map.get(hash);
                    if (keyMapValue != null) {
                        fileBuffers.computeIfAbsent(keyMapValue.fileName(), k -> new ConcurrentLinkedQueue<>())
                                .add(keyMapValue.primaryKey());
                        totalFilled.incrementAndGet();
                    }
                }

                if (dynamicBatchSize.get() < standardBatchSize) {
                    dynamicBatchSize.set(Math.min(dynamicBatchSize.get() * 2, standardBatchSize));
                }
            } finally {
                refillLock.unlock();
            }
        };

        refillBuffers.run();

        final Map<String, Iterable<String>> fileGroups = new HashMap<>();
        final Set<String> filesToIterate = sourceExhausted.get() ? fileBuffers.keySet() : dbFiles;

        for (final String file : filesToIterate) {
            fileGroups.put(file, () -> new Iterator<>() {
                @Override
                public boolean hasNext() {
                    var buffer = fileBuffers.get(file);
                    while ((buffer == null || buffer.isEmpty()) && !sourceExhausted.get()
                            && totalFilled.get() < limit) {
                        refillBuffers.run();
                        buffer = fileBuffers.get(file);
                    }
                    return buffer != null && !buffer.isEmpty();
                }

                @Override
                public String next() {
                    if (!hasNext())
                        throw new NoSuchElementException();
                    return fileBuffers.get(file).poll();
                }
            });
        }

        return new GroupedKeys(fileGroups, sharedKeyMap::close);
    }

    /**
     * Groups pre-calculated hashes by their file location with a limit, excluding
     * specified keys.
     * Exclusion is applied after resolving hash to primary key. No re-hashing
     * required.
     *
     * @param hashes       the 16-byte hashes to look up
     * @param limit        maximum number of keys to process
     * @param excludedKeys primary keys to exclude from results
     * @return a {@link GroupedKeys} record (must be closed via try-with-resources)
     */
    private GroupedKeys getDbFilesFromHashes(final Iterable<byte[]> hashes, final int limit,
            final Set<String> excludedKeys) {
        final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath());
        final Iterator<byte[]> sourceIterator = hashes.iterator();
        final var dbFiles = getDataFileState().fileNames();

        final Map<String, ConcurrentLinkedQueue<String>> fileBuffers = new ConcurrentHashMap<>();

        final int standardBatchSize = 25_000;
        final AtomicInteger totalFilled = new AtomicInteger();
        final AtomicInteger dynamicBatchSize = new AtomicInteger(Math.min(limit, standardBatchSize));
        final AtomicBoolean sourceExhausted = new AtomicBoolean(false);

        final ReentrantLock refillLock = new ReentrantLock();

        final Runnable refillBuffers = () -> {
            refillLock.lock();
            try {
                if (sourceExhausted.get() || totalFilled.get() >= limit)
                    return;

                final List<byte[]> hashBatch = new ArrayList<>(dynamicBatchSize.get());

                int count = 0;
                while (sourceIterator.hasNext() && count < dynamicBatchSize.get()
                        && totalFilled.get() + count < limit) {
                    hashBatch.add(sourceIterator.next());
                    count++;
                }
                if (!sourceIterator.hasNext()) {
                    sourceExhausted.set(true);
                }

                for (final var hash : hashBatch) {
                    final var keyMapValue = sharedKeyMap.map.get(hash);
                    if (keyMapValue != null) {
                        final var primaryKey = keyMapValue.primaryKey();
                        // Skip excluded keys
                        if (excludedKeys != null && excludedKeys.contains(primaryKey)) {
                            continue;
                        }
                        fileBuffers.computeIfAbsent(keyMapValue.fileName(), k -> new ConcurrentLinkedQueue<>())
                                .add(primaryKey);
                        totalFilled.incrementAndGet();
                    }
                }

                if (dynamicBatchSize.get() < standardBatchSize) {
                    dynamicBatchSize.set(Math.min(dynamicBatchSize.get() * 2, standardBatchSize));
                }
            } finally {
                refillLock.unlock();
            }
        };

        refillBuffers.run();

        final Map<String, Iterable<String>> fileGroups = new HashMap<>();
        final Set<String> filesToIterate = sourceExhausted.get() ? fileBuffers.keySet() : dbFiles;

        for (final String file : filesToIterate) {
            fileGroups.put(file, () -> new Iterator<>() {
                @Override
                public boolean hasNext() {
                    var buffer = fileBuffers.get(file);
                    while ((buffer == null || buffer.isEmpty()) && !sourceExhausted.get()
                            && totalFilled.get() < limit) {
                        refillBuffers.run();
                        buffer = fileBuffers.get(file);
                    }
                    return buffer != null && !buffer.isEmpty();
                }

                @Override
                public String next() {
                    if (!hasNext())
                        throw new NoSuchElementException();
                    return fileBuffers.get(file).poll();
                }
            });
        }

        return new GroupedKeys(fileGroups, sharedKeyMap::close);
    }

    /**
     * Removes a single key from the KeyMap using its pre-calculated hash.
     *
     * @param key     the primary key (for logging only)
     * @param keyHash the 16-byte hash of the key
     */
    private void removeFromKeyMap(final String key, final byte[] keyHash) {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            sharedKeyMap.map.remove(keyHash);
        }
        Logger.debug("Deleted [{}] from KeyMap at [{}].", key, dataPath());
    }

    /**
     * Removes multiple keys from the KeyMap using pre-calculated hashes.
     *
     * @param keyHashMap map of primary key to 16-byte hash
     */
    private void removeAllFromKeyMap(final Map<String, byte[]> keyHashMap) {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            keyHashMap.values().parallelStream().forEach(sharedKeyMap.map::remove);
        }
        Logger.debug("Deleted [{}] keys from KeyMap at [{}].", keyHashMap.size(), dataPath());
    }

    /**
     * Adds a single key to the KeyMap using its pre-calculated hash.
     *
     * @param key     the primary key
     * @param file    the database file name containing this key
     * @param keyHash the 16-byte hash of the key
     */
    private void addToKeyMap(final String key, final String file, final byte[] keyHash) {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            sharedKeyMap.map.put(keyHash, new KeyMapValue(key, file));
        }
        Logger.debug("Inserted [{}] to KeyMap at [{}].", key, dataPath());
    }

    /**
     * Adds multiple keys to the KeyMap using pre-calculated hashes.
     * Uses parallel processing for efficiency.
     *
     * @param keyToFile  map of primary key to database file name
     * @param keyHashMap map of primary key to 16-byte hash
     */
    private void addAllToKeyMap(final Map<String, String> keyToFile, final Map<String, byte[]> keyHashMap) {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            keyToFile.entrySet().parallelStream().forEach(e -> sharedKeyMap.map.put(
                    keyHashMap.get(e.getKey()),
                    new KeyMapValue(e.getKey(), e.getValue())));
        }
        Logger.debug("Inserted [{}] keys to KeyMap at [{}].", keyToFile.size(), dataPath());
    }

    /**
     * Convert a set of String keys to a set of byte[] hashes for exclusion
     * filtering.
     *
     * @param excludedKeys set of String keys to exclude
     * @return set of byte[] hashes
     */
    private Set<byte[]> preCalculateExcludedHashes(final Set<String> excludedKeys) {
        if (excludedKeys == null || excludedKeys.isEmpty()) {
            return Collections.emptySet();
        }
        return excludedKeys.parallelStream()
                .map(CHRONICLE_UTILS::to128BitHash)
                .collect(Collectors.toSet());
    }

    /**
     * Resize a specific file to a diff size
     *
     * @param fileName the data- file name
     * @param newSize  the new size to set
     */
    default void resizeDb(final String fileName, final long newSize) {
        CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), new SafeRunnable(() -> {
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
        }, "Resize DB - " + dataPath() + fileName));
    }

    /**
     * Reclaims disk space by consolidating multiple data files into a single file.
     * <p>
     * This operation backs up all existing data files, deletes them, and re-inserts
     * all records from the backup. This eliminates fragmentation and removes
     * deleted
     * record space.
     * </p>
     * <p>
     * <b>Thread Safety:</b> This method acquires a write lock on the entire
     * database.
     * Note that {@code insert()} is called within the locked section, which will
     * re-acquire the same lock (reentrant behavior). This is safe because
     * ReentrantLock
     * allows the same thread to acquire a lock it already holds.
     * </p>
     *
     */
    default void vacuum() {
        // backup all files then read from these files and insert afresh
        if (getDataFileState().fileNames().size() <= 1) {
            Logger.info("Vacuuming not required at [{}]", dataPath());
            return;
        }

        Logger.info("Vacuuming database at [{}]", dataPath());
        CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), new SafeRunnable(() -> {
            backup();
            deleteDataFiles();
            deleteIndexes();
            CHRONICLE_UTILS.deleteFileIfExists(getKeyMapPath());
            DATA_FILE_CACHE.remove(dataPath());

            // Re-insert all records from backup files
            // Note: insert() will re-acquire the same lock (reentrant lock behavior)
            for (final String file : CHRONICLE_UTILS.getFileList(dataPath() + BACKUP_DIR)) {
                if (file.startsWith("data")) {
                    try (final var shared = openDb(BACKUP_DIR, file)) {
                        // Copy to HashMap for thread-safe parallel processing in updateIndex()
                        insert(new HashMap<>(shared.map));
                    }
                }
            }
        }, "Vacuum DB - " + dataPath()));
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
                    result.put(key, entry.value().getUsing(null));
                    return result.size() < limit;
                });
            }
        }
        Logger.debug("Fetched [{}] entries at [{}].", result.size(), dataPath());
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
        Logger.debug("Fetched [{}] CSV entries at [{}].", rowQueue.size(), dataPath());

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
        Logger.debug("Fetched [{}] entries at [{}].", result.size(), dataPath());
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
        Logger.debug("Fetched subset CSV [{}] entries at [{}].", rowQueue.size(), dataPath());
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
        final var result = ConcurrentHashMap.<String>newKeySet();
        CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
            try (final var shared = openDb(file)) {
                shared.map.forEachEntry(entry -> result.add(entry.key().get()));
            }
        });
        Logger.debug("Fetched [{}] keys at [{}].", result.size(), dataPath());
        return result;
    }

    default List<String> fetchKeysList() {
        final var result = new ConcurrentLinkedQueue<String>();
        CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
            try (final var shared = openDb(file)) {
                shared.map.forEachEntry(entry -> result.add(entry.key().get()));
            }
        });
        Logger.debug("Fetched [{}] keys list at [{}].", result.size(), dataPath());
        return new ArrayList<>(result);
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

        Logger.debug("Querying key [{}] at [{}].", key, dataPath());
        final var file = getDbFile(key);
        if (file == null) {
            return null;
        }

        try (final var shared = openDb(file)) {
            return shared.map.get(key);
        }
    }

    private void processKeysByFile(final Iterable<String> keys, final BiConsumer<String, V> valueConsumer) {
        try (final var grouped = getDbFiles(keys)) {
            CHRONICLE_UTILS.processInParallel(grouped.fileGroups().entrySet(), entry -> {
                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), Integer.MAX_VALUE, key -> {
                        final var value = shared.map.get(key);
                        if (value != null) {
                            valueConsumer.accept(key, value);
                            return true;
                        }
                        return false;
                    });
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void processKeysByFileUsing(final Iterable<String> keys, final BiConsumer<String, V> valueConsumer) {
        try (final var grouped = getDbFiles(keys)) {
            CHRONICLE_UTILS.processInParallel(grouped.fileGroups().entrySet(), entry -> {
                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), Integer.MAX_VALUE, key -> {
                        final var value = shared.map.getUsing(key, using());
                        if (value != null) {
                            valueConsumer.accept(key, value);
                            return true;
                        }
                        return false;
                    });
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void processKeysByFile(final Iterable<String> keys, final int limit,
            final BiConsumer<String, V> valueConsumer) {
        try (final var grouped = getDbFiles(keys, limit)) {
            final var counter = new AtomicInteger(0);
            CHRONICLE_UTILS.processInParallel(grouped.fileGroups().entrySet(), entry -> {
                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), limit, counter, key -> {
                        final var value = shared.map.get(key);
                        if (value != null) {
                            valueConsumer.accept(key, value);
                            return true;
                        }
                        return false;
                    });
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void processKeysByFileUsing(final Iterable<String> keys, final int limit,
            final BiConsumer<String, V> valueConsumer) {
        try (final var grouped = getDbFiles(keys, limit)) {
            final var counter = new AtomicInteger(0);
            CHRONICLE_UTILS.processInParallel(grouped.fileGroups().entrySet(), entry -> {
                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), limit, counter, key -> {
                        final var value = shared.map.getUsing(key, using());
                        if (value != null) {
                            valueConsumer.accept(key, value);
                            return true;
                        }
                        return false;
                    });
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void processKeysByFile(final Iterable<String> keys, final int limit,
            final Set<String> excludedKeys, final BiConsumer<String, V> valueConsumer) {
        try (final var grouped = getDbFiles(keys, limit, excludedKeys)) {
            final var counter = new AtomicInteger(0);
            CHRONICLE_UTILS.processInParallel(grouped.fileGroups().entrySet(), entry -> {
                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), limit, counter, key -> {
                        final var value = shared.map.get(key);
                        if (value != null) {
                            valueConsumer.accept(key, value);
                            return true;
                        }
                        return false;
                    });
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Process keys from byte[] hashes with limit - uses direct KeyMap lookup
     * without re-hashing.
     */
    private void processKeysByFileFromHashes(final Iterable<byte[]> hashes, final int limit,
            final BiConsumer<String, V> valueConsumer) {
        try (final var grouped = getDbFilesFromHashes(hashes, limit)) {
            final var counter = new AtomicInteger(0);
            CHRONICLE_UTILS.processInParallel(grouped.fileGroups().entrySet(), entry -> {
                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), limit, counter, key -> {
                        final var value = shared.map.get(key);
                        if (value != null) {
                            valueConsumer.accept(key, value);
                            return true;
                        }
                        return false;
                    });
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Process keys from byte[] hashes with limit and excluded keys - uses direct
     * KeyMap lookup.
     */
    private void processKeysByFileFromHashes(final Iterable<byte[]> hashes, final int limit,
            final Set<String> excludedKeys, final BiConsumer<String, V> valueConsumer) {
        try (final var grouped = getDbFilesFromHashes(hashes, limit, excludedKeys)) {
            final var counter = new AtomicInteger(0);
            CHRONICLE_UTILS.processInParallel(grouped.fileGroups().entrySet(), entry -> {
                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), limit, counter, key -> {
                        final var value = shared.map.get(key);
                        if (value != null) {
                            valueConsumer.accept(key, value);
                            return true;
                        }
                        return false;
                    });
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void processKeysByFileUsing(final Iterable<String> keys, final int limit,
            final Set<String> excludedKeys, final BiConsumer<String, V> valueConsumer) {
        try (final var grouped = getDbFiles(keys, limit, excludedKeys)) {
            final var counter = new AtomicInteger(0);
            CHRONICLE_UTILS.processInParallel(grouped.fileGroups().entrySet(), entry -> {
                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), limit, counter, key -> {
                        final var value = shared.map.getUsing(key, using());
                        if (value != null) {
                            valueConsumer.accept(key, value);
                            return true;
                        }
                        return false;
                    });
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void processKeys(final Iterable<String> keys, final BiConsumer<String, V> valueConsumer) {
        try (final var shared = openDb()) {
            CHRONICLE_UTILS.parallelIterable(keys, Integer.MAX_VALUE, key -> {
                final var value = shared.map.get(key);
                if (value != null) {
                    valueConsumer.accept(key, value);
                    return true;
                }
                return false;
            });
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void processKeysUsing(final Iterable<String> keys, final BiConsumer<String, V> valueConsumer) {
        try (final var shared = openDb()) {
            CHRONICLE_UTILS.parallelIterable(keys, Integer.MAX_VALUE, key -> {
                final var value = shared.map.getUsing(key, using());
                if (value != null) {
                    valueConsumer.accept(key, value);
                    return true;
                }
                return false;
            });
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void processKeys(final Iterable<String> keys, final int limit, final BiConsumer<String, V> valueConsumer) {
        try (final var shared = openDb()) {
            CHRONICLE_UTILS.parallelIterable(keys, limit, key -> {
                final var value = shared.map.get(key);
                if (value != null) {
                    valueConsumer.accept(key, value);
                    return true;
                }
                return false;
            });
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void processKeysUsing(final Iterable<String> keys, final int limit,
            final BiConsumer<String, V> valueConsumer) {
        try (final var shared = openDb()) {
            CHRONICLE_UTILS.parallelIterable(keys, limit, key -> {
                final var value = shared.map.getUsing(key, using());
                if (value != null) {
                    valueConsumer.accept(key, value);
                    return true;
                }
                return false;
            });
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void processKeys(final Iterable<String> keys, final int limit, final Set<String> excludedKeys,
            final BiConsumer<String, V> valueConsumer) {
        try (final var shared = openDb()) {
            CHRONICLE_UTILS.parallelIterable(keys, limit, key -> {
                if (!excludedKeys.contains(key)) {
                    final var value = shared.map.get(key);
                    if (value != null) {
                        valueConsumer.accept(key, value);
                        return true;
                    }
                }
                return false;
            });
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void processKeysUsing(final Iterable<String> keys, final int limit, final Set<String> excludedKeys,
            final BiConsumer<String, V> valueConsumer) {
        try (final var shared = openDb()) {
            CHRONICLE_UTILS.parallelIterable(keys, limit, key -> {
                if (!excludedKeys.contains(key)) {
                    final var value = shared.map.getUsing(key, using());
                    if (value != null) {
                        valueConsumer.accept(key, value);
                        return true;
                    }
                }
                return false;
            });
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
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

        Logger.debug("Querying multiple keys at [{}].", dataPath());
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

        Logger.debug("Querying multiple keys for CSV at [{}].", dataPath());
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final AtomicReference<String[]> headers = new AtomicReference<>(null);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        if (getDataFileState().fileNames().size() <= 1) {
            processKeysUsing(keys, (key, value) -> {
                headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
            });

            return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
        }

        processKeysByFileUsing(keys, (key, value) -> {
            headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(classData, value));
            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
        });

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    default Map<String, Map<String, Object>> getSubset(final Iterable<String> keys, final String[] fields) {
        if (keys == null || !keys.iterator().hasNext()) {
            return Collections.emptyMap();
        }

        Logger.debug("Querying subset multiple keys at [{}].", dataPath());
        final var map = new ConcurrentHashMap<String, Map<String, Object>>(10_000);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        if (getDataFileState().fileNames().size() <= 1) {
            processKeysUsing(keys,
                    (key, value) -> map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value)));

            return map;
        }

        processKeysByFileUsing(keys,
                (key, value) -> map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value)));

        return map;
    }

    default CsvObject getSubsetCsv(final Iterable<String> keys, final String[] fields) {
        if (keys == null || !keys.iterator().hasNext()) {
            return CsvObject.empty();
        }

        Logger.debug("Querying subset CSV multiple keys at [{}].", dataPath());
        final String[] headers = CHRONICLE_UTILS.getCsvHeaders(fields);
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        if (getDataFileState().fileNames().size() <= 1) {
            processKeysUsing(keys, (key, value) -> rowQueue
                    .add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value)));

            return new CsvObject(headers, new ArrayList<>(rowQueue));
        }

        processKeysByFileUsing(keys,
                (key, value) -> rowQueue.add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value)));

        return new CsvObject(headers, new ArrayList<>(rowQueue));
    }

    default Map<String, V> get(final Iterable<String> keys, final int limit) {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return Collections.emptyMap();
        }

        Logger.debug("Querying multiple keys at [{}] with limit [{}].", dataPath(), limit);
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

        Logger.debug("Querying multiple keys at [{}] with limit [{}].", dataPath(), limit);
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());
        final AtomicReference<String[]> headers = new AtomicReference<>(null);

        if (getDataFileState().fileNames().size() <= 1) {
            processKeysUsing(keys, limit, (key, value) -> {
                headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
            });

            return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
        }

        processKeysByFileUsing(keys, limit, (key, value) -> {
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

        Logger.debug("Querying subset multiple keys at [{}] with limit [{}].", dataPath(), limit);
        final var map = new ConcurrentHashMap<String, Map<String, Object>>(limit);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        if (getDataFileState().fileNames().size() <= 1) {
            processKeysUsing(keys, limit,
                    (key, value) -> map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value)));

            return map;
        }

        processKeysByFileUsing(keys, limit,
                (key, value) -> map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value)));

        return map;
    }

    default CsvObject getSubsetCsv(final Iterable<String> keys, final String[] fields, final int limit) {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.debug("Querying subset CSV multiple keys at [{}] with limit [{}].", dataPath(), limit);
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var headers = CHRONICLE_UTILS.getCsvHeaders(fields);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        if (getDataFileState().fileNames().size() <= 1) {
            processKeysUsing(keys, limit, (key, value) -> rowQueue
                    .add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value)));

            return new CsvObject(headers, new ArrayList<>(rowQueue));
        }

        processKeysByFileUsing(keys, limit,
                (key, value) -> rowQueue.add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value)));

        return new CsvObject(headers, new ArrayList<>(rowQueue));
    }

    // ========== HASH-BASED OVERLOADS (no re-hashing, uses direct KeyMap lookup)
    // ==========

    /**
     * Get values by byte[] hashes - uses direct KeyMap lookup without re-hashing.
     */
    default Map<String, V> getFromHashes(final Iterable<byte[]> hashes, final int limit) {
        if (hashes == null || !hashes.iterator().hasNext() || limit <= 0) {
            return Collections.emptyMap();
        }

        Logger.debug("Querying by hashes at [{}] with limit [{}].", dataPath(), limit);
        final var map = new ConcurrentHashMap<String, V>(limit);

        processKeysByFileFromHashes(hashes, limit, (key, value) -> map.put(key, value));

        return map;
    }

    /**
     * Get subset values by byte[] hashes - uses direct KeyMap lookup without
     * re-hashing.
     */
    default Map<String, Map<String, Object>> getSubsetFromHashes(final Iterable<byte[]> hashes,
            final String[] fields, final int limit) {
        if (hashes == null || !hashes.iterator().hasNext() || limit <= 0) {
            return Collections.emptyMap();
        }

        Logger.debug("Querying subset by hashes at [{}] with limit [{}].", dataPath(), limit);
        final var map = new ConcurrentHashMap<String, Map<String, Object>>(limit);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        processKeysByFileFromHashes(hashes, limit,
                (key, value) -> map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value)));

        return map;
    }

    /**
     * Get CSV by byte[] hashes - uses direct KeyMap lookup without re-hashing.
     */
    default CsvObject getCsvFromHashes(final Iterable<byte[]> hashes, final int limit) {
        if (hashes == null || !hashes.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.debug("Querying CSV by hashes at [{}] with limit [{}].", dataPath(), limit);
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());
        final AtomicReference<String[]> headers = new AtomicReference<>(null);

        processKeysByFileFromHashes(hashes, limit, (key, value) -> {
            headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(classData, value));
            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
        });

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    /**
     * Get subset CSV by byte[] hashes - uses direct KeyMap lookup without
     * re-hashing.
     */
    default CsvObject getSubsetCsvFromHashes(final Iterable<byte[]> hashes, final String[] fields, final int limit) {
        if (hashes == null || !hashes.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.debug("Querying subset CSV by hashes at [{}] with limit [{}].", dataPath(), limit);
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var headers = CHRONICLE_UTILS.getCsvHeaders(fields);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        processKeysByFileFromHashes(hashes, limit,
                (key, value) -> rowQueue.add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value)));

        return new CsvObject(headers, new ArrayList<>(rowQueue));
    }

    /**
     * Convert byte[] hashes to String keys using KeyMap lookup.
     * Use this when you need the actual key strings.
     */
    default Set<String> toSetOfKeysFromHashes(final Iterable<byte[]> hashes) {
        if (hashes == null || !hashes.iterator().hasNext()) {
            return Collections.emptySet();
        }

        // Use a high initial capacity to avoid resizing overhead for 4M records
        final var result = ConcurrentHashMap.<String>newKeySet(10_000);
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            CHRONICLE_UTILS.parallelIterable(hashes, Integer.MAX_VALUE, hash -> {
                final var keyMapValue = sharedKeyMap.map.get(hash);
                if (keyMapValue != null) {
                    result.add(keyMapValue.primaryKey());
                }
            });
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            Logger.error("InterruptedException while resolving keys to set from hashes", e);
        }

        return result;
    }

    /**
     * Convert byte[] hashes to String keys list using KeyMap lookup.
     * Use this when you need the actual key strings.
     */
    default List<String> toListOfKeysFromHashes(final Iterable<byte[]> hashes) {
        if (hashes == null || !hashes.iterator().hasNext()) {
            return Collections.emptyList();
        }

        // Lock-free collection
        final var result = new ConcurrentLinkedQueue<String>();
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            CHRONICLE_UTILS.parallelIterable(hashes, Integer.MAX_VALUE, hash -> {
                final var keyMapValue = sharedKeyMap.map.get(hash);
                if (keyMapValue != null) {
                    result.add(keyMapValue.primaryKey());
                }
            });
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            Logger.error("InterruptedException while resolving keys to list from hashes", e);
        }

        // Convert to final list
        return new ArrayList<>(result);
    }

    // ========== HASH-BASED OVERLOADS WITH EXCLUDED KEYS ==========

    /**
     * Get values by byte[] hashes with excluded keys - uses direct KeyMap lookup
     * without re-hashing.
     */
    default Map<String, V> getFromHashes(final Iterable<byte[]> hashes, final Set<String> excludedKeys,
            final int limit) {
        if (hashes == null || !hashes.iterator().hasNext() || limit <= 0) {
            return Collections.emptyMap();
        }

        Logger.debug("Querying by hashes at [{}] with limit [{}] and excluded keys.", dataPath(), limit);
        final var map = new ConcurrentHashMap<String, V>(limit);
        processKeysByFileFromHashes(hashes, limit, excludedKeys, (key, value) -> map.put(key, value));
        return map;
    }

    /**
     * Get CSV by byte[] hashes with excluded keys - uses direct KeyMap lookup
     * without re-hashing.
     */
    default CsvObject getCsvFromHashes(final Iterable<byte[]> hashes, final Set<String> excludedKeys,
            final int limit) {
        if (hashes == null || !hashes.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.debug("Querying CSV by hashes at [{}] with limit [{}] and excluded keys.", dataPath(), limit);
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());
        final AtomicReference<String[]> headers = new AtomicReference<>(null);

        processKeysByFileFromHashes(hashes, limit, excludedKeys, (key, value) -> {
            headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(classData, value));
            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
        });

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    /**
     * Get subset by byte[] hashes with excluded keys - uses direct KeyMap lookup
     * without re-hashing.
     */
    default Map<String, Map<String, Object>> getSubsetFromHashes(final Iterable<byte[]> hashes,
            final Set<String> excludedKeys, final String[] fields, final int limit) {
        if (hashes == null || !hashes.iterator().hasNext() || limit <= 0) {
            return Collections.emptyMap();
        }

        Logger.debug("Querying subset by hashes at [{}] with limit [{}] and excluded keys.", dataPath(), limit);
        final var map = new ConcurrentHashMap<String, Map<String, Object>>(limit);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());
        processKeysByFileFromHashes(hashes, limit, excludedKeys,
                (key, value) -> map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value)));
        return map;
    }

    /**
     * Get subset CSV by byte[] hashes with excluded keys - uses direct KeyMap
     * lookup without re-hashing.
     */
    default CsvObject getSubsetCsvFromHashes(final Iterable<byte[]> hashes, final Set<String> excludedKeys,
            final String[] fields, final int limit) {
        if (hashes == null || !hashes.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.debug("Querying subset CSV by hashes at [{}] with limit [{}] and excluded keys.", dataPath(), limit);
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var headers = CHRONICLE_UTILS.getCsvHeaders(fields);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        processKeysByFileFromHashes(hashes, limit, excludedKeys,
                (key, value) -> rowQueue.add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value)));

        return new CsvObject(headers, new ArrayList<>(rowQueue));
    }

    // ========== END HASH-BASED OVERLOADS ==========

    default Map<String, V> get(final Iterable<String> keys, final Set<String> excludedKeys, final int limit) {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return Collections.emptyMap();
        }

        Logger.debug("Querying multiple keys at [{}] with limit [{}] and excluded keys.", dataPath(), limit);
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

        Logger.debug("Querying multiple keys at [{}] with limit [{}] and excluded keys.", dataPath(), limit);
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());
        final AtomicReference<String[]> headers = new AtomicReference<>(null);

        if (getDataFileState().fileNames().size() <= 1) {
            processKeysUsing(keys, limit, excludedKeys, (key, value) -> {
                headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
            });

            return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
        }

        processKeysByFileUsing(keys, limit, excludedKeys, (key, value) -> {
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

        Logger.debug("Querying subset multiple keys at [{}] with limit [{}] and excluded keys.", dataPath(), limit);
        final var map = new ConcurrentHashMap<String, Map<String, Object>>(limit);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        if (getDataFileState().fileNames().size() <= 1) {
            processKeysUsing(keys, limit, excludedKeys,
                    (key, value) -> map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value)));

            return map;
        }

        processKeysByFileUsing(keys, limit, excludedKeys,
                (key, value) -> map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value)));

        return map;
    }

    default CsvObject getSubsetCsv(final Iterable<String> keys, final Set<String> excludedKeys, final String[] fields,
            final int limit) {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.debug("Querying subset CSV multiple keys at [{}] with limit [{}] and excluded keys.", dataPath(), limit);
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var headers = CHRONICLE_UTILS.getCsvHeaders(fields);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        if (getDataFileState().fileNames().size() <= 1) {
            processKeysUsing(keys, limit, excludedKeys, (key, value) -> rowQueue
                    .add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value)));

            return new CsvObject(headers, new ArrayList<>(rowQueue));
        }

        processKeysByFileUsing(keys, limit, excludedKeys,
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

        // Pre-calculate hash ONCE before all operations
        final byte[] keyHash = CHRONICLE_UTILS.to128BitHash(key);
        final var keyHashMap = Map.of(key, keyHash);

        // STEP 0: Slow lookup outside the lock using pre-calculated hash
        final var file = getDbFileFromHash(keyHash);
        if (file == null)
            return false;

        // STEP 1: The Critical Section (Short Lock)
        return CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
            V deletedValue = null;
            try (final var shared = openDb(file)) {
                deletedValue = shared.map.remove(key);
            }

            if (deletedValue != null) {
                // STEP 2: The Cleanup
                removeFromKeyMap(key, keyHash);
                CHRONICLE_UTILS.removeFromIndex(name(), dataPath(), indexFileNames(), Map.of(key, deletedValue),
                        averageValue().getClass(), keyHashMap);

                Logger.info("Deleted using key [{}] at [{}].", key, dataPath());
                return true;
            }

            return false;
        });
    }

    private void removeFromIndex(final Map<String, V> deletedMap, final Map<String, byte[]> keyHashMap) {
        CHRONICLE_UTILS.removeFromIndex(name(), dataPath(), indexFileNames(), deletedMap, averageValue().getClass(),
                keyHashMap);
        Logger.info("Deleted [{}] records at [{}].", deletedMap.size(), dataPath());
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

        // Pre-calculate hashes ONCE before all operations
        final var keyHashMap = CHRONICLE_UTILS.preCalculateKeyHashes(keys);

        final var deletedMap = new ConcurrentHashMap<String, V>(1000);
        // STEP 0: Group keys by file using pre-calculated hashes
        final var grouped = getDbFilesFromHashes(keys, keyHashMap);
        if (grouped.fileGroups().isEmpty()) {
            return false;
        }

        try {
            // STEP 1: The Critical Section (Short Lock)
            return CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
                CHRONICLE_UTILS.processInParallel(grouped.fileGroups().entrySet(), entry -> {
                    final var file = entry.getKey();
                    try (final var shared = openDb(file)) {
                        CHRONICLE_UTILS.parallelIterable(entry.getValue(), Integer.MAX_VALUE, key -> {
                            final var deleted = shared.map.remove(key);
                            if (deleted != null) {
                                deletedMap.put(key, deleted);
                                return true;
                            }
                            return false;
                        });
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                });

                if (deletedMap.isEmpty()) {
                    return false;
                }

                // Filter keyHashMap to only include deleted keys
                final var deletedKeyHashMap = new HashMap<String, byte[]>();
                for (final var key : deletedMap.keySet()) {
                    deletedKeyHashMap.put(key, keyHashMap.get(key));
                }

                // STEP 2: Parallel Metadata Cleanup (Outside Lock)
                // Since we are doing many keys, parallelizing here is great.
                final var tasks = new ArrayList<Runnable>();
                tasks.add(() -> removeAllFromKeyMap(deletedKeyHashMap));
                tasks.add(() -> removeFromIndex(deletedMap, deletedKeyHashMap));
                CHRONICLE_UTILS.processInParallel(tasks);
                return true;
            });
        } finally {
            try {
                grouped.close();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private SharedChronicleMap<String, V> checkAndRotate(final SharedChronicleMap<String, V> shared) {
        if (shared.map.size() >= entries()) {
            // keymap is always being updated, this method just creates a new file
            shared.close();
            final var dataFileState = getDataFileState();
            final String newFile = "data-" + (dataFileState.fileNames().size() + 1);
            dataFileState.fileNames().add(newFile);
            Logger.info("Rotated file [{}] at [{}].", dataFileState.currentFile(), dataPath());
            final var updateFileState = dataFileState.withCurrentFile(newFile);
            DATA_FILE_CACHE.put(dataPath(), updateFileState);
            return openDb(newFile); // open new db
        }
        return shared;
    }

    private SharedChronicleMap<String, V> checkAndRotate(final SharedChronicleMap<String, V> shared,
            final Map<String, String> keyMapUpdate, final Map<String, byte[]> keyHashMap) {
        if (shared.map.size() >= entries()) {
            final var dataFileState = getDataFileState();
            final var currentSize = dataFileState.fileNames().size();
            final String newFile = "data-" + (currentSize + 1);
            // add the new keys to map
            addAllToKeyMap(keyMapUpdate, keyHashMap);
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

        // Pre-calculate hash ONCE before all operations
        final byte[] keyHash = CHRONICLE_UTILS.to128BitHash(key);
        final var keyHashMap = Map.of(key, keyHash);

        // 1. Prepare Index (OUTSIDE LOCK)
        final var preparedIndex = CHRONICLE_UTILS.prepareIndexAdditions(indexFileNames, Map.of(key, value),
                averageValue().getClass(), indexExclusions(), keyHashMap);

        // 2. PRE-LOCK: Look in MapDB using pre-calculated hash
        final var initialFile = getDbFileFromHash(keyHash);

        // 3. Lock short time
        return CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
            String file;
            String capturedFile = null;
            // If key wasnt found and another thread beat current one in the insert
            if (initialFile == null) {
                file = getDbFileFromHash(keyHash);
                if (file == null) {
                    file = getDataFileState().currentFile();
                    capturedFile = file;
                }
            } else {
                file = initialFile;
            }

            var shared = openDb(file); // no try with resource here for rotation
            // only rotate if current file is full and insert mode
            try {
                if (getDataFileState().currentFile().equals(file) && !shared.map.containsKey(key)) {
                    shared = checkAndRotate(shared);
                    // if rotated, then the file = latest file
                    capturedFile = getDataFileState().currentFile();
                }

                final var prevValue = shared.map.put(key, value);
                if (capturedFile != null) {
                    addToKeyMap(key, capturedFile, keyHash);
                }

                if (prevValue != null) {
                    CHRONICLE_UTILS.applyIndexUpdates(dataPath(), indexFileNames, preparedIndex, Map.of(key, prevValue),
                            averageValue().getClass(), indexExclusions(), keyHashMap);
                    Logger.info("Updated using key [{}] at [{}].", key, dataPath());
                    return PutStatus.UPDATED;
                } else {
                    CHRONICLE_UTILS.applyIndexAdditions(dataPath(), preparedIndex);
                    Logger.info("Inserted using key [{}] at [{}].", key, dataPath());
                    return PutStatus.INSERTED;
                }
            } finally {
                shared.close();
            }
        });
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
        if (key == null) {
            return PutStatus.FAILED;
        }

        // Pre-calculate hash ONCE before all operations
        final byte[] keyHash = CHRONICLE_UTILS.to128BitHash(key);
        final var keyHashMap = Map.of(key, keyHash);

        // 1. Prepare Index (OUTSIDE LOCK)
        final var preparedIndex = CHRONICLE_UTILS.prepareIndexAdditions(indexFileNames, Map.of(key, value),
                averageValue().getClass(), indexExclusions(), keyHashMap);

        // 2. PRE-LOCK: Look in MapDB using pre-calculated hash
        final var file = getDbFileFromHash(keyHash);
        if (file == null) {
            Logger.error("Key [{}] does not exist during update at [{}].", key, dataPath());
            return PutStatus.FAILED;
        }

        // 3. Now update
        return CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
            V prevValue = null;
            try (final var shared = openDb(file)) {
                // check if deleted in between by another thread
                if (shared.map.containsKey(key)) {
                    prevValue = shared.map.put(key, value);
                }
            }

            if (prevValue != null) {
                CHRONICLE_UTILS.applyIndexUpdates(dataPath(), indexFileNames, preparedIndex, Map.of(key, prevValue),
                        averageValue().getClass(), indexExclusions(), keyHashMap);
                Logger.info("Updated using key [{}] at [{}].", key, dataPath());
                return PutStatus.UPDATED;
            }
            return PutStatus.FAILED;
        });

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
        if (key == null) {
            return PutStatus.FAILED;
        }

        // Pre-calculate hash ONCE before all operations
        final byte[] keyHash = CHRONICLE_UTILS.to128BitHash(key);
        final var keyHashMap = Map.of(key, keyHash);

        // 1. Prepare Index (OUTSIDE LOCK)
        final var preparedIndex = CHRONICLE_UTILS.prepareIndexAdditions(indexFileNames, Map.of(key, value),
                averageValue().getClass(), indexExclusions(), keyHashMap);

        return CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
            final var file = getDbFileFromHash(keyHash);
            if (file != null) {
                Logger.error("Key [{}] already exists during insert at [{}].", key, dataPath());
                return PutStatus.FAILED;
            }

            var shared = openDb();
            try {
                if (!shared.map.containsKey(key)) {
                    shared = checkAndRotate(shared);
                    shared.map.put(key, value);
                    addToKeyMap(key, getDataFileState().currentFile(), keyHash);
                    CHRONICLE_UTILS.applyIndexAdditions(dataPath(), preparedIndex);
                    Logger.info("Inserted using key [{}] at [{}].", key, dataPath());
                    return PutStatus.INSERTED;
                }
                return PutStatus.FAILED;
            } finally {
                shared.close();
            }
        });
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
        final var keyMapUpdate = new ConcurrentHashMap<String, String>(putSize);

        // 0. Pre-calculate key hashes ONCE (OUTSIDE LOCK)
        final var keyHashMap = CHRONICLE_UTILS.preCalculateKeyHashes(keysToInsert);

        // 1. Prepare index additions (OUTSIDE LOCK)
        final var preparedIndex = CHRONICLE_UTILS.prepareIndexAdditions(indexFileNames(), map,
                averageValue().getClass(), indexExclusions(), keyHashMap);

        try (final var grouped = getDbFilesFromHashes(map.keySet(), keyHashMap)) {
            return CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
                var status = PutStatus.INSERTED;

                // 2. PHASE 1: Updates (Inside Lock)
                CHRONICLE_UTILS.processInParallel(grouped.fileGroups().entrySet(), entry -> {
                    final var file = entry.getKey();
                    try (final var shared = openDb(file)) {
                        CHRONICLE_UTILS.parallelIterable(entry.getValue(), Integer.MAX_VALUE, key -> {
                            if (shared.map.containsKey(key)) {
                                prevValues.put(key, shared.map.put(key, map.get(key)));
                                return true;
                            }
                            return false;
                        });
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                });

                if (!prevValues.isEmpty()) {
                    status = PutStatus.UPDATED;
                    Logger.info("Updated [{}] records at [{}].", prevValues.size(), dataPath());
                    keysToInsert.removeAll(prevValues.keySet());
                }

                final var tasks = new ArrayList<Runnable>();
                // 3. PHASE 2: Inserts (Inside Lock)
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
                            final String currentFileSnap = getDataFileState().currentFile();
                            final var currentShared = shared;
                            try {
                                CHRONICLE_UTILS.parallelIterable(batch, Integer.MAX_VALUE, key -> {
                                    final V value = map.get(key);
                                    currentShared.map.put(key, value);
                                    keyMapUpdate.put(key, currentFileSnap);
                                });
                            } catch (final InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new RuntimeException(e);
                            }

                            startIndex = endIndex;

                            // More to insert? Rotate and continue
                            if (startIndex < batches.size()) {
                                shared = checkAndRotate(shared, keyMapUpdate, keyHashMap);
                            }
                        }
                        Logger.info("Inserted [{}] records at [{}].", keysToInsert.size(), dataPath());
                    } finally {
                        shared.close();
                    }

                    // Filter prepared index to only include inserts for the additions task
                    final var preparedInserts = new ConcurrentHashMap<String, Map<String, byte[]>>();
                    preparedIndex.entrySet().parallelStream().forEach(entry -> {
                        final var filtered = new HashMap<>(entry.getValue());
                        filtered.keySet().retainAll(keysToInsert);

                        if (!filtered.isEmpty()) {
                            preparedInserts.put(entry.getKey(), filtered);
                        }
                    });

                    tasks.add(() -> CHRONICLE_UTILS.applyIndexAdditions(dataPath(), preparedInserts));
                }

                // 4. PHASE 3: KeyMap Update
                if (!keyMapUpdate.isEmpty()) {
                    tasks.add(() -> addAllToKeyMap(keyMapUpdate, keyHashMap));
                }

                // 5. PHASE 4: Indexing
                // Apply Updates (Existing Keys)
                tasks.add(
                        () -> CHRONICLE_UTILS.applyIndexUpdates(dataPath(), indexFileNames(), preparedIndex, prevValues,
                                averageValue().getClass(), indexExclusions(), keyHashMap));
                CHRONICLE_UTILS.processInParallel(tasks);
                return status;
            });
        } catch (final Exception e) {
            throw new RuntimeException(e);
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

        final var mapSize = map.size();
        final var prevValues = new ConcurrentHashMap<String, V>(mapSize);

        // 0. Pre-calculate key hashes ONCE (OUTSIDE LOCK)
        final var keys = map.keySet();
        final var keyHashMap = CHRONICLE_UTILS.preCalculateKeyHashes(keys);

        // 1. PHASE 1: Preparation (OUTSIDE LOCK)
        // Identify exactly which files hold these keys before blocking other writers.
        // Also prepare index additions (heavy reflection & hashing)
        final var preparedIndex = CHRONICLE_UTILS.prepareIndexAdditions(indexFileNames(), map,
                averageValue().getClass(), indexExclusions(), keyHashMap);

        try (final var grouped = getDbFilesFromHashes(keys, keyHashMap)) {
            // 2. PHASE 2: Data Update (INSIDE LOCK)
            return CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
                CHRONICLE_UTILS.processInParallel(grouped.fileGroups().entrySet(), entry -> {
                    final var file = entry.getKey();
                    try (final var shared = openDb(file)) {
                        CHRONICLE_UTILS.parallelIterable(entry.getValue(), Integer.MAX_VALUE, key -> {
                            // STRICT UPDATE: Only put if it actually exists in this file
                            if (shared.map.containsKey(key)) {
                                prevValues.put(key, shared.map.put(key, map.get(key)));
                                return true;
                            }
                            return false;
                        });
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                });

                final var prevValueSize = prevValues.size();
                if (prevValueSize == 0) {
                    Logger.error("All [{}] values do not exist during update at [{}]", mapSize, dataPath());
                    return PutStatus.FAILED;
                }

                var status = PutStatus.UPDATED;
                if (prevValueSize != mapSize) {
                    // Calculation for logging can stay inside since it's error-path only
                    final var missingKeys = new HashSet<>(keys);
                    missingKeys.removeAll(prevValues.keySet());
                    Logger.error("{} keys missing during update at [{}]. Missing: {}.",
                            missingKeys.size(), dataPath(), missingKeys);
                    status = PutStatus.PARTIAL;
                }

                // 3. PHASE 3: Indexing
                // Only happens if the transaction succeeded.
                CHRONICLE_UTILS.applyIndexUpdates(dataPath(), indexFileNames(), preparedIndex, prevValues,
                        averageValue().getClass(), indexExclusions(), keyHashMap);
                Logger.info("Updated [{}] records at [{}].", prevValues.size(), dataPath());
                return status;
            });

        } catch (final Exception e) {
            throw new RuntimeException(e);
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

        // 0. Pre-calculate key hashes ONCE (OUTSIDE LOCK)
        final var keyHashMap = CHRONICLE_UTILS.preCalculateKeyHashes(map.keySet());

        final var dataToInsert = new HashMap<>(map);
        final var keyMapUpdate = new ConcurrentHashMap<String, String>();

        // 1. OUTSIDE LOCK: Optimistic Preparation
        // Filter out existing keys to avoid preparing indexes for them (using
        // pre-calculated hashes)
        final var initiallyExisting = existsListFromHashes(dataToInsert.keySet(), keyHashMap);
        dataToInsert.keySet().removeAll(initiallyExisting);

        if (dataToInsert.isEmpty()) {
            Logger.error("No new records found (all exist) during insert at [{}].", dataPath());
            return PutStatus.FAILED;
        }

        // Filter keyHashMap to only include keys we're actually inserting
        final var filteredKeyHashMap = new HashMap<String, byte[]>(dataToInsert.size());
        dataToInsert.keySet().forEach(key -> filteredKeyHashMap.put(key, keyHashMap.get(key)));

        // Prepare index for the "optimistically safe" subset
        final var preparedIndex = CHRONICLE_UTILS.prepareIndexAdditions(indexFileNames(), dataToInsert,
                averageValue().getClass(), indexExclusions(), filteredKeyHashMap);

        return CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
            // 2. INSIDE LOCK: Double-Check for Race Conditions (using pre-calculated
            // hashes)
            final var raceConditionKeys = existsListFromHashes(dataToInsert.keySet(), keyHashMap);

            if (!raceConditionKeys.isEmpty()) {
                // Race detected! Another thread inserted these keys while we were preparing.
                dataToInsert.keySet().removeAll(raceConditionKeys);
                if (dataToInsert.isEmpty()) {
                    return PutStatus.FAILED;
                }

                // Cleanup: Remove the race-condition keys from our prepared index batch and
                // hash map. This is fast and avoids re-calculating everything.
                preparedIndex.values().forEach(idxMap -> idxMap.keySet().removeAll(raceConditionKeys));
                filteredKeyHashMap.keySet().removeAll(raceConditionKeys);
            }

            // Insert in batches
            var shared = openDb();
            try {
                final var batches = new ArrayList<>(dataToInsert.entrySet());
                int startIndex = 0;

                while (startIndex < batches.size()) {
                    final long remainingEntries = entries() - shared.map.size();
                    final int endIndex = (int) Math.min(startIndex + remainingEntries, batches.size());
                    final var batch = batches.subList(startIndex, endIndex);
                    final var currentFileSnap = getDataFileState().currentFile();
                    final var currentShared = shared;

                    try {
                        CHRONICLE_UTILS.parallelIterable(batch, Integer.MAX_VALUE, entry -> {
                            currentShared.map.put(entry.getKey(), entry.getValue());
                            keyMapUpdate.put(entry.getKey(), currentFileSnap);
                        });
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }

                    startIndex = endIndex;
                    if (startIndex < batches.size()) {
                        shared = checkAndRotate(shared, keyMapUpdate, filteredKeyHashMap);
                    }
                }
            } finally {
                shared.close();
            }

            final var tasks = new ArrayList<Runnable>();
            tasks.add(() -> addAllToKeyMap(keyMapUpdate, filteredKeyHashMap));

            // 3. Apply Indexing (Always use the prepared batch, now cleaned)
            tasks.add(() -> CHRONICLE_UTILS.applyIndexAdditions(dataPath(), preparedIndex));

            CHRONICLE_UTILS.processInParallel(tasks);
            Logger.info("Inserted [{}] records at [{}].", dataToInsert.size(), dataPath());
            return PutStatus.INSERTED;
        });
    }

    default Map<String, V> search(final Iterable<String> keys, final List<Search> filters, final int limit)
            throws InterruptedException {
        if (keys == null || !keys.iterator().hasNext() || filters.isEmpty()) {
            return Collections.emptyMap();
        }

        Logger.debug("Querying filtered keys at [{}] with [{}] remaining filters.", dataPath(), filters.size());
        final var map = new ConcurrentHashMap<String, V>(limit);
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, limit,
                        key -> {
                            final V value = shared.map.get(key);
                            if (value == null)
                                return false;

                            for (final var search : preparedFilters) {
                                if (!CHRONICLE_UTILS.search(search, key, value)) {
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
        try (final var grouped = getDbFiles(keys)) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                if (count.get() >= limit)
                    break;

                final var file = entry.getKey();
                final var keysForFile = entry.getValue(); // Iterable<String>

                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(keysForFile, limit - count.get(), count, key -> {
                        final V value = shared.map.get(key);
                        if (value == null)
                            return false;

                        for (final var search : preparedFilters) {
                            if (!CHRONICLE_UTILS.search(search, key, value)) {
                                return false;
                            }
                        }

                        map.put(key, value);
                        return true;
                    });
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return map;
    }

    default CsvObject searchCsv(final Iterable<String> keys, final List<Search> filters, final int limit)
            throws InterruptedException {
        if (keys == null || !keys.iterator().hasNext() || filters.isEmpty()) {
            return CsvObject.empty();
        }

        Logger.debug("Querying CSV filtered keys at [{}] with [{}] remaining filters.", dataPath(), filters.size());
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);
        final AtomicReference<String[]> headers = new AtomicReference<>(null);

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, limit,
                        key -> {
                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : preparedFilters) {
                                if (!CHRONICLE_UTILS.search(search, key, value)) {
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
        try (final var grouped = getDbFiles(keys)) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                if (count.get() >= limit)
                    break;

                final var file = entry.getKey();
                final var keysForFile = entry.getValue(); // Iterable<String>

                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(keysForFile, limit - count.get(), count, key -> {
                        final V value = shared.map.getUsing(key, using());
                        if (value == null)
                            return false;

                        for (final var search : preparedFilters) {
                            if (!CHRONICLE_UTILS.search(search, key, value)) {
                                return false;
                            }
                        }

                        headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                        rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
                        return true;
                    });
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    default Map<String, Map<String, Object>> searchSubset(final Iterable<String> keys,
            final List<Search> filters, final String[] fields, final int limit) throws InterruptedException {
        if (keys == null || !keys.iterator().hasNext()) {
            return Collections.emptyMap();
        }

        Logger.debug("Querying subset filtered keys at [{}] with [{}] remaining filters.", dataPath(), filters.size());
        final var map = new ConcurrentHashMap<String, Map<String, Object>>(limit);
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, limit,
                        key -> {
                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : preparedFilters) {
                                if (!CHRONICLE_UTILS.search(search, key, value)) {
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
        try (final var grouped = getDbFiles(keys)) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                if (count.get() >= limit)
                    break;

                final var file = entry.getKey();
                final var keysForFile = entry.getValue(); // Iterable<String>

                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(keysForFile, limit - count.get(), count, key -> {
                        final V value = shared.map.getUsing(key, using());
                        if (value == null)
                            return false;

                        for (final var search : preparedFilters) {
                            if (!CHRONICLE_UTILS.search(search, key, value)) {
                                return false;
                            }
                        }

                        map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value));
                        return true;
                    });
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return map;
    }

    default CsvObject searchSubsetCsv(final Iterable<String> keys, final List<Search> filters, final String[] fields,
            final int limit) throws InterruptedException {
        if (keys == null || !keys.iterator().hasNext()) {
            return CsvObject.empty();
        }

        Logger.debug("Querying subset CSV filtered keys at [{}] with [{}] remaining filters.", dataPath(),
                filters.size());
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var headers = CHRONICLE_UTILS.getCsvHeaders(fields);
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, limit,
                        key -> {
                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : preparedFilters) {
                                if (!CHRONICLE_UTILS.search(search, key, value)) {
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
        try (final var grouped = getDbFiles(keys)) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                if (count.get() >= limit)
                    break;

                final var file = entry.getKey();
                final var keysForFile = entry.getValue(); // Iterable<String>

                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(keysForFile, limit - count.get(), count, key -> {
                        final V value = shared.map.getUsing(key, using());
                        if (value == null)
                            return false;

                        for (final var search : preparedFilters) {
                            if (!CHRONICLE_UTILS.search(search, key, value)) {
                                return false;
                            }
                        }

                        rowQueue.add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value));
                        return true;
                    });
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return new CsvObject(headers, new ArrayList<>(rowQueue));
    }

    private void searchKeys(final Iterable<String> keys, final List<Search> filters, final Collection<String> results)
            throws InterruptedException {
        if (keys == null || !keys.iterator().hasNext()) {
            return;
        }

        Logger.debug("Querying filtered keys at [{}] with [{}] remaining filters.", dataPath(), filters.size());
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, Integer.MAX_VALUE,
                        key -> {
                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : preparedFilters) {
                                if (!CHRONICLE_UTILS.search(search, key, value)) {
                                    return false;
                                }
                            }

                            results.add(key);
                            return true;
                        });
            }
            return;
        }

        try (final var grouped = getDbFiles(keys)) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                final var file = entry.getKey();
                final var keysForFile = entry.getValue(); // Iterable<String>

                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(keysForFile, Integer.MAX_VALUE, key -> {
                        final V value = shared.map.getUsing(key, using());
                        if (value == null)
                            return false;

                        for (final var search : preparedFilters) {
                            if (!CHRONICLE_UTILS.search(search, key, value)) {
                                return false;
                            }
                        }

                        results.add(key);
                        return true;
                    });
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    default Map<String, V> search(final Iterable<String> keys, final List<Search> filters,
            final Set<String> excludedKeys, final int limit) throws InterruptedException {
        if (keys == null || !keys.iterator().hasNext()) {
            return Collections.emptyMap();
        }

        Logger.debug("Querying filtered keys at [{}] with [{}] remaining filters and [{}] excluded keys.", dataPath(),
                filters.size(), excludedKeys.size());
        final var map = new ConcurrentHashMap<String, V>(limit);
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, limit,
                        key -> {
                            if (excludedKeys.contains(key))
                                return false;

                            final V value = shared.map.get(key);
                            if (value == null)
                                return false;

                            for (final var search : preparedFilters) {
                                if (!CHRONICLE_UTILS.search(search, key, value)) {
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
        try (final var grouped = getDbFiles(keys)) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                if (count.get() >= limit)
                    break;

                final var file = entry.getKey();
                final var keysForFile = entry.getValue(); // Iterable<String>

                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(keysForFile, limit - count.get(), count,
                            key -> {
                                if (!excludedKeys.contains(key)) {
                                    final V value = shared.map.get(key);
                                    if (value == null)
                                        return false;

                                    for (final var search : preparedFilters) {
                                        if (!CHRONICLE_UTILS.search(search, key, value)) {
                                            return false;
                                        }
                                    }

                                    map.put(key, value);
                                    return true;
                                }
                                return false;
                            });
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return map;
    }

    default CsvObject searchCsv(final Iterable<String> keys, final List<Search> filters,
            final Set<String> excludedKeys, final int limit) throws InterruptedException {
        if (keys == null || !keys.iterator().hasNext()) {
            return CsvObject.empty();
        }

        Logger.debug("Querying filtered keys at [{}] with [{}] remaining filters and [{}] excluded keys.", dataPath(),
                filters.size(), excludedKeys.size());
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();
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

                            for (final var search : preparedFilters) {
                                if (!CHRONICLE_UTILS.search(search, key, value)) {
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
        try (final var grouped = getDbFiles(keys)) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                if (count.get() >= limit)
                    break;

                final var file = entry.getKey();
                final var keysForFile = entry.getValue(); // Iterable<String>

                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(keysForFile, limit - count.get(), count,
                            key -> {
                                if (!excludedKeys.contains(key)) {
                                    final V value = shared.map.getUsing(key, using());
                                    if (value == null)
                                        return false;

                                    for (final var search : preparedFilters) {
                                        if (!CHRONICLE_UTILS.search(search, key, value)) {
                                            return false;
                                        }
                                    }

                                    headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                                    rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
                                    return true;
                                }
                                return false;
                            });
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    default Map<String, Map<String, Object>> searchSubset(final Iterable<String> keys,
            final List<Search> filters, final Set<String> excludedKeys, final int limit, final String[] fields)
            throws InterruptedException {
        if (keys == null || !keys.iterator().hasNext()) {
            return Collections.emptyMap();
        }

        Logger.debug("Querying subset filtered keys at [{}] with [{}] remaining filters and [{}] excluded keys.",
                dataPath(), filters.size(), excludedKeys.size());
        final var map = new ConcurrentHashMap<String, Map<String, Object>>(limit);
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();
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

                            for (final var search : preparedFilters) {
                                if (!CHRONICLE_UTILS.search(search, key, value)) {
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
        try (final var grouped = getDbFiles(keys)) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                if (count.get() >= limit)
                    break;

                final var file = entry.getKey();
                final var keysForFile = entry.getValue(); // Iterable<String>

                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(keysForFile, limit - count.get(), count,
                            key -> {
                                if (!excludedKeys.contains(key)) {
                                    final V value = shared.map.getUsing(key, using());
                                    if (value == null)
                                        return false;

                                    for (final var search : preparedFilters) {
                                        if (!CHRONICLE_UTILS.search(search, key, value)) {
                                            return false;
                                        }
                                    }

                                    map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value));
                                    return true;
                                }
                                return false;
                            });
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return map;
    }

    default CsvObject searchSubsetCsv(final Iterable<String> keys, final List<Search> filters,
            final Set<String> excludedKeys, final String[] fields, final int limit) throws InterruptedException {
        if (keys == null || !keys.iterator().hasNext()) {
            return CsvObject.empty();
        }

        Logger.debug("Querying subset CSV filtered keys at [{}] with [{}] remaining filters and [{}] excluded keys.",
                dataPath(), filters.size(), excludedKeys.size());
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var headers = CHRONICLE_UTILS.getCsvHeaders(fields);
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();
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

                            for (final var search : preparedFilters) {
                                if (!CHRONICLE_UTILS.search(search, key, value)) {
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
        try (final var grouped = getDbFiles(keys)) {
            for (final var entry : grouped.fileGroups().entrySet()) {
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

                                for (final var search : preparedFilters) {
                                    if (!CHRONICLE_UTILS.search(search, key, value)) {
                                        return false;
                                    }
                                }

                                rowQueue.add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value));
                                return true;
                            });
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return new CsvObject(headers, new ArrayList<>(rowQueue));
    }

    default long searchCount(final Iterable<String> keys, final List<Search> filters)
            throws InterruptedException {
        if (keys == null || !keys.iterator().hasNext()) {
            return 0;
        }

        Logger.debug("Counting filtered keys at [{}] with [{}] remaining filters.", dataPath(), filters.size());
        final var limit = Integer.MAX_VALUE;

        // Determine minimum positive limit across all filters
        final var count = new LongAdder();
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, limit,
                        key -> {
                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : preparedFilters) {
                                if (!CHRONICLE_UTILS.search(search, key, value)) {
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
        try (final var grouped = getDbFiles(keys)) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                final var file = entry.getKey();
                final var keysForFile = entry.getValue(); // Iterable<String>

                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(keysForFile, limit - counter.get(), counter,
                            key -> {
                                final V value = shared.map.getUsing(key, using());
                                if (value == null)
                                    return false;

                                for (final var search : preparedFilters) {
                                    if (!CHRONICLE_UTILS.search(search, key, value)) {
                                        return false;
                                    }
                                }

                                count.increment();
                                return true;
                            });
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return count.sum();
    }

    // ========== HASH-BASED SEARCH METHODS (no re-hashing) ==========

    /**
     * Search by byte[] hashes with filters - uses direct KeyMap lookup without
     * re-hashing.
     */
    default Map<String, V> searchFromHashes(final Iterable<byte[]> hashes, final List<Search> filters, final int limit)
            throws InterruptedException {
        if (hashes == null || !hashes.iterator().hasNext() || filters.isEmpty()) {
            return Collections.emptyMap();
        }

        Logger.debug("Searching by hashes at [{}] with [{}] filters.", dataPath(), filters.size());
        final var map = new ConcurrentHashMap<String, V>(limit);
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        final AtomicInteger count = new AtomicInteger();
        try (final var grouped = getDbFilesFromHashes(hashes)) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                if (count.get() >= limit)
                    break;

                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), limit, count,
                            key -> {
                                final V value = shared.map.get(key);
                                if (value == null)
                                    return false;

                                for (final var search : preparedFilters) {
                                    if (!CHRONICLE_UTILS.search(search, key, value)) {
                                        return false;
                                    }
                                }

                                map.put(key, value);
                                return true;
                            });
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return map;
    }

    /**
     * Search subset by byte[] hashes with filters - uses direct KeyMap lookup
     * without re-hashing.
     */
    default Map<String, Map<String, Object>> searchSubsetFromHashes(final Iterable<byte[]> hashes,
            final List<Search> filters, final String[] fields, final int limit)
            throws InterruptedException {
        if (hashes == null || !hashes.iterator().hasNext()) {
            return Collections.emptyMap();
        }

        Logger.debug("Searching subset by hashes at [{}] with [{}] filters.", dataPath(), filters.size());
        final var map = new ConcurrentHashMap<String, Map<String, Object>>(limit);
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);

        final AtomicInteger count = new AtomicInteger();
        try (final var grouped = getDbFilesFromHashes(hashes)) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                if (count.get() >= limit)
                    break;

                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), limit, count,
                            key -> {
                                final V value = shared.map.getUsing(key, using());
                                if (value == null)
                                    return false;

                                for (final var search : preparedFilters) {
                                    if (!CHRONICLE_UTILS.search(search, key, value)) {
                                        return false;
                                    }
                                }

                                map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value));
                                return true;
                            });
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return map;
    }

    /**
     * Search CSV by byte[] hashes with filters - uses direct KeyMap lookup without
     * re-hashing.
     */
    default CsvObject searchCsvFromHashes(final Iterable<byte[]> hashes, final List<Search> filters, final int limit)
            throws InterruptedException {
        if (hashes == null || !hashes.iterator().hasNext() || filters.isEmpty()) {
            return CsvObject.empty();
        }

        Logger.debug("Searching CSV by hashes at [{}] with [{}] filters.", dataPath(), filters.size());
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);
        final AtomicReference<String[]> headers = new AtomicReference<>(null);

        final AtomicInteger count = new AtomicInteger();
        try (final var grouped = getDbFilesFromHashes(hashes)) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                if (count.get() >= limit)
                    break;

                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), limit, count,
                            key -> {
                                final V value = shared.map.getUsing(key, using());
                                if (value == null)
                                    return false;

                                for (final var search : preparedFilters) {
                                    if (!CHRONICLE_UTILS.search(search, key, value)) {
                                        return false;
                                    }
                                }

                                headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                                rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
                                return true;
                            });
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    /**
     * Search subset CSV by byte[] hashes with filters - uses direct KeyMap lookup
     * without re-hashing.
     */
    default CsvObject searchSubsetCsvFromHashes(final Iterable<byte[]> hashes, final List<Search> filters,
            final String[] fields, final int limit) throws InterruptedException {
        if (hashes == null || !hashes.iterator().hasNext()) {
            return CsvObject.empty();
        }

        Logger.debug("Searching subset CSV by hashes at [{}] with [{}] filters.", dataPath(), filters.size());
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);
        final var csvHeaders = CHRONICLE_UTILS.getCsvHeaders(fields);

        final AtomicInteger count = new AtomicInteger();
        try (final var grouped = getDbFilesFromHashes(hashes)) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                if (count.get() >= limit)
                    break;

                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), limit, count,
                            key -> {
                                final V value = shared.map.getUsing(key, using());
                                if (value == null)
                                    return false;

                                for (final var search : preparedFilters) {
                                    if (!CHRONICLE_UTILS.search(search, key, value)) {
                                        return false;
                                    }
                                }

                                rowQueue.add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value));
                                return true;
                            });
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return new CsvObject(csvHeaders, new ArrayList<>(rowQueue));
    }

    /**
     * Search keys by byte[] hashes with filters - returns String keys.
     */
    default Set<String> searchKeysFromHashes(final Iterable<byte[]> hashes, final List<Search> filters)
            throws InterruptedException {
        if (hashes == null || !hashes.iterator().hasNext()) {
            return Collections.emptySet();
        }

        Logger.debug("Searching keys by hashes at [{}] with [{}] filters.", dataPath(), filters.size());
        final var result = ConcurrentHashMap.<String>newKeySet();
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        try (final var grouped = getDbFilesFromHashes(hashes)) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), Integer.MAX_VALUE,
                            key -> {
                                final V value = shared.map.getUsing(key, using());
                                if (value == null)
                                    return false;

                                for (final var search : preparedFilters) {
                                    if (!CHRONICLE_UTILS.search(search, key, value)) {
                                        return false;
                                    }
                                }

                                result.add(key);
                                return true;
                            });
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    /**
     * Search keys list by byte[] hashes with filters - returns String keys.
     */
    default List<String> searchKeysListFromHashes(final Iterable<byte[]> hashes, final List<Search> filters)
            throws InterruptedException {
        if (hashes == null || !hashes.iterator().hasNext()) {
            return Collections.emptyList();
        }

        Logger.debug("Searching keys list by hashes at [{}] with [{}] filters.", dataPath(), filters.size());
        final var result = new ConcurrentLinkedQueue<String>();
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        try (final var grouped = getDbFilesFromHashes(hashes)) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), Integer.MAX_VALUE,
                            key -> {
                                final V value = shared.map.getUsing(key, using());
                                if (value == null)
                                    return false;

                                for (final var search : preparedFilters) {
                                    if (!CHRONICLE_UTILS.search(search, key, value)) {
                                        return false;
                                    }
                                }

                                result.add(key);
                                return true;
                            });
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return new ArrayList<>(result);
    }

    /**
     * Search count by byte[] hashes with filters.
     */
    default long searchCountFromHashes(final Iterable<byte[]> hashes, final List<Search> filters)
            throws InterruptedException {
        if (hashes == null || !hashes.iterator().hasNext()) {
            return 0;
        }

        Logger.debug("Counting by hashes at [{}] with [{}] filters.", dataPath(), filters.size());
        final var count = new LongAdder();
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        try (final var grouped = getDbFilesFromHashes(hashes)) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), Integer.MAX_VALUE,
                            key -> {
                                final V value = shared.map.getUsing(key, using());
                                if (value == null)
                                    return false;

                                for (final var search : preparedFilters) {
                                    if (!CHRONICLE_UTILS.search(search, key, value)) {
                                        return false;
                                    }
                                }

                                count.increment();
                                return true;
                            });
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return count.sum();
    }

    // ========== HASH-BASED SEARCH METHODS WITH EXCLUDED KEYS ==========

    /**
     * Search by byte[] hashes with filters and excluded keys.
     */
    default Map<String, V> searchFromHashes(final Iterable<byte[]> hashes, final List<Search> filters,
            final Set<String> excludedKeys, final int limit) throws InterruptedException {
        if (hashes == null || !hashes.iterator().hasNext() || limit <= 0) {
            return Collections.emptyMap();
        }

        Logger.debug("Searching by hashes at [{}] with [{}] filters and excluded keys.", dataPath(), filters.size());
        final var result = new ConcurrentHashMap<String, V>(limit);
        final var counter = new AtomicInteger();
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        try (final var grouped = getDbFilesFromHashes(hashes, Integer.MAX_VALUE, excludedKeys)) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                if (counter.get() >= limit)
                    break;

                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), limit, counter,
                            key -> {
                                final V value = shared.map.getUsing(key, using());
                                if (value == null)
                                    return false;

                                for (final var search : preparedFilters) {
                                    if (!CHRONICLE_UTILS.search(search, key, value)) {
                                        return false;
                                    }
                                }

                                result.put(key, value);
                                return true;
                            });
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    /**
     * Search CSV by byte[] hashes with filters and excluded keys.
     */
    default CsvObject searchCsvFromHashes(final Iterable<byte[]> hashes, final List<Search> filters,
            final Set<String> excludedKeys, final int limit) throws InterruptedException {
        if (hashes == null || !hashes.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.debug("Searching CSV by hashes at [{}] with [{}] filters and excluded keys.", dataPath(),
                filters.size());
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());
        final AtomicReference<String[]> csvHeaders = new AtomicReference<>(null);
        final AtomicInteger count = new AtomicInteger();
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        try (final var grouped = getDbFilesFromHashes(hashes, Integer.MAX_VALUE, excludedKeys)) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                if (count.get() >= limit)
                    break;

                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), limit, count,
                            key -> {
                                final V value = shared.map.getUsing(key, using());
                                if (value == null)
                                    return false;

                                for (final var search : preparedFilters) {
                                    if (!CHRONICLE_UTILS.search(search, key, value)) {
                                        return false;
                                    }
                                }

                                csvHeaders.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                                rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
                                return true;
                            });
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return new CsvObject(csvHeaders.get(), new ArrayList<>(rowQueue));
    }

    /**
     * Search subset by byte[] hashes with filters and excluded keys.
     */
    default Map<String, Map<String, Object>> searchSubsetFromHashes(final Iterable<byte[]> hashes,
            final List<Search> filters, final Set<String> excludedKeys, final String[] fields, final int limit)
            throws InterruptedException {
        if (hashes == null || !hashes.iterator().hasNext() || limit <= 0) {
            return Collections.emptyMap();
        }

        Logger.debug("Searching subset by hashes at [{}] with [{}] filters and excluded keys.", dataPath(),
                filters.size());
        final var result = new ConcurrentHashMap<String, Map<String, Object>>(limit);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());
        final AtomicInteger count = new AtomicInteger();
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        try (final var grouped = getDbFilesFromHashes(hashes, Integer.MAX_VALUE, excludedKeys)) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                if (count.get() >= limit)
                    break;

                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), limit, count,
                            key -> {
                                final V value = shared.map.getUsing(key, using());
                                if (value == null)
                                    return false;

                                for (final var search : preparedFilters) {
                                    if (!CHRONICLE_UTILS.search(search, key, value)) {
                                        return false;
                                    }
                                }

                                result.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value));
                                return true;
                            });
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    /**
     * Search subset CSV by byte[] hashes with filters and excluded keys.
     */
    default CsvObject searchSubsetCsvFromHashes(final Iterable<byte[]> hashes, final List<Search> filters,
            final Set<String> excludedKeys, final String[] fields, final int limit) throws InterruptedException {
        if (hashes == null || !hashes.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.debug("Searching subset CSV by hashes at [{}] with [{}] filters and excluded keys.", dataPath(),
                filters.size());
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());
        final var csvHeaders = CHRONICLE_UTILS.getCsvHeaders(fields);
        final AtomicInteger count = new AtomicInteger();
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        try (final var grouped = getDbFilesFromHashes(hashes, Integer.MAX_VALUE, excludedKeys)) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                if (count.get() >= limit)
                    break;

                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), limit, count,
                            key -> {
                                final V value = shared.map.getUsing(key, using());
                                if (value == null)
                                    return false;

                                for (final var search : preparedFilters) {
                                    if (!CHRONICLE_UTILS.search(search, key, value)) {
                                        return false;
                                    }
                                }

                                rowQueue.add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value));
                                return true;
                            });
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return new CsvObject(csvHeaders, new ArrayList<>(rowQueue));
    }

    // ========== END HASH-BASED SEARCH METHODS ==========

    private void search(final ChronicleMap<String, V> db, final List<Search> filters, final int limit,
            final Map<String, V> result, final AtomicInteger counter) {
        Logger.debug("Searching DB at [{}] for {} filters.", dataPath(), filters.size());
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        db.forEachEntryWhile(entry -> {
            final String key = entry.key().get();
            final V value = entry.value().getUsing(null);

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
        Logger.debug("Searching DB at [{}] for {} filters.", dataPath(), filters.size());
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
        Logger.debug("Subset searching DB at [{}] for {} filters.", dataPath(), filters.size());
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);

        db.forEachEntryWhile(entry -> {
            final String key = entry.key().get();
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

    private void searchSubsetCsv(final ChronicleMap<String, V> db, final List<Search> filters, final int limit,
            final ConcurrentLinkedQueue<Object[]> rowQueue, final AtomicInteger counter, final String[] fields) {
        Logger.debug("Subset CSV searching DB at [{}] for {} filters.", dataPath(), filters.size());
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);

        db.forEachEntryWhile(entry -> {
            final String key = entry.key().get();
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
            final Collection<String> results) {
        Logger.debug("Searching DB keys at [{}] for {} filters.", dataPath(), filters.size());
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

            results.add(key);
            return true;
        });
    }

    private void search(final ChronicleMap<String, V> db, final List<Search> filters, final Set<String> excludedKeys,
            final int limit, final Map<String, V> result, final AtomicInteger counter) {
        Logger.debug("Searching DB at [{}] using [{}] filters and [{}] excluded keys. Limit [{}]",
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

            final V value = entry.value().getUsing(null);
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
        Logger.debug("Searching DB at [{}] using [{}] filters and [{}] excluded keys. Limit [{}]",
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
        Logger.debug("Searching DB at [{}] using [{}] filters and [{}] excluded keys. Limit [{}]",
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
        Logger.debug("Subset CSV searching DB at [{}] using [{}] filters and [{}] excluded keys. Limit [{}]",
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
        Logger.debug("Searching DB keys at [{}] using [{}] filters and [{}] excluded keys.",
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

            final V value = entry.value().getUsing(using());
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
        Logger.debug("Counting DB at [{}] for {} filters.", dataPath(), filters.size());
        final var count = new AtomicInteger();
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
        Logger.debug("Searching in-memory map of [{}] with [{}] filters.", size, filters.size());
        final Map<String, V> result = new ConcurrentHashMap<>(Math.min(size, 10_000));
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        CHRONICLE_UTILS.processInParallel(db.entrySet(), entry -> {
            final String key = entry.getKey();
            final V value = entry.getValue();

            if (value == null) {
                return;
            }

            boolean match = true;
            for (final var search : preparedFilters) {
                if (!CHRONICLE_UTILS.search(search, key, value)) {
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
        Logger.debug("Index searching at [{}] for {}.", dataPath(), search.field());
        if (index == null || index.isEmpty()) {
            return new SearchResult(Collections.emptyList());
        }

        final SearchType searchType = search.searchType();
        final String searchTerm = CHRONICLE_UTILS.toStringOptimized(search.searchTerm());
        final Set<String> searchTermSet = (searchType == SearchType.IN || searchType == SearchType.NOT_IN)
                ? new HashSet<>((Collection<String>) search.searchTerm())
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
            final Set<byte[]> excludedHashes) {
        Logger.debug("Index searching at [{}] with [{}] excluded keys for {}.", dataPath(), excludedHashes.size(),
                search.field());
        if (index == null || index.isEmpty()) {
            return new SearchResult(Collections.emptyList());
        }

        final SearchType searchType = search.searchType();
        final String searchTerm = CHRONICLE_UTILS.toStringOptimized(search.searchTerm());
        final Set<String> searchTermSet = (searchType == SearchType.IN || searchType == SearchType.NOT_IN)
                ? new HashSet<>((Collection<String>) search.searchTerm())
                : null;
        final var searchTermBetween = searchType == SearchType.BETWEEN ? (List<Object>) search.searchTerm() : null;

        return switch (searchType) {
            case EQUAL -> MAP_DB.getEqualIndexSearch(index, searchTerm, search.limit(), excludedHashes);
            case NOT_EQUAL -> {
                final var keysBefore = MAP_DB.getBeforeIndexSearch(index, searchTerm, search.limit(), excludedHashes);
                final var keysAfter = MAP_DB.getAfterIndexSearch(index, searchTerm, search.limit(), excludedHashes);
                yield new SearchResult(
                        CHRONICLE_UTILS.concatIterable(keysBefore.results(), keysAfter.results(), search.limit()));
            }
            case LESS -> MAP_DB.getLessThanIndexSearch(index, searchTerm, search.limit(), excludedHashes);
            case LESS_OR_EQUAL ->
                MAP_DB.getLessThanOrEqualIndexSearch(index, searchTerm, search.limit(), excludedHashes);
            case GREATER -> MAP_DB.getGreaterThanIndexSearch(index, searchTerm, search.limit(), excludedHashes);
            case GREATER_OR_EQUAL ->
                MAP_DB.getGreaterThanOrEqualIndexSearch(index, searchTerm, search.limit(), excludedHashes);
            case LIKE -> MAP_DB.getLikeIndexSearch(index, searchTerm, search.limit(), excludedHashes);
            case NOT_LIKE -> MAP_DB.getNotLikeIndexSearch(index, searchTerm, search.limit(), excludedHashes);
            case STARTS_WITH -> MAP_DB.getStartsWithIndexSearch(index, searchTerm, search.limit(), excludedHashes);
            case ENDS_WITH -> MAP_DB.getEndsWithIndexSearch(index, searchTerm, search.limit(), excludedHashes);
            case IN -> MAP_DB.getInIndexSearch(index, searchTermSet, search.limit(), excludedHashes);
            case NOT_IN -> MAP_DB.getNotInIndexSearch(index, searchTermSet, search.limit(), excludedHashes);
            case BETWEEN -> MAP_DB.getBetweenIndexSearch(index, searchTermBetween.get(0).toString(),
                    searchTermBetween.get(1).toString(), search.limit(), excludedHashes);
            default -> throw new UnsupportedOperationException("Search type not supported: " + searchType);
        };
    }

    private <T> boolean isResultEmpty(final Iterable<T> result) {
        return result == null || !result.iterator().hasNext();
    }

    default Map<String, V> indexedSearch(final Search search) {
        final String indexPath = getIndexPath(search.field());
        final int limit = search.limit() > 0 ? search.limit() : HARD_LIMIT;
        try (final var sharedIndexSet = MAP_DB.openIndex(indexPath)) {
            final var searchResult = indexedSearch(search, sharedIndexSet.index);
            if (isResultEmpty(searchResult.results())) {
                return Collections.emptyMap();
            }
            return getFromHashes(searchResult.results(), limit);
        }
    }

    default Set<String> indexedSearchKeys(final Search search) {
        final String indexPath = getIndexPath(search.field());
        try (final var sharedIndexSet = MAP_DB.openIndex(indexPath)) {
            final var searchResult = indexedSearch(search, sharedIndexSet.index);
            if (isResultEmpty(searchResult.results())) {
                return Collections.emptySet();
            }
            return toSetOfKeysFromHashes(searchResult.results());
        }
    }

    default List<String> indexedSearchKeysList(final Search search) {
        final String indexPath = getIndexPath(search.field());
        try (final var sharedIndexSet = MAP_DB.openIndex(indexPath)) {
            final var searchResult = indexedSearch(search, sharedIndexSet.index);
            if (isResultEmpty(searchResult.results())) {
                return Collections.emptyList();
            }
            return toListOfKeysFromHashes(searchResult.results());
        }
    }

    default Map<String, V> indexedSearch(final Search search, final Set<String> excludedKeys) {
        final String indexPath = getIndexPath(search.field());
        final int limit = search.limit() > 0 ? search.limit() : HARD_LIMIT;
        try (final var sharedIndexSet = MAP_DB.openIndex(indexPath)) {
            final var excludedHashes = preCalculateExcludedHashes(excludedKeys);
            final var searchResult = indexedSearch(search, sharedIndexSet.index, excludedHashes);
            if (isResultEmpty(searchResult.results())) {
                return Collections.emptyMap();
            }
            return getFromHashes(searchResult.results(), limit);
        }
    }

    default Set<String> indexedSearchKeys(final Search search, final Set<String> excludedKeys) {
        final String indexPath = getIndexPath(search.field());
        try (final var sharedIndexSet = MAP_DB.openIndex(indexPath)) {
            final var excludedHashes = preCalculateExcludedHashes(excludedKeys);
            final var searchResult = indexedSearch(search, sharedIndexSet.index, excludedHashes);
            if (isResultEmpty(searchResult.results())) {
                return Collections.emptySet();
            }
            return toSetOfKeysFromHashes(searchResult.results());
        }
    }

    default List<String> indexedSearchKeysList(final Search search, final Set<String> excludedKeys) {
        final String indexPath = getIndexPath(search.field());
        try (final var sharedIndexSet = MAP_DB.openIndex(indexPath)) {
            final var excludedHashes = preCalculateExcludedHashes(excludedKeys);
            final var searchResult = indexedSearch(search, sharedIndexSet.index, excludedHashes);
            if (isResultEmpty(searchResult.results())) {
                return Collections.emptyList();
            }
            return toListOfKeysFromHashes(searchResult.results());
        }
    }

    default Map<String, V> searchMatching(final Collection<String> matchingKeys, final Search search)
            throws InterruptedException {
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

        CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
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
        CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
            try (final var shared = openDb(file)) {
                searchKeys(shared.map, searches, results);
            }
        });
        return results;
    }

    default List<String> searchKeysList(final List<Search> searches) {
        final var result = new ConcurrentLinkedQueue<String>();
        CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
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

        CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
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
        CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
            try (final var shared = openDb(file)) {
                searchKeys(shared.map, searches, excludedKeys, results);
            }
        });
        return results;
    }

    default List<String> searchKeysList(final List<Search> searches, final Set<String> excludedKeys) {
        final var result = new ConcurrentLinkedQueue<String>();
        CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
            try (final var shared = openDb(file)) {
                searchKeys(shared.map, searches, excludedKeys, result);
            }
        });
        return new ArrayList<>(result);
    }

    default Set<String> searchKeys(final Iterable<String> keys, final List<Search> filters)
            throws InterruptedException {
        final var results = ConcurrentHashMap.<String>newKeySet();
        searchKeys(keys, filters, results);
        return new HashSet<>(results);
    }

    default List<String> searchKeysList(final Iterable<String> keys, final List<Search> filters)
            throws InterruptedException {
        final var results = new ConcurrentLinkedQueue<String>();
        searchKeys(keys, filters, results);
        return new ArrayList<>(results);
    }

    default Map<String, Map<String, Object>> multiSearchSubset(final List<Search> searches, final String[] fields)
            throws InterruptedException {
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
                indexedSearch = searches.size() == 1 ? s : s.withLimit(-1);
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = (minLimit == Integer.MAX_VALUE) ? HARD_LIMIT : minLimit;

        SearchResult searchResult = null;
        String indexPath = null;
        SharedIndexSet sharedIndexSet = null;
        // Step 2: Perform indexed search (only if indexedSearch != null)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            sharedIndexSet = MAP_DB.openIndex(indexPath);
            searchResult = indexedSearch(indexedSearch, sharedIndexSet.index);
            if (isResultEmpty(searchResult.results())) {
                sharedIndexSet.close();
                return Collections.emptyMap();
            }

            if (remainingSearches.isEmpty()) {
                try {
                    return getSubsetFromHashes(searchResult.results(), fields, limit);
                } finally {
                    sharedIndexSet.close();
                }
            }
        }

        // Step 3: Manual search
        try {
            if (searchResult == null) {
                final var result = new ConcurrentHashMap<String, Map<String, Object>>(limit);
                final var counter = new AtomicInteger();
                CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
                    if (counter.get() >= limit) {
                        return;
                    }
                    try (final var shared = openDb(file)) {
                        searchSubset(shared.map, searches, limit, result, counter, fields);
                    }
                });

                return result;
            } else {
                return searchSubsetFromHashes(searchResult.results(), remainingSearches, fields, limit);
            }
        } finally {
            if (sharedIndexSet != null) {
                sharedIndexSet.close();
            }
        }
    }

    default CsvObject multiSearchSubsetCsv(final List<Search> searches, final String[] fields)
            throws InterruptedException {
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
                indexedSearch = searches.size() == 1 ? s : s.withLimit(-1);
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = (minLimit == Integer.MAX_VALUE) ? HARD_LIMIT : minLimit;

        SearchResult searchResult = null;
        String indexPath = null;
        SharedIndexSet sharedIndexSet = null;
        // Step 2: Perform indexed search (only if indexedSearch != null)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            sharedIndexSet = MAP_DB.openIndex(indexPath);
            searchResult = indexedSearch(indexedSearch, sharedIndexSet.index);
            if (isResultEmpty(searchResult.results())) {
                sharedIndexSet.close();
                return CsvObject.empty();
            }

            if (remainingSearches.isEmpty()) {
                try {
                    return getSubsetCsvFromHashes(searchResult.results(), fields, limit);
                } finally {
                    sharedIndexSet.close();
                }
            }
        }

        // Step 3: Manual search
        try {
            if (searchResult == null) {
                final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
                final var counter = new AtomicInteger();
                CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
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
                return searchSubsetCsvFromHashes(searchResult.results(), remainingSearches, fields, limit);
            }
        } finally {
            if (sharedIndexSet != null) {
                sharedIndexSet.close();
            }
        }
    }

    default Map<String, V> multiSearch(final List<Search> searches) throws InterruptedException {
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
                indexedSearch = searches.size() == 1 ? s : s.withLimit(-1);
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = (minLimit == Integer.MAX_VALUE) ? HARD_LIMIT : minLimit;

        SearchResult searchResult = null;
        String indexPath = null;
        SharedIndexSet sharedIndexSet = null;
        // Step 2: Perform indexed search (only if indexedSearch != null)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            sharedIndexSet = MAP_DB.openIndex(indexPath);
            searchResult = indexedSearch(indexedSearch, sharedIndexSet.index);
            if (isResultEmpty(searchResult.results())) {
                sharedIndexSet.close();
                return Collections.emptyMap();
            }

            if (remainingSearches.isEmpty()) {
                try {
                    return getFromHashes(searchResult.results(), limit);
                } finally {
                    sharedIndexSet.close();
                }
            }
        }

        // Step 3: Manual search
        try {
            if (searchResult == null) {
                final Map<String, V> result = new ConcurrentHashMap<>(limit);
                final var counter = new AtomicInteger();
                CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
                    if (counter.get() >= limit) {
                        return;
                    }
                    try (final var shared = openDb(file)) {
                        search(shared.map, searches, limit, result, counter);
                    }
                });

                return result;
            } else {
                return searchFromHashes(searchResult.results(), remainingSearches, limit);
            }
        } finally {
            if (sharedIndexSet != null) {
                sharedIndexSet.close();
            }
        }
    }

    default CsvObject multiSearchCsv(final List<Search> searches) throws InterruptedException {
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
                indexedSearch = searches.size() == 1 ? s : s.withLimit(-1);
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = (minLimit == Integer.MAX_VALUE) ? HARD_LIMIT : minLimit;

        SearchResult searchResult = null;
        String indexPath = null;
        SharedIndexSet sharedIndexSet = null;
        // Step 2: Perform indexed search (only if indexedSearch != null)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            sharedIndexSet = MAP_DB.openIndex(indexPath);
            searchResult = indexedSearch(indexedSearch, sharedIndexSet.index);
            if (isResultEmpty(searchResult.results())) {
                sharedIndexSet.close();
                return CsvObject.empty();
            }

            if (remainingSearches.isEmpty()) {
                try {
                    return getCsvFromHashes(searchResult.results(), limit);
                } finally {
                    sharedIndexSet.close();
                }
            }
        }

        // Step 3: Manual search
        try {
            if (searchResult == null) {
                final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
                final AtomicReference<String[]> headers = new AtomicReference<>(null);
                final var counter = new AtomicInteger();
                CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
                    if (counter.get() >= limit) {
                        return;
                    }
                    try (final var shared = openDb(file)) {
                        searchCsv(shared.map, searches, limit, rowQueue, counter, headers);
                    }
                });

                return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
            } else {
                return searchCsvFromHashes(searchResult.results(), remainingSearches, limit);
            }
        } finally {
            if (sharedIndexSet != null) {
                sharedIndexSet.close();
            }
        }
    }

    default Set<String> multiSearchKeys(final List<Search> searches) throws InterruptedException {
        if (searches == null || searches.isEmpty()) {
            return Collections.emptySet();
        }

        final Set<String> indexFileNames = indexFileNames();

        // Step 1: Separate searches into first-indexed and remaining
        Search indexedSearch = null;
        final List<Search> remainingSearches = new ArrayList<>(searches.size());

        for (final Search s : searches) {
            if (!s.skipIndex() && indexedSearch == null && indexFileNames.contains(s.field())) {
                indexedSearch = searches.size() == 1 ? s : s.withLimit(-1);
            } else {
                remainingSearches.add(s);
            }
        }

        SearchResult searchResult = null;
        String indexPath = null;
        SharedIndexSet sharedIndexSet = null;
        // Step 2: Perform indexed search (only if indexedSearch != null)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            sharedIndexSet = MAP_DB.openIndex(indexPath);
            searchResult = indexedSearch(indexedSearch, sharedIndexSet.index);
            if (isResultEmpty(searchResult.results())) {
                sharedIndexSet.close();
                return Collections.emptySet();
            }

            if (remainingSearches.isEmpty()) {
                try {
                    return toSetOfKeysFromHashes(searchResult.results());
                } finally {
                    sharedIndexSet.close();
                }
            }
        }

        // Step 3: Manual search
        try {
            if (searchResult == null) {
                return searchKeys(searches);
            } else {
                return searchKeysFromHashes(searchResult.results(), remainingSearches);
            }
        } finally {
            if (sharedIndexSet != null) {
                sharedIndexSet.close();
            }
        }
    }

    default List<String> multiSearchKeysList(final List<Search> searches) throws InterruptedException {
        if (searches == null || searches.isEmpty()) {
            return Collections.emptyList();
        }

        final Set<String> indexFileNames = indexFileNames();

        // Step 1: Separate searches into first-indexed and remaining
        Search indexedSearch = null;
        final List<Search> remainingSearches = new ArrayList<>(searches.size());

        for (final Search s : searches) {
            if (!s.skipIndex() && indexedSearch == null && indexFileNames.contains(s.field())) {
                indexedSearch = searches.size() == 1 ? s : s.withLimit(-1);
            } else {
                remainingSearches.add(s);
            }
        }

        SearchResult searchResult = null;
        String indexPath = null;
        SharedIndexSet sharedIndexSet = null;
        // Step 2: Perform indexed search (only if indexedSearch != null)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            sharedIndexSet = MAP_DB.openIndex(indexPath);
            searchResult = indexedSearch(indexedSearch, sharedIndexSet.index);
            if (isResultEmpty(searchResult.results())) {
                sharedIndexSet.close();
                return Collections.emptyList();
            }

            if (remainingSearches.isEmpty()) {
                try {
                    return toListOfKeysFromHashes(searchResult.results());
                } finally {
                    sharedIndexSet.close();
                }
            }
        }

        // Step 3: Manual search
        try {
            if (searchResult == null) {
                return searchKeysList(searches);
            } else {
                return searchKeysListFromHashes(searchResult.results(), remainingSearches);
            }
        } finally {
            if (sharedIndexSet != null) {
                sharedIndexSet.close();
            }
        }
    }

    default Map<String, V> multiSearch(final List<Search> searches, final Set<String> excludedKeys)
            throws InterruptedException {
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
                indexedSearch = searches.size() == 1 ? s : s.withLimit(-1);
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = (minLimit == Integer.MAX_VALUE) ? HARD_LIMIT : minLimit;

        SearchResult searchResult = null;
        String indexPath = null;
        SharedIndexSet sharedIndexSet = null;
        // Step 2: Perform indexed search (only if indexedSearch != null)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            sharedIndexSet = MAP_DB.openIndex(indexPath);
            searchResult = indexedSearch(indexedSearch, sharedIndexSet.index);
            if (isResultEmpty(searchResult.results())) {
                sharedIndexSet.close();
                return Collections.emptyMap();
            }

            if (remainingSearches.isEmpty()) {
                try {
                    return getFromHashes(searchResult.results(), excludedKeys, limit);
                } finally {
                    sharedIndexSet.close();
                }
            }
        }

        // Step 3: Manual search
        try {
            if (searchResult == null) {
                final Map<String, V> result = new ConcurrentHashMap<>(limit);
                final var counter = new AtomicInteger(0);
                CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
                    if (counter.get() >= limit) {
                        return;
                    }
                    try (final var shared = openDb(file)) {
                        search(shared.map, searches, excludedKeys, limit, result, counter);
                    }
                });

                return result;
            } else {
                return searchFromHashes(searchResult.results(), remainingSearches, excludedKeys, limit);
            }
        } finally {
            if (sharedIndexSet != null) {
                sharedIndexSet.close();
            }
        }
    }

    default CsvObject multiSearchCsv(final List<Search> searches, final Set<String> excludedKeys)
            throws InterruptedException {
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
                indexedSearch = searches.size() == 1 ? s : s.withLimit(-1);
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = (minLimit == Integer.MAX_VALUE) ? HARD_LIMIT : minLimit;

        SearchResult searchResult = null;
        String indexPath = null;
        SharedIndexSet sharedIndexSet = null;
        // Step 2: Perform indexed search (only if indexedSearch != null)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            sharedIndexSet = MAP_DB.openIndex(indexPath);
            searchResult = indexedSearch(indexedSearch, sharedIndexSet.index);
            if (isResultEmpty(searchResult.results())) {
                sharedIndexSet.close();
                return CsvObject.empty();
            }

            if (remainingSearches.isEmpty()) {
                try {
                    return getCsvFromHashes(searchResult.results(), excludedKeys, limit);
                } finally {
                    sharedIndexSet.close();
                }
            }
        }

        // Step 3: Manual search
        try {
            if (searchResult == null) {
                final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
                final AtomicReference<String[]> headers = new AtomicReference<>(null);
                final var counter = new AtomicInteger(0);
                CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
                    if (counter.get() >= limit) {
                        return;
                    }
                    try (final var shared = openDb(file)) {
                        searchCsv(shared.map, searches, excludedKeys, limit, rowQueue, counter, headers);
                    }
                });

                return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
            } else {
                return searchCsvFromHashes(searchResult.results(), remainingSearches, excludedKeys, limit);
            }
        } finally {
            if (sharedIndexSet != null) {
                sharedIndexSet.close();
            }
        }
    }

    default Map<String, Map<String, Object>> multiSearchSubset(final List<Search> searches,
            final Set<String> excludedKeys, final String[] fields) throws InterruptedException {
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
                indexedSearch = searches.size() == 1 ? s : s.withLimit(-1);
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = (minLimit == Integer.MAX_VALUE) ? HARD_LIMIT : minLimit;

        SearchResult searchResult = null;
        String indexPath = null;
        SharedIndexSet sharedIndexSet = null;
        // Step 2: Perform indexed search (only if indexedSearch != null)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            sharedIndexSet = MAP_DB.openIndex(indexPath);
            searchResult = indexedSearch(indexedSearch, sharedIndexSet.index);
            if (isResultEmpty(searchResult.results())) {
                sharedIndexSet.close();
                return Collections.emptyMap();
            }

            if (remainingSearches.isEmpty()) {
                try {
                    return getSubsetFromHashes(searchResult.results(), excludedKeys, fields, limit);
                } finally {
                    sharedIndexSet.close();
                }
            }
        }

        // Step 3: Manual search
        try {
            if (searchResult == null) {
                final var result = new ConcurrentHashMap<String, Map<String, Object>>(limit);
                final var counter = new AtomicInteger(0);
                CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
                    if (counter.get() >= limit) {
                        return;
                    }
                    try (final var shared = openDb(file)) {
                        searchSubset(shared.map, searches, excludedKeys, limit, result, counter, fields);
                    }
                });

                return result;
            } else {
                return searchSubsetFromHashes(searchResult.results(), remainingSearches, excludedKeys, fields, limit);
            }
        } finally {
            if (sharedIndexSet != null) {
                sharedIndexSet.close();
            }
        }
    }

    default CsvObject multiSearchSubsetCsv(final List<Search> searches, final Set<String> excludedKeys,
            final String[] fields) throws InterruptedException {
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
                indexedSearch = searches.size() == 1 ? s : s.withLimit(-1);
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = (minLimit == Integer.MAX_VALUE) ? HARD_LIMIT : minLimit;

        SearchResult searchResult = null;
        String indexPath = null;
        SharedIndexSet sharedIndexSet = null;
        // Step 2: Perform indexed search (only if indexedSearch != null)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            sharedIndexSet = MAP_DB.openIndex(indexPath);
            searchResult = indexedSearch(indexedSearch, sharedIndexSet.index);
            if (isResultEmpty(searchResult.results())) {
                sharedIndexSet.close();
                return CsvObject.empty();
            }

            if (remainingSearches.isEmpty()) {
                try {
                    return getSubsetCsvFromHashes(searchResult.results(), excludedKeys, fields, limit);
                } finally {
                    sharedIndexSet.close();
                }
            }
        }

        // Step 3: Manual search
        try {
            if (searchResult == null) {
                final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
                final var counter = new AtomicInteger(0);
                CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
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
                return searchSubsetCsvFromHashes(searchResult.results(), remainingSearches, excludedKeys, fields,
                        limit);
            }
        } finally {
            if (sharedIndexSet != null) {
                sharedIndexSet.close();
            }
        }
    }

    default Map<String, V> multiSearch(final Collection<String> matchingKeys, final List<Search> searches)
            throws InterruptedException {
        final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(HARD_LIMIT);

        return search(matchingKeys, searches, limit);
    }

    default CsvObject multiSearchCsv(final Collection<String> matchingKeys, final List<Search> searches)
            throws InterruptedException {
        final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(HARD_LIMIT);

        return searchCsv(matchingKeys, searches, limit);
    }

    default Map<String, Map<String, Object>> multiSearchSubset(final Collection<String> matchingKeys,
            final List<Search> searches, final String[] fields) throws InterruptedException {
        final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(HARD_LIMIT);

        return searchSubset(matchingKeys, searches, fields, limit);
    }

    default CsvObject multiSearchSubsetCsv(final Collection<String> matchingKeys, final List<Search> searches,
            final String[] fields) throws InterruptedException {
        final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(HARD_LIMIT);

        return searchSubsetCsv(matchingKeys, searches, fields, limit);
    }

    default long multiSearchCount(final List<Search> searches) throws InterruptedException {
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
                indexedSearch = searches.size() == 1 ? s : s.withLimit(-1);
            } else {
                remainingSearches.add(s);
            }
        }

        String indexPath = null;
        SearchResult searchResult = null;
        SharedIndexSet sharedIndexSet = null;
        // Step 2: Indexed search (only first index used)
        if (indexedSearch != null) {
            indexPath = getIndexPath(indexedSearch.field());
            sharedIndexSet = MAP_DB.openIndex(indexPath);
            searchResult = indexedSearch(indexedSearch, sharedIndexSet.index);
            if (isResultEmpty(searchResult.results())) {
                sharedIndexSet.close();
                return 0;
            }
            if (remainingSearches.isEmpty()) {
                try {
                    return MAP_DB.fastCount(searchResult.results());
                } finally {
                    sharedIndexSet.close();
                }
            }
        }

        try {
            if (searchResult == null) {
                final var totalCount = new java.util.concurrent.atomic.LongAdder();
                CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
                    try (final var shared = openDb(file)) {
                        totalCount.add(searchCount(shared.map, searches));
                    }
                });
                return totalCount.intValue();
            } else {
                return searchCountFromHashes(searchResult.results(), remainingSearches);
            }
        } finally {
            if (sharedIndexSet != null) {
                sharedIndexSet.close();
            }
        }
    }

    /**
     * Current size of the data
     *
     * @return int size
     */
    default int size() {
        Logger.debug("Getting DB size at [{}].", dataPath());
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            return sharedKeyMap.map.size();
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

    /**
     * Checks if a key exists in the database.
     *
     * @param key the primary key to check
     * @return true if the key exists, false otherwise
     */
    default boolean exists(final String key) {
        Logger.debug("Checking [{}] existence at [{}].", key, dataPath());
        return existsFromHash(CHRONICLE_UTILS.to128BitHash(key));
    }

    /**
     * Checks if a key exists using a pre-calculated hash.
     * Avoids re-hashing when the hash is already available.
     *
     * @param keyHash the 16-byte hash of the key
     * @return true if the key exists, false otherwise
     */
    default boolean existsFromHash(final byte[] keyHash) {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            return sharedKeyMap.map.containsKey(keyHash);
        }
    }

    /**
     * Checks existence for multiple keys.
     *
     * @param keys the keys to check
     * @return map of key to existence boolean
     */
    default Map<String, Boolean> exists(final Collection<String> keys) {
        Logger.debug("Checking [{}] keys existence at [{}].", keys.size(), dataPath());
        return existsFromHashes(keys, CHRONICLE_UTILS.preCalculateKeyHashes(keys));
    }

    /**
     * Checks existence for multiple keys using pre-calculated hashes.
     * Avoids re-hashing when hashes are already available.
     *
     * @param keys       the keys to check
     * @param keyHashMap pre-calculated map of primary key to 16-byte hash
     * @return map of key to existence boolean
     */
    default Map<String, Boolean> existsFromHashes(final Collection<String> keys, final Map<String, byte[]> keyHashMap) {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            return keys.parallelStream()
                    .collect(Collectors.toConcurrentMap(
                            key -> key,
                            key -> sharedKeyMap.map.containsKey(keyHashMap.get(key))));
        }
    }

    /**
     * Returns only the keys that exist in the database.
     *
     * @param keys the keys to check
     * @return list of keys that exist
     */
    default List<String> existsList(final Collection<String> keys) {
        Logger.debug("Checking [{}] keys existence at [{}].", keys.size(), dataPath());
        return existsListFromHashes(keys, CHRONICLE_UTILS.preCalculateKeyHashes(keys));
    }

    /**
     * Returns only the keys that exist, using pre-calculated hashes.
     * Avoids re-hashing when hashes are already available.
     *
     * @param keys       the keys to check
     * @param keyHashMap pre-calculated map of primary key to 16-byte hash
     * @return list of keys that exist
     */
    default List<String> existsListFromHashes(final Collection<String> keys, final Map<String, byte[]> keyHashMap) {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            return keys.parallelStream()
                    .filter(key -> sharedKeyMap.map.containsKey(keyHashMap.get(key)))
                    .collect(Collectors.toList());
        }
    }

    /**
     * Returns only the keys that do not exist in the database.
     *
     * @param keys the keys to check
     * @return set of keys that do not exist
     */
    default Set<String> notExists(final Collection<String> keys) {
        Logger.debug("Checking [{}] keys non-existence at [{}].", keys.size(), dataPath());
        return notExistsFromHashes(keys, CHRONICLE_UTILS.preCalculateKeyHashes(keys));
    }

    /**
     * Returns only the keys that do not exist, using pre-calculated hashes.
     * Avoids re-hashing when hashes are already available.
     *
     * @param keys       the keys to check
     * @param keyHashMap pre-calculated map of primary key to 16-byte hash
     * @return set of keys that do not exist
     */
    default Set<String> notExistsFromHashes(final Collection<String> keys, final Map<String, byte[]> keyHashMap) {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            return keys.parallelStream()
                    .filter(k -> !sharedKeyMap.map.containsKey(keyHashMap.get(k)))
                    .collect(Collectors.toSet());
        }
    }

    /**
     * Returns only the keys that do not exist in the database.
     *
     * @param keys the keys to check
     * @return list of keys that do not exist
     */
    default List<String> notExistsList(final Collection<String> keys) {
        Logger.debug("Checking [{}] keys non-existence at [{}].", keys.size(), dataPath());
        return notExistsListFromHashes(keys, CHRONICLE_UTILS.preCalculateKeyHashes(keys));
    }

    /**
     * Returns only the keys that do not exist, using pre-calculated hashes.
     * Avoids re-hashing when hashes are already available.
     *
     * @param keys       the keys to check
     * @param keyHashMap pre-calculated map of primary key to 16-byte hash
     * @return list of keys that do not exist
     */
    default List<String> notExistsListFromHashes(final Collection<String> keys, final Map<String, byte[]> keyHashMap) {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            return keys.parallelStream()
                    .filter(k -> !sharedKeyMap.map.containsKey(keyHashMap.get(k)))
                    .collect(Collectors.toList());
        }
    }
}
