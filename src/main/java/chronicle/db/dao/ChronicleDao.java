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
 * <li>{@code /files/} - static files (pdf, html)</li>
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
            DATA_FILE = "data", LOCKED_FILE = "locked-", RECOVER_FILE = "recovery", ENTRY_SIZE_FILE = "entrySize",
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
     * reaches this limit, a new file is created (file rotation).
     * </p>
     * <p>
     * Can be overridden at runtime via system property:
     * {@code chronicle.<entityName>.entries=<count>}
     * <br>
     * Example: {@code -Dchronicle.User.entries=50000}
     * </p>
     * <p>
     * After changing this value, run vacuum to recreate files with new sizing.
     * DAOs can override this method to provide a different default.
     * </p>
     *
     * @return Expected entries per file (default 1000, or system property override)
     */
    default long entries() {
        return Long.getLong("chronicle." + name() + ".entries", 1000L);
    }

    /**
     * Returns the average key size in bytes for Chronicle Map allocation.
     * <p>
     * Can be overridden at runtime via system property:
     * {@code chronicle.<entityName>.key.size=<bytes>}
     * <br>
     * Example: {@code -Dchronicle.User.key.size=64}
     * </p>
     * <p>
     * DAOs can override this method to provide a different default.
     * </p>
     *
     * @return Average key size in bytes (default 36 for UUID)
     */
    default int averageKeySize() {
        return Integer.getInteger("chronicle." + name() + ".key.size", 36);
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
     * <p>
     * Can be set at runtime via system property:
     * {@code chronicle.<entityName>.indexes=field1,field2,field3}
     * <br>
     * Example: {@code -Dchronicle.User.indexes=email,status}
     * </p>
     * <p>
     * DAOs can override this method to provide hardcoded indexes.
     * </p>
     *
     * @return Set of field names to index (default: empty, or from system property)
     */
    default Set<String> indexFileNames() {
        final var indexes = System.getProperty("chronicle." + name() + ".indexes");
        if (indexes != null && !indexes.isBlank()) {
            final var result = new HashSet<String>();
            for (final var index : indexes.split(",")) {
                final var trimmed = index.trim();
                if (!trimmed.isEmpty()) {
                    result.add(trimmed);
                }
            }
            return result;
        }
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

    /**
     * Opens a ChronicleMap database file from a specific directory.
     * <p>
     * Returns a {@link SharedChronicleMap} that must be closed after use (use
     * try-with-resources).
     * Uses the configured {@link #entries()} for capacity.
     * </p>
     *
     * @param dataDir  the subdirectory (e.g., {@link #DATA_DIR},
     *                 {@link #BACKUP_DIR})
     * @param fileName the database file name
     * @return a SharedChronicleMap handle (must be closed)
     */
    default SharedChronicleMap<String, V> openDb(final String dataDir, final String fileName) {
        return CHRONICLE_DB.open(name(), entries(), averageKeySize(), averageValue(), dataPath() + dataDir + fileName,
                bloatFactor());
    }

    /**
     * Opens a ChronicleMap database file with a custom entry capacity.
     * <p>
     * Use this overload when creating a new file with a different size than the
     * default.
     * </p>
     *
     * @param dataDir  the subdirectory (e.g., {@link #DATA_DIR},
     *                 {@link #BACKUP_DIR})
     * @param fileName the database file name
     * @param entries  the maximum number of entries for this file
     * @return a SharedChronicleMap handle (must be closed)
     */
    default SharedChronicleMap<String, V> openDb(final String dataDir, final String fileName, final long entries) {
        return CHRONICLE_DB.open(name(), entries, averageKeySize(), averageValue(), dataPath() + dataDir + fileName,
                bloatFactor());
    }

    /**
     * Opens the current active data file for this DAO.
     * <p>
     * This is the primary method for accessing data. Opens the current file
     * (the latest file in rotation sequence) from the data directory.
     * </p>
     *
     * @return a SharedChronicleMap handle (must be closed)
     */
    default SharedChronicleMap<String, V> openDb() {
        return openDb(DATA_DIR, getDataFileState().currentFile());
    }

    /**
     * Opens a specific data file by name from the data directory.
     *
     * @param fileName the database file name (e.g., "data", "data-2")
     * @return a SharedChronicleMap handle (must be closed)
     */
    default SharedChronicleMap<String, V> openDb(final String fileName) {
        return openDb(DATA_DIR, fileName);
    }

    /**
     * Opens a specific data file with a custom entry capacity.
     *
     * @param fileName the database file name
     * @param entries  the maximum number of entries for this file
     * @return a SharedChronicleMap handle (must be closed)
     */
    default SharedChronicleMap<String, V> openDb(final String fileName, final long entries) {
        return openDb(DATA_DIR, fileName, entries);
    }

    /**
     * Returns the file path for the KeyMap (hash → primary key + file mapping).
     * <p>
     * The KeyMap is a MapDB HTreeMap that maps 128-bit hashes to
     * {@link KeyMapValue}
     * containing the primary key and the data file where it resides.
     * Path is cached for performance.
     * </p>
     *
     * @return the KeyMap file path
     */
    default String getKeyMapPath() {
        return KEY_MAP_PATH_CACHE.computeIfAbsent(dataPath(), k -> k + DATA_DIR + KEY_FILE);
    }

    /**
     * Populates the KeyMap from all data files.
     * <p>
     * Iterates through all data files in parallel, collecting keys and inserting
     * them into the KeyMap with their 128-bit hash and file location.
     * Called during initialization when KeyMap doesn't exist.
     * </p>
     *
     * @param dataFiles the set of data file names to scan
     * @param keyMap    the MapDB HTreeMap to populate
     */
    private void populateKeyMap(final Set<String> dataFiles, final HTreeMap<byte[], KeyMapValue> keyMap) {
        CHRONICLE_UTILS.processInParallel(dataFiles, file -> {
            try (final var shared = openDb(file)) {
                // Collect keys first (forEachEntry doesn't support parallel)
                final var keys = new ArrayList<String>((int) shared.map.size());
                CHRONICLE_UTILS.safeForEachEntry(shared, entry -> keys.add(entry.key().get()));

                // Sequential insert to avoid MapDB concurrency issues
                keys.forEach(primaryKey -> keyMap.put(CHRONICLE_UTILS.to128BitHash(primaryKey),
                        new KeyMapValue(primaryKey, file)));
            }
        });
    }

    /**
     * Checks if this DAO requires a KeyMap for operations.
     * <p>
     * KeyMap is required when:
     * <ul>
     * <li>Multiple data files exist (need to look up which file contains a
     * key)</li>
     * <li>Indexes exist (indexes store hashes, so KeyMap is needed for
     * lookups)</li>
     * </ul>
     * When {@code false}, operations can skip hash calculations and KeyMap lookups,
     * directly accessing the single data file.
     * </p>
     *
     * @return {@code true} if KeyMap is needed, {@code false} for single-file
     *         no-index DAOs
     */
    private boolean hasKeyMap() {
        return getDataFileState().fileNames().size() > 1 || !indexFileNames().isEmpty();
    }

    /**
     * Initializes the directory structure and KeyMap for this DAO.
     * <p>
     * Creates the required subdirectories ({@code /data/}, {@code /indexes/},
     * {@code /files/}, {@code /backup/}) if they don't exist.
     * </p>
     * <p>
     * Also initializes the KeyMap if {@link #hasKeyMap()} returns {@code true}
     * and the KeyMap file doesn't exist. Skipped in recovery mode to avoid
     * deadlocks.
     * </p>
     * <p>
     * Call this method once during application startup for each DAO.
     * </p>
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
            final var keyMapPath = getKeyMapPath();
            // only if we have indexes/have multiple files otherwise hashes are not required
            // to be tracked
            if (hasKeyMap() && !Files.exists(Path.of(keyMapPath))) {
                try (final var sharedKeyMap = MAP_DB.openMap(keyMapPath, entries())) {
                    populateKeyMap(getDataFileState().fileNames(), sharedKeyMap.map);
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
     * Backs up all data files from {@code /data/} to {@code /backup/}.
     * <p>
     * Clears existing backup files first, then copies all current data files.
     * Does not backup indexes or KeyMap (these can be rebuilt).
     * </p>
     *
     * @throws IOException if file operations fail
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

    /**
     * Deletes all data files for this DAO.
     * <p>
     * <b>Warning:</b> This permanently deletes all data. Typically called
     * after {@link #backup()} during vacuum or truncate operations.
     * </p>
     */
    default void deleteDataFiles() {
        for (final var file : getDataFileState().fileNames()) {
            CHRONICLE_UTILS.deleteFileIfExists(dataPath() + DATA_DIR + file);
        }
    }

    /**
     * Deletes all secondary index files for this DAO.
     * <p>
     * Useful before bulk inserts - delete indexes, insert data, then
     * call {@link #refreshIndexes()} to rebuild. This is faster than
     * updating indexes incrementally during large batch operations.
     * </p>
     *
     * @return the set of index field names that were deleted
     */
    default Set<String> deleteIndexes() {
        final var available = indexFileNames();

        available.forEach(f -> {
            CHRONICLE_UTILS.deleteFileIfExists(dataPath() + INDEX_DIR + f);
        });

        return available;
    }

    /**
     * Returns the file path for a secondary index.
     *
     * @param field the field name that is indexed
     * @return the full path to the index file
     */
    default String getIndexPath(final String field) {
        return dataPath() + INDEX_DIR + field;
    }

    /**
     * Returns the current state of data files for this DAO.
     * <p>
     * Lazily initializes and caches the file state by scanning the data directory.
     * Returns the set of all data file names and the current active file for
     * writes.
     * </p>
     * <p>
     * File naming: {@code data} (first file), {@code data-2}, {@code data-3}, etc.
     * The current file is always the highest numbered file.
     * </p>
     *
     * @return the {@link DataFileState} containing file names and current file
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
        final int actualFiles = getDataFileState().fileNames().size();
        if (actualFiles <= 1) {
            return null;  // Single file - no vacuum needed
        }

        // Only count records if multiple files exist
        final int size = size();
        final long entriesPerFile = entries();
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
     * Builds secondary indexes for the specified fields from a single data file.
     *
     * @param db     the open ChronicleMap to index
     * @param fields the field names to create indexes for
     */
    private void initIndex(final SharedChronicleMap<String, V> db, final Set<String> fields) {
        CHRONICLE_UTILS.index(db, name(), fields, dataPath(), averageValue().getClass(), indexExclusions(), entries());
    }

    /**
     * Builds secondary indexes for the specified fields across all data files.
     * <p>
     * Processes all data files in parallel for efficiency.
     * </p>
     *
     * @param fields the field names to create indexes for
     */
    private void initIndex(final Set<String> fields) {
        CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
            try (final var shared = openDb(file)) {
                initIndex(shared, fields);
            }
        });
    }

    /**
     * Rebuilds all secondary indexes from scratch.
     * <p>
     * Deletes existing index files and recreates them by scanning all data files.
     * Acquires a write lock to prevent concurrent modifications.
     * </p>
     * <p>
     * Use this after bulk operations or when indexes may be out of sync.
     * </p>
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

    /**
     * Rebuilds the KeyMap from scratch by scanning all data files.
     * <p>
     * Deletes the existing KeyMap and repopulates it with all keys from all data
     * files.
     * Acquires a write lock to prevent concurrent modifications.
     * </p>
     * <p>
     * Use this when the KeyMap may be out of sync with the data files.
     * </p>
     */
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

    /**
     * Returns the list of index files that currently exist on disk.
     *
     * @return list of index file names
     * @throws IOException if directory listing fails
     */
    default List<String> availableIndexes() throws IOException {
        return CHRONICLE_UTILS.getFileList(dataPath() + INDEX_DIR);
    }

    /**
     * Synchronizes indexes with {@link #indexFileNames()} configuration.
     * <p>
     * Performs two operations:
     * <ul>
     * <li>Deletes stale indexes that exist on disk but are no longer in
     * indexFileNames()</li>
     * <li>Creates missing indexes that are in indexFileNames() but don't exist on
     * disk</li>
     * </ul>
     * This ensures that if an index is removed and later re-added, it will be
     * rebuilt fresh without stale data causing inconsistencies.
     * Called during application startup. Skipped in recovery mode to avoid
     * deadlocks.
     * </p>
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

                // Find stale indexes (exist on disk but no longer in indexFileNames)
                final Set<String> staleIndexes = new HashSet<>(availableIndexes);
                staleIndexes.removeAll(indexFileNames);
                if (!staleIndexes.isEmpty()) {
                    for (final var staleIndex : staleIndexes) {
                        final var indexPath = getIndexPath(staleIndex);
                        MAP_DB.closeIndex(indexPath);
                        CHRONICLE_UTILS.deleteFileIfExists(indexPath);
                    }
                    Logger.info("Deleted stale indexes {} at [{}]", staleIndexes, dataPath());
                }

                // Find missing indexes (in indexFileNames but not on disk)
                final Set<String> missingIndexes = new HashSet<>(indexFileNames);
                missingIndexes.removeAll(availableIndexes);
                if (!missingIndexes.isEmpty()) {
                    initIndex(missingIndexes);
                    Logger.info("Initialized {} indexes at [{}]", missingIndexes, dataPath());
                }
            }, "Init Indexes - " + dataPath()));
        }
    }

    /**
     * Attempts to recover a corrupted ChronicleMap data file.
     * <p>
     * Creates a backup of the corrupted file to {@code /backup/corrupted<filename>}
     * before attempting recovery. Uses ChronicleMap's built-in recovery mechanism.
     * </p>
     *
     * @param dataFileName the data file name to recover (e.g., "data", "data-2")
     * @throws IOException if backup or recovery fails
     */
    default void recoverData(final String dataFileName) throws IOException {
        final var dataFileStr = dataPath() + DATA_DIR + dataFileName;
        final var dataFilePath = Path.of(dataFileStr);

        // 1. Make a backup before recovery
        final var backupPath = Path.of(dataPath() + BACKUP_DIR + LOCKED_FILE + dataFileName);
        Files.copy(dataFilePath, backupPath, StandardCopyOption.REPLACE_EXISTING);
        Logger.info("Backed up file to {}", backupPath);
        try (final var db = CHRONICLE_DB.recoverDb(name(), entries(), averageKeySize(), averageValue(),
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
        return getDbFile(CHRONICLE_UTILS.to128BitHash(key));
    }

    /**
     * Looks up the database file name using a pre-calculated key hash.
     * Avoids re-hashing when the hash is already available.
     *
     * @param keyHash the 16-byte hash of the primary key
     * @return the database file name containing this key, or null if not found
     */
    private String getDbFile(final byte[] keyHash) {
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
     * Groups pre-calculated hashes by their file location using direct KeyMap
     * lookup.
     * No re-hashing required. Returns primary keys grouped by file.
     *
     * @param hashes the 16-byte hashes to look up
     * @return a {@link GroupedKeys} record (must be closed via try-with-resources)
     */
    private GroupedKeys getDbFiles(final Iterable<byte[]> hashes) {
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
                hashBatch.parallelStream().forEach(hash -> {
                    final var keyMapValue = sharedKeyMap.map.get(hash);
                    if (keyMapValue != null) {
                        fileBuffers.computeIfAbsent(keyMapValue.fileName(), k -> new ConcurrentLinkedQueue<>())
                                .add(keyMapValue.primaryKey());
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
    private GroupedKeys getDbFiles(final Iterable<byte[]> hashes, final int limit) {
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

                hashBatch.parallelStream().forEach(hash -> {
                    final var keyMapValue = sharedKeyMap.map.get(hash);
                    if (keyMapValue != null) {
                        totalFilled.incrementAndGet();
                        fileBuffers.computeIfAbsent(keyMapValue.fileName(), k -> new ConcurrentLinkedQueue<>())
                                .add(keyMapValue.primaryKey());
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
    private GroupedKeys getDbFiles(final Iterable<byte[]> hashes, final int limit,
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

                hashBatch.parallelStream().forEach(hash -> {
                    final var keyMapValue = sharedKeyMap.map.get(hash);
                    if (keyMapValue != null) {
                        final var primaryKey = keyMapValue.primaryKey();
                        // Skip excluded keys
                        if (excludedKeys != null && excludedKeys.contains(primaryKey)) {
                            return;
                        }
                        totalFilled.incrementAndGet();
                        fileBuffers.computeIfAbsent(keyMapValue.fileName(), k -> new ConcurrentLinkedQueue<>())
                                .add(primaryKey);
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
            keyHashMap.values().forEach(sharedKeyMap.map::remove);
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
            keyToFile.forEach((key, file) -> sharedKeyMap.map.put(
                    keyHashMap.get(key), new KeyMapValue(key, file)));
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
     * Fetches records from all data files up to the specified limit.
     * <p>
     * Iterates through data files sequentially and returns full entity objects.
     * For large datasets, prefer using search methods with indexes.
     * </p>
     *
     * @param limit maximum number of records to return
     * @return map of primary key to entity
     */
    default Map<String, V> fetch(final int limit) {
        final Map<String, V> result = new HashMap<>(limit);
        for (final String file : getDataFileState().fileNames()) {
            if (result.size() >= limit) {
                break;
            }
            try (final var shared = openDb(file)) {
                CHRONICLE_UTILS.safeForEachEntryWhile(shared, entry -> {
                    final var key = entry.key().get();
                    result.put(key, entry.value().getUsing(null));
                    return result.size() < limit;
                });
            }
        }
        Logger.debug("Fetched [{}] entries at [{}].", result.size(), dataPath());
        return result;
    }

    /**
     * Fetches records as CSV-formatted data up to the specified limit.
     * <p>
     * Returns data suitable for CSV export with headers and row arrays.
     * </p>
     *
     * @param limit maximum number of records to return
     * @return {@link CsvObject} containing headers and rows
     */
    default CsvObject fetchCsv(final int limit) {
        final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());
        final var headers = new AtomicReference<>(new String[0]);

        for (final String file : getDataFileState().fileNames()) {
            if (rowQueue.size() >= limit) {
                break;
            }
            try (final var shared = openDb(file)) {
                CHRONICLE_UTILS.safeForEachEntryWhile(shared, entry -> {
                    final var key = entry.key().get();
                    final var value = entry.value().getUsing(using());
                    if (headers.get().length == 0) {
                        headers.set(CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                    }
                    rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
                    return rowQueue.size() < limit;
                });
            }
        }
        Logger.debug("Fetched [{}] CSV entries at [{}].", rowQueue.size(), dataPath());

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    /**
     * Fetches only specified fields from records up to the limit.
     * <p>
     * More memory-efficient than {@link #fetch(int)} when only specific fields are
     * needed.
     * </p>
     *
     * @param fields the field names to include in the result
     * @param limit  maximum number of records to return
     * @return map of primary key to field-value map
     */
    default Map<String, Map<String, Object>> fetchSubset(final String[] fields, final int limit) {
        final var result = new ConcurrentHashMap<String, Map<String, Object>>(limit);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());
        for (final String file : getDataFileState().fileNames()) {
            if (result.size() >= limit) {
                break;
            }
            try (final var shared = openDb(file)) {
                CHRONICLE_UTILS.safeForEachEntryWhile(shared, entry -> {
                    result.put(entry.key().get(),
                            CHRONICLE_UTILS.getSubsetFromObject(classData, fields, entry.value().getUsing(using())));
                    return result.size() < limit;
                });
            }
        }
        Logger.debug("Fetched [{}] entries at [{}].", result.size(), dataPath());
        return result;
    }

    /**
     * Fetches only specified fields as CSV-formatted data up to the limit.
     *
     * @param fields the field names to include
     * @param limit  maximum number of records to return
     * @return {@link CsvObject} containing headers and rows
     */
    default CsvObject fetchSubsetCsv(final String[] fields, final int limit) {
        final String[] headers = CHRONICLE_UTILS.getCsvHeaders(fields);
        final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        for (final String file : getDataFileState().fileNames()) {
            if (rowQueue.size() >= limit) {
                break;
            }
            try (final var shared = openDb(file)) {
                CHRONICLE_UTILS.safeForEachEntryWhile(shared, entry -> {
                    rowQueue.add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, entry.key().get(), fields,
                            entry.value().getUsing(using())));
                    return rowQueue.size() < limit;
                });
            }
        }
        Logger.debug("Fetched subset CSV [{}] entries at [{}].", rowQueue.size(), dataPath());
        return new CsvObject(headers, new ArrayList<>(rowQueue));
    }

    /**
     * Fetches all records up to {@link #HARD_LIMIT}.
     *
     * @return map of primary key to entity
     * @see #fetch(int)
     */
    default Map<String, V> fetch() {
        return fetch(HARD_LIMIT);
    }

    /**
     * Fetches all records as CSV up to {@link #HARD_LIMIT}.
     *
     * @return {@link CsvObject} containing headers and rows
     * @see #fetchCsv(int)
     */
    default CsvObject fetchCsv() {
        return fetchCsv(HARD_LIMIT);
    }

    /**
     * Fetches specified fields from all records up to {@link #HARD_LIMIT}.
     *
     * @param fields the field names to include
     * @return map of primary key to field-value map
     * @see #fetchSubset(String[], int)
     */
    default Map<String, Map<String, Object>> fetchSubset(final String[] fields) {
        return fetchSubset(fields, HARD_LIMIT);
    }

    /**
     * Fetches specified fields as CSV from all records up to {@link #HARD_LIMIT}.
     *
     * @param fields the field names to include
     * @return {@link CsvObject} containing headers and rows
     * @see #fetchSubsetCsv(String[], int)
     */
    default CsvObject fetchSubsetCsv(final String[] fields) {
        return fetchSubsetCsv(fields, HARD_LIMIT);
    }

    /**
     * Fetches all primary keys from all data files.
     * <p>
     * Processes files in parallel for efficiency.
     * <b>Warning:</b> May be slow and memory-intensive for large datasets.
     * </p>
     *
     * @return set of all primary keys
     */
    default Set<String> fetchKeys() {
        final var result = ConcurrentHashMap.<String>newKeySet();
        CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
            try (final var shared = openDb(file)) {
                CHRONICLE_UTILS.safeForEachEntry(shared, entry -> result.add(entry.key().get()));
            }
        });
        Logger.debug("Fetched [{}] keys at [{}].", result.size(), dataPath());
        return result;
    }

    /**
     * Fetches all primary keys from all data files as a list.
     * <p>
     * Similar to {@link #fetchKeys()} but returns a {@link List} instead of a
     * {@link Set}.
     * Processes files in parallel using a {@link ConcurrentLinkedQueue} for
     * thread-safe collection.
     * </p>
     * <p>
     * <b>Warning:</b> May be slow and memory-intensive for large datasets.
     * </p>
     *
     * @return list of all primary keys (order is not guaranteed)
     * @see #fetchKeys()
     */
    default List<String> fetchKeysList() {
        final var result = new ConcurrentLinkedQueue<String>();
        CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
            try (final var shared = openDb(file)) {
                CHRONICLE_UTILS.safeForEachEntry(shared, entry -> result.add(entry.key().get()));
            }
        });
        Logger.debug("Fetched [{}] keys list at [{}].", result.size(), dataPath());
        return new ArrayList<>(result);
    }

    /**
     * Retrieves a single entity by its primary key.
     * <p>
     * For single-file DAOs, directly accesses the data file.
     * For multi-file DAOs, looks up the file location via KeyMap first.
     * </p>
     *
     * @param key the primary key
     * @return the entity, or {@code null} if not found
     */
    default V get(final String key) {
        if (key == null) {
            return null;
        }

        Logger.debug("Querying key [{}] at [{}].", key, dataPath());

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                return shared.map.get(key);
            }
        }

        final var file = getDbFile(key);
        if (file == null) {
            return null;
        }

        try (final var shared = openDb(file)) {
            return shared.map.get(key);
        }
    }

    /**
     * Processes keys grouped by their data file using pre-calculated hashes.
     * <p>
     * Groups keys by file location via {@link #getDbFiles(Iterable)}, then
     * processes
     * each file group in parallel. Within each file, keys are processed in parallel
     * using {@code parallelIterable}.
     * </p>
     *
     * @param hashes        pre-calculated 128-bit hashes of the keys
     * @param valueConsumer callback receiving (key, value) pairs for existing
     *                      records
     */
    private void processKeysByFileFromHashes(final Iterable<byte[]> hashes, final BiConsumer<String, V> valueConsumer) {
        try (final var grouped = getDbFiles(hashes)) {
            CHRONICLE_UTILS.processInParallel(grouped.fileGroups().entrySet(), entry -> {
                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), Integer.MAX_VALUE, key -> {
                        final var value = shared.map.get(key);
                        if (value != null) {
                            valueConsumer.accept(key, value);
                        }
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
     * Processes keys grouped by their data file.
     * <p>
     * Convenience wrapper that pre-calculates hashes and delegates to
     * {@link #processKeysByFileFromHashes(Iterable, BiConsumer)}.
     * </p>
     *
     * @param keys          the primary keys to process
     * @param valueConsumer callback receiving (key, value) pairs for existing
     *                      records
     */
    private void processKeysByFile(final Iterable<String> keys, final BiConsumer<String, V> valueConsumer) {
        processKeysByFileFromHashes(CHRONICLE_UTILS.preCalculateKeyHashes(keys), valueConsumer);
    }

    /**
     * Processes keys grouped by their data file using memory-efficient value reuse.
     * <p>
     * Similar to {@link #processKeysByFileFromHashes(Iterable, BiConsumer)} but
     * uses
     * {@code getUsing()} with a reusable value instance from {@link #using()}.
     * This reduces GC pressure for large batch operations.
     * </p>
     *
     * @param hashes        pre-calculated 128-bit hashes of the keys
     * @param valueConsumer callback receiving (key, value) pairs for existing
     *                      records
     */
    private void processKeysByFileUsingFromHashes(final Iterable<byte[]> hashes,
            final BiConsumer<String, V> valueConsumer) {
        try (final var grouped = getDbFiles(hashes)) {
            CHRONICLE_UTILS.processInParallel(grouped.fileGroups().entrySet(), entry -> {
                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), Integer.MAX_VALUE, key -> {
                        final var value = shared.map.getUsing(key, using());
                        if (value != null) {
                            valueConsumer.accept(key, value);
                        }
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
     * Processes keys grouped by their data file using memory-efficient value reuse.
     * <p>
     * Convenience wrapper that pre-calculates hashes and delegates to
     * {@link #processKeysByFileUsingFromHashes(Iterable, BiConsumer)}.
     * </p>
     *
     * @param keys          the primary keys to process
     * @param valueConsumer callback receiving (key, value) pairs for existing
     *                      records
     */
    private void processKeysByFileUsing(final Iterable<String> keys, final BiConsumer<String, V> valueConsumer) {
        processKeysByFileUsingFromHashes(CHRONICLE_UTILS.preCalculateKeyHashes(keys), valueConsumer);
    }

    /**
     * Processes keys grouped by their data file with a limit on total results.
     * <p>
     * Uses an {@link AtomicInteger} counter shared across parallel file processing
     * to enforce the limit. Processing stops early once the limit is reached.
     * </p>
     *
     * @param hashes        pre-calculated 128-bit hashes of the keys
     * @param limit         maximum number of records to process
     * @param valueConsumer callback receiving (key, value) pairs for existing
     *                      records
     */
    private void processKeysByFileFromHashes(final Iterable<byte[]> hashes, final int limit,
            final BiConsumer<String, V> valueConsumer) {
        try (final var grouped = getDbFiles(hashes, limit)) {
            final var counter = new AtomicInteger(0);
            CHRONICLE_UTILS.processInParallel(grouped.fileGroups().entrySet(), entry -> {
                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), limit, counter, key -> {
                        final var value = shared.map.get(key);
                        if (value != null) {
                            valueConsumer.accept(key, value);
                        }
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
     * Processes keys grouped by their data file with a result limit.
     * <p>
     * Convenience wrapper that pre-calculates hashes and delegates to
     * {@link #processKeysByFileFromHashes(Iterable, int, BiConsumer)}.
     * </p>
     *
     * @param keys          the primary keys to process
     * @param limit         maximum number of records to process
     * @param valueConsumer callback receiving (key, value) pairs for existing
     *                      records
     */
    private void processKeysByFile(final Iterable<String> keys, final int limit,
            final BiConsumer<String, V> valueConsumer) {
        processKeysByFileFromHashes(CHRONICLE_UTILS.preCalculateKeyHashes(keys), limit, valueConsumer);
    }

    /**
     * Processes keys grouped by their data file with limit and memory-efficient
     * value reuse.
     * <p>
     * Combines the limit enforcement with the memory-efficient {@code getUsing()}
     * pattern.
     * Uses an {@link AtomicInteger} counter shared across parallel file processing.
     * </p>
     *
     * @param hashes        pre-calculated 128-bit hashes of the keys
     * @param limit         maximum number of records to process
     * @param valueConsumer callback receiving (key, value) pairs for existing
     *                      records
     */
    private void processKeysByFileUsingFromHashes(final Iterable<byte[]> hashes, final int limit,
            final BiConsumer<String, V> valueConsumer) {
        try (final var grouped = getDbFiles(hashes, limit)) {
            final var counter = new AtomicInteger(0);
            CHRONICLE_UTILS.processInParallel(grouped.fileGroups().entrySet(), entry -> {
                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), limit, counter, key -> {
                        final var value = shared.map.getUsing(key, using());
                        if (value != null) {
                            valueConsumer.accept(key, value);
                        }
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
     * Processes keys grouped by their data file with limit and memory-efficient
     * value reuse.
     * <p>
     * Convenience wrapper that pre-calculates hashes and delegates to
     * {@link #processKeysByFileUsingFromHashes(Iterable, int, BiConsumer)}.
     * </p>
     *
     * @param keys          the primary keys to process
     * @param limit         maximum number of records to process
     * @param valueConsumer callback receiving (key, value) pairs for existing
     *                      records
     */
    private void processKeysByFileUsing(final Iterable<String> keys, final int limit,
            final BiConsumer<String, V> valueConsumer) {
        processKeysByFileUsingFromHashes(CHRONICLE_UTILS.preCalculateKeyHashes(keys), limit, valueConsumer);
    }

    /**
     * Processes keys grouped by their data file with limit and key exclusion.
     * <p>
     * Excluded keys are filtered out during hash pre-calculation, avoiding
     * unnecessary KeyMap lookups for keys that will be skipped.
     * </p>
     *
     * @param hashes        pre-calculated 128-bit hashes (already filtered for
     *                      exclusions)
     * @param limit         maximum number of records to process
     * @param excludedKeys  keys to skip (already filtered in hash calculation)
     * @param valueConsumer callback receiving (key, value) pairs for existing
     *                      records
     */
    private void processKeysByFileFromHashes(final Iterable<byte[]> hashes, final int limit,
            final Set<String> excludedKeys, final BiConsumer<String, V> valueConsumer) {
        try (final var grouped = getDbFiles(hashes, limit)) {
            final var counter = new AtomicInteger(0);
            CHRONICLE_UTILS.processInParallel(grouped.fileGroups().entrySet(), entry -> {
                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), limit, counter, key -> {
                        final var value = shared.map.get(key);
                        if (value != null) {
                            valueConsumer.accept(key, value);
                        }
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
     * Processes keys grouped by their data file with limit and key exclusion.
     * <p>
     * Pre-calculates hashes while filtering out excluded keys, then delegates to
     * {@link #processKeysByFileFromHashes(Iterable, int, Set, BiConsumer)}.
     * </p>
     *
     * @param keys          the primary keys to process
     * @param limit         maximum number of records to process
     * @param excludedKeys  keys to skip during processing
     * @param valueConsumer callback receiving (key, value) pairs for existing
     *                      records
     */
    private void processKeysByFile(final Iterable<String> keys, final int limit,
            final Set<String> excludedKeys, final BiConsumer<String, V> valueConsumer) {
        processKeysByFileFromHashes(CHRONICLE_UTILS.preCalculateKeyHashes(keys, excludedKeys), limit, excludedKeys,
                valueConsumer);
    }

    /**
     * Processes keys grouped by file with limit, exclusion, and memory-efficient
     * value reuse.
     * <p>
     * Combines all optimizations: key exclusion filtering, result limiting,
     * and the memory-efficient {@code getUsing()} pattern.
     * </p>
     *
     * @param hashes        pre-calculated 128-bit hashes (already filtered for
     *                      exclusions)
     * @param limit         maximum number of records to process
     * @param excludedKeys  keys to skip (already filtered in hash calculation)
     * @param valueConsumer callback receiving (key, value) pairs for existing
     *                      records
     */
    private void processKeysByFileUsingFromHashes(final Iterable<byte[]> hashes, final int limit,
            final Set<String> excludedKeys, final BiConsumer<String, V> valueConsumer) {
        try (final var grouped = getDbFiles(hashes, limit)) {
            final var counter = new AtomicInteger(0);
            CHRONICLE_UTILS.processInParallel(grouped.fileGroups().entrySet(), entry -> {
                final var file = entry.getKey();
                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(entry.getValue(), limit, counter, key -> {
                        final var value = shared.map.getUsing(key, using());
                        if (value != null) {
                            valueConsumer.accept(key, value);
                        }
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
     * Processes keys grouped by file with limit, exclusion, and memory-efficient
     * value reuse.
     * <p>
     * Pre-calculates hashes while filtering out excluded keys, then delegates to
     * {@link #processKeysByFileUsingFromHashes(Iterable, int, Set, BiConsumer)}.
     * </p>
     *
     * @param keys          the primary keys to process
     * @param limit         maximum number of records to process
     * @param excludedKeys  keys to skip during processing
     * @param valueConsumer callback receiving (key, value) pairs for existing
     *                      records
     */
    private void processKeysByFileUsing(final Iterable<String> keys, final int limit,
            final Set<String> excludedKeys, final BiConsumer<String, V> valueConsumer) {
        processKeysByFileUsingFromHashes(CHRONICLE_UTILS.preCalculateKeyHashes(keys, excludedKeys), limit, excludedKeys,
                valueConsumer);
    }

    /**
     * Processes keys directly from the single data file.
     * <p>
     * Used for single-file DAOs (when {@code fileNames().size() <= 1}).
     * Avoids the overhead of file grouping and KeyMap lookups.
     * </p>
     *
     * @param keys          the primary keys to process
     * @param valueConsumer callback receiving (key, value) pairs for existing
     *                      records
     */
    private void processKeys(final Iterable<String> keys, final BiConsumer<String, V> valueConsumer) {
        try (final var shared = openDb()) {
            CHRONICLE_UTILS.parallelIterable(keys, Integer.MAX_VALUE, key -> {
                final var value = shared.map.get(key);
                if (value != null) {
                    valueConsumer.accept(key, value);
                }
            });
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Processes keys directly from the single data file with memory-efficient value
     * reuse.
     * <p>
     * Used for single-file DAOs. Uses {@code getUsing()} with a reusable value
     * instance
     * to reduce GC pressure for large batch operations.
     * </p>
     *
     * @param keys          the primary keys to process
     * @param valueConsumer callback receiving (key, value) pairs for existing
     *                      records
     */
    private void processKeysUsing(final Iterable<String> keys, final BiConsumer<String, V> valueConsumer) {
        try (final var shared = openDb()) {
            CHRONICLE_UTILS.parallelIterable(keys, Integer.MAX_VALUE, key -> {
                final var value = shared.map.getUsing(key, using());
                if (value != null) {
                    valueConsumer.accept(key, value);
                }
            });
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Processes keys directly from the single data file with a result limit.
     * <p>
     * Used for single-file DAOs. Stops processing early once the limit is reached.
     * </p>
     *
     * @param keys          the primary keys to process
     * @param limit         maximum number of records to process
     * @param valueConsumer callback receiving (key, value) pairs for existing
     *                      records
     */
    private void processKeys(final Iterable<String> keys, final int limit, final BiConsumer<String, V> valueConsumer) {
        try (final var shared = openDb()) {
            CHRONICLE_UTILS.parallelIterable(keys, limit, key -> {
                final var value = shared.map.get(key);
                if (value != null) {
                    valueConsumer.accept(key, value);
                }
            });
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Processes keys directly from the single data file with limit and
     * memory-efficient value reuse.
     * <p>
     * Combines result limiting with the memory-efficient {@code getUsing()} pattern
     * for single-file DAOs.
     * </p>
     *
     * @param keys          the primary keys to process
     * @param limit         maximum number of records to process
     * @param valueConsumer callback receiving (key, value) pairs for existing
     *                      records
     */
    private void processKeysUsing(final Iterable<String> keys, final int limit,
            final BiConsumer<String, V> valueConsumer) {
        try (final var shared = openDb()) {
            CHRONICLE_UTILS.parallelIterable(keys, limit, key -> {
                final var value = shared.map.getUsing(key, using());
                if (value != null) {
                    valueConsumer.accept(key, value);
                }
            });
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Processes keys directly from the single data file with limit and key
     * exclusion.
     * <p>
     * For single-file DAOs. Skips keys present in the exclusion set and stops
     * once the limit is reached.
     * </p>
     *
     * @param keys          the primary keys to process
     * @param limit         maximum number of records to process
     * @param excludedKeys  keys to skip during processing
     * @param valueConsumer callback receiving (key, value) pairs for existing
     *                      records
     */
    private void processKeys(final Iterable<String> keys, final int limit, final Set<String> excludedKeys,
            final BiConsumer<String, V> valueConsumer) {
        try (final var shared = openDb()) {
            CHRONICLE_UTILS.parallelIterable(keys, limit, key -> {
                if (!excludedKeys.contains(key)) {
                    final var value = shared.map.get(key);
                    if (value != null) {
                        valueConsumer.accept(key, value);
                    }
                }
            });
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Processes keys directly from the single data file with limit, exclusion, and
     * memory reuse.
     * <p>
     * For single-file DAOs. Combines all optimizations: key exclusion, result
     * limiting,
     * and the memory-efficient {@code getUsing()} pattern.
     * </p>
     *
     * @param keys          the primary keys to process
     * @param limit         maximum number of records to process
     * @param excludedKeys  keys to skip during processing
     * @param valueConsumer callback receiving (key, value) pairs for existing
     *                      records
     */
    private void processKeysUsing(final Iterable<String> keys, final int limit, final Set<String> excludedKeys,
            final BiConsumer<String, V> valueConsumer) {
        try (final var shared = openDb()) {
            CHRONICLE_UTILS.parallelIterable(keys, limit, key -> {
                if (!excludedKeys.contains(key)) {
                    final var value = shared.map.getUsing(key, using());
                    if (value != null) {
                        valueConsumer.accept(key, value);
                    }
                }
            });
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieves values for multiple keys in batch.
     * <p>
     * For single-file DAOs, uses direct lookup via
     * {@link #processKeys(Iterable, BiConsumer)}.
     * For multi-file DAOs, groups keys by file via
     * {@link #processKeysByFile(Iterable, BiConsumer)}
     * to minimize file opens.
     * </p>
     *
     * @param keys the primary keys to retrieve
     * @return map of key to value for existing records (missing keys are omitted)
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

    /**
     * Retrieves values for multiple keys as CSV format.
     * <p>
     * Uses memory-efficient {@code getUsing()} pattern for processing.
     * Headers are extracted from the first encountered value.
     * </p>
     *
     * @param keys the primary keys to retrieve
     * @return {@link CsvObject} containing headers and rows for existing records
     */
    default CsvObject getCsv(final Iterable<String> keys) {
        if (keys == null || !keys.iterator().hasNext()) {
            return CsvObject.empty();
        }

        Logger.debug("Querying multiple keys for CSV at [{}].", dataPath());
        final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
        final var headers = new AtomicReference<>(new String[0]);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        if (getDataFileState().fileNames().size() <= 1) {
            processKeysUsing(keys, (key, value) -> {
                if (headers.get().length == 0) {
                    headers.set(CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                }
                rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
            });

            return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
        }

        processKeysByFileUsing(keys, (key, value) -> {
            if (headers.get().length == 0) {
                headers.set(CHRONICLE_UTILS.getHeadersFromObject(classData, value));
            }
            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
        });

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    /**
     * Retrieves specified fields for multiple keys in batch.
     * <p>
     * Uses memory-efficient {@code getUsing()} pattern. Only the specified fields
     * are extracted from each record.
     * </p>
     *
     * @param keys   the primary keys to retrieve
     * @param fields the field names to include in the result
     * @return map of key to field-value map for existing records
     */
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

    /**
     * Retrieves specified fields for multiple keys as CSV format.
     * <p>
     * Combines field projection with CSV formatting. Uses memory-efficient
     * {@code getUsing()} pattern.
     * </p>
     *
     * @param keys   the primary keys to retrieve
     * @param fields the field names to include
     * @return {@link CsvObject} containing headers and rows for existing records
     */
    default CsvObject getSubsetCsv(final Iterable<String> keys, final String[] fields) {
        if (keys == null || !keys.iterator().hasNext()) {
            return CsvObject.empty();
        }

        Logger.debug("Querying subset CSV multiple keys at [{}].", dataPath());
        final String[] headers = CHRONICLE_UTILS.getCsvHeaders(fields);
        final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
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

    /**
     * Retrieves values for multiple keys with a result limit.
     * <p>
     * Processing stops early once the limit is reached. Useful for paginated
     * retrieval or when only a subset of results is needed.
     * </p>
     *
     * @param keys  the primary keys to retrieve
     * @param limit maximum number of records to return
     * @return map of key to value for existing records (up to limit)
     */
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

    /**
     * Retrieves values for multiple keys as CSV format with a result limit.
     * <p>
     * Uses memory-efficient {@code getUsing()} pattern. Processing stops
     * early once the limit is reached.
     * </p>
     *
     * @param keys  the primary keys to retrieve
     * @param limit maximum number of records to return
     * @return {@link CsvObject} containing headers and rows (up to limit)
     */
    default CsvObject getCsv(final Iterable<String> keys, final int limit) {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.debug("Querying multiple keys at [{}] with limit [{}].", dataPath(), limit);
        final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());
        final var headers = new AtomicReference<>(new String[0]);

        if (getDataFileState().fileNames().size() <= 1) {
            processKeysUsing(keys, limit, (key, value) -> {
                if (headers.get().length == 0) {
                    headers.set(CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                }
                rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
            });

            return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
        }

        processKeysByFileUsing(keys, limit, (key, value) -> {
            if (headers.get().length == 0) {
                headers.set(CHRONICLE_UTILS.getHeadersFromObject(classData, value));
            }
            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
        });

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    /**
     * Retrieves specified fields for multiple keys with a result limit.
     * <p>
     * Combines field projection with result limiting. Processing stops early
     * once the limit is reached.
     * </p>
     *
     * @param keys   the primary keys to retrieve
     * @param fields the field names to include
     * @param limit  maximum number of records to return
     * @return map of key to field-value map (up to limit)
     */
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

    /**
     * Retrieves specified fields for multiple keys as CSV format with a result
     * limit.
     * <p>
     * Combines field projection, CSV formatting, and result limiting.
     * Processing stops early once the limit is reached.
     * </p>
     *
     * @param keys   the primary keys to retrieve
     * @param fields the field names to include
     * @param limit  maximum number of records to return
     * @return {@link CsvObject} containing headers and rows (up to limit)
     */
    default CsvObject getSubsetCsv(final Iterable<String> keys, final String[] fields, final int limit) {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.debug("Querying subset CSV multiple keys at [{}] with limit [{}].", dataPath(), limit);
        final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
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

    /**
     * Retrieves values using pre-calculated 128-bit hashes.
     * <p>
     * Optimized for cases where hashes were already computed (e.g., from a search).
     * Avoids the overhead of re-calculating hashes from keys.
     * </p>
     *
     * @param hashes pre-calculated 128-bit hashes
     * @param limit  maximum number of records to return
     * @return map of key to value for existing records (up to limit)
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
     * Retrieves specified fields using pre-calculated 128-bit hashes.
     * <p>
     * Optimized for cases where hashes were already computed. Combines hash-based
     * lookup with field projection.
     * </p>
     *
     * @param hashes pre-calculated 128-bit hashes
     * @param fields the field names to include
     * @param limit  maximum number of records to return
     * @return map of key to field-value map (up to limit)
     */
    default Map<String, Map<String, Object>> getSubsetFromHashes(final Iterable<byte[]> hashes,
            final String[] fields, final int limit) {
        if (hashes == null || !hashes.iterator().hasNext() || limit <= 0) {
            return Collections.emptyMap();
        }

        Logger.debug("Querying subset by hashes at [{}] with limit [{}].", dataPath(), limit);
        final var map = new ConcurrentHashMap<String, Map<String, Object>>(limit);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        processKeysByFileUsingFromHashes(hashes, limit,
                (key, value) -> map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value)));

        return map;
    }

    /**
     * Retrieves values as CSV format using pre-calculated 128-bit hashes.
     * <p>
     * Optimized for cases where hashes were already computed. Combines hash-based
     * lookup with CSV formatting.
     * </p>
     *
     * @param hashes pre-calculated 128-bit hashes
     * @param limit  maximum number of records to return
     * @return {@link CsvObject} containing headers and rows (up to limit)
     */
    default CsvObject getCsvFromHashes(final Iterable<byte[]> hashes, final int limit) {
        if (hashes == null || !hashes.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.debug("Querying CSV by hashes at [{}] with limit [{}].", dataPath(), limit);
        final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());
        final var headers = new AtomicReference<>(new String[0]);

        processKeysByFileUsingFromHashes(hashes, limit, (key, value) -> {
            if (headers.get().length == 0) {
                headers.set(CHRONICLE_UTILS.getHeadersFromObject(classData, value));
            }
            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
        });

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    /**
     * Retrieves specified fields as CSV format using pre-calculated 128-bit hashes.
     * <p>
     * Combines hash-based lookup, field projection, and CSV formatting.
     * </p>
     *
     * @param hashes pre-calculated 128-bit hashes
     * @param fields the field names to include
     * @param limit  maximum number of records to return
     * @return {@link CsvObject} containing headers and rows (up to limit)
     */
    default CsvObject getSubsetCsvFromHashes(final Iterable<byte[]> hashes, final String[] fields, final int limit) {
        if (hashes == null || !hashes.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.debug("Querying subset CSV by hashes at [{}] with limit [{}].", dataPath(), limit);
        final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
        final var headers = CHRONICLE_UTILS.getCsvHeaders(fields);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        processKeysByFileUsingFromHashes(hashes, limit,
                (key, value) -> rowQueue.add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value)));

        return new CsvObject(headers, new ArrayList<>(rowQueue));
    }

    /**
     * Converts pre-calculated 128-bit hashes back to their string keys.
     * <p>
     * Performs KeyMap lookups to retrieve the original keys. Useful when you've
     * been working with hashes (e.g., from index operations) and need the actual
     * key strings.
     * </p>
     *
     * @param hashes pre-calculated 128-bit hashes
     * @return set of string keys corresponding to the hashes
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
     * Converts pre-calculated 128-bit hashes back to their string keys as a list.
     * <p>
     * Similar to {@link #toSetOfKeysFromHashes(Iterable)} but returns a
     * {@link List}.
     * Uses a {@link ConcurrentLinkedQueue} internally for thread-safe collection.
     * </p>
     *
     * @param hashes pre-calculated 128-bit hashes
     * @return list of string keys corresponding to the hashes (order is not
     *         guaranteed)
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

    /**
     * Retrieves values using pre-calculated hashes with key exclusion.
     * <p>
     * Combines hash-based lookup with key exclusion filtering.
     * Excluded keys are skipped during processing.
     * </p>
     *
     * @param hashes       pre-calculated 128-bit hashes
     * @param excludedKeys keys to skip during processing
     * @param limit        maximum number of records to return
     * @return map of key to value for existing records (up to limit, excluding
     *         specified keys)
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
     * Retrieves values as CSV format using pre-calculated hashes with key
     * exclusion.
     * <p>
     * Combines hash-based lookup, CSV formatting, and key exclusion.
     * </p>
     *
     * @param hashes       pre-calculated 128-bit hashes
     * @param excludedKeys keys to skip during processing
     * @param limit        maximum number of records to return
     * @return {@link CsvObject} containing headers and rows (up to limit)
     */
    default CsvObject getCsvFromHashes(final Iterable<byte[]> hashes, final Set<String> excludedKeys,
            final int limit) {
        if (hashes == null || !hashes.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.debug("Querying CSV by hashes at [{}] with limit [{}] and excluded keys.", dataPath(), limit);
        final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());
        final var headers = new AtomicReference<>(new String[0]);

        processKeysByFileUsingFromHashes(hashes, limit, excludedKeys, (key, value) -> {
            if (headers.get().length == 0) {
                headers.set(CHRONICLE_UTILS.getHeadersFromObject(classData, value));
            }
            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
        });

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    /**
     * Retrieves specified fields using pre-calculated hashes with key exclusion.
     * <p>
     * Combines hash-based lookup, field projection, and key exclusion.
     * </p>
     *
     * @param hashes       pre-calculated 128-bit hashes
     * @param excludedKeys keys to skip during processing
     * @param fields       the field names to include
     * @param limit        maximum number of records to return
     * @return map of key to field-value map (up to limit)
     */
    default Map<String, Map<String, Object>> getSubsetFromHashes(final Iterable<byte[]> hashes,
            final Set<String> excludedKeys, final String[] fields, final int limit) {
        if (hashes == null || !hashes.iterator().hasNext() || limit <= 0) {
            return Collections.emptyMap();
        }

        Logger.debug("Querying subset by hashes at [{}] with limit [{}] and excluded keys.", dataPath(), limit);
        final var map = new ConcurrentHashMap<String, Map<String, Object>>(limit);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());
        processKeysByFileUsingFromHashes(hashes, limit, excludedKeys,
                (key, value) -> map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, fields, value)));
        return map;
    }

    /**
     * Retrieves specified fields as CSV format using pre-calculated hashes with key
     * exclusion.
     * <p>
     * Combines hash-based lookup, field projection, CSV formatting, and key
     * exclusion.
     * </p>
     *
     * @param hashes       pre-calculated 128-bit hashes
     * @param excludedKeys keys to skip during processing
     * @param fields       the field names to include
     * @param limit        maximum number of records to return
     * @return {@link CsvObject} containing headers and rows (up to limit)
     */
    default CsvObject getSubsetCsvFromHashes(final Iterable<byte[]> hashes, final Set<String> excludedKeys,
            final String[] fields, final int limit) {
        if (hashes == null || !hashes.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.debug("Querying subset CSV by hashes at [{}] with limit [{}] and excluded keys.", dataPath(), limit);
        final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
        final var headers = CHRONICLE_UTILS.getCsvHeaders(fields);
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());

        processKeysByFileUsingFromHashes(hashes, limit, excludedKeys,
                (key, value) -> rowQueue.add(CHRONICLE_UTILS.getSubsetRowFromObject(classData, key, fields, value)));

        return new CsvObject(headers, new ArrayList<>(rowQueue));
    }

    /**
     * Retrieves values for multiple keys with exclusion and result limit.
     * <p>
     * Skips keys present in the exclusion set. Processing stops early once
     * the limit is reached.
     * </p>
     *
     * @param keys         the primary keys to retrieve
     * @param excludedKeys keys to skip during processing
     * @param limit        maximum number of records to return
     * @return map of key to value (up to limit, excluding specified keys)
     */
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

    /**
     * Retrieves values as CSV format with exclusion and result limit.
     * <p>
     * Combines CSV formatting, key exclusion, and result limiting.
     * </p>
     *
     * @param keys         the primary keys to retrieve
     * @param excludedKeys keys to skip during processing
     * @param limit        maximum number of records to return
     * @return {@link CsvObject} containing headers and rows (up to limit)
     */
    default CsvObject getCsv(final Iterable<String> keys, final Set<String> excludedKeys, final int limit) {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.debug("Querying multiple keys at [{}] with limit [{}] and excluded keys.", dataPath(), limit);
        final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());
        final var headers = new AtomicReference<>(new String[0]);

        if (getDataFileState().fileNames().size() <= 1) {
            processKeysUsing(keys, limit, excludedKeys, (key, value) -> {
                if (headers.get().length == 0) {
                    headers.set(CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                }
                rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
            });

            return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
        }

        processKeysByFileUsing(keys, limit, excludedKeys, (key, value) -> {
            if (headers.get().length == 0) {
                headers.set(CHRONICLE_UTILS.getHeadersFromObject(classData, value));
            }
            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
        });

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    /**
     * Retrieves specified fields with exclusion and result limit.
     * <p>
     * Combines field projection, key exclusion, and result limiting.
     * </p>
     *
     * @param keys         the primary keys to retrieve
     * @param excludedKeys keys to skip during processing
     * @param fields       the field names to include
     * @param limit        maximum number of records to return
     * @return map of key to field-value map (up to limit)
     */
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

    /**
     * Retrieves specified fields as CSV format with exclusion and result limit.
     * <p>
     * Combines field projection, CSV formatting, key exclusion, and result
     * limiting.
     * </p>
     *
     * @param keys         the primary keys to retrieve
     * @param excludedKeys keys to skip during processing
     * @param fields       the field names to include
     * @param limit        maximum number of records to return
     * @return {@link CsvObject} containing headers and rows (up to limit)
     */
    default CsvObject getSubsetCsv(final Iterable<String> keys, final Set<String> excludedKeys, final String[] fields,
            final int limit) {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.debug("Querying subset CSV multiple keys at [{}] with limit [{}] and excluded keys.", dataPath(), limit);
        final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
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
     * Deletes a single entity by its primary key.
     * <p>
     * For single-file DAOs without indexes, directly removes from the data file.
     * For multi-file or indexed DAOs, also updates the KeyMap and indexes.
     * </p>
     *
     * @param key the primary key to delete
     * @return {@code true} if the key existed and was deleted, {@code false}
     *         otherwise
     */
    default boolean delete(final String key) {
        if (key == null) {
            return false;
        }

        if (!hasKeyMap()) {
            return CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
                V deletedValue = null;
                try (final var shared = openDb()) {
                    deletedValue = shared.map.remove(key);
                }

                if (deletedValue != null) {
                    Logger.info("Deleted using key [{}] at [{}].", key, dataPath());
                    return true;
                }

                return false;
            });
        }

        // Pre-calculate hash ONCE before all operations
        final byte[] keyHash = CHRONICLE_UTILS.to128BitHash(key);
        final var keyHashMap = Map.of(key, keyHash);

        // STEP 0: Slow lookup outside the lock using pre-calculated hash
        final var file = getDbFile(keyHash);
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

    /**
     * Deletes multiple entities by their primary keys.
     * <p>
     * For single-file DAOs without indexes, directly removes from the data file.
     * For multi-file or indexed DAOs, groups keys by file, performs deletions
     * in parallel, then updates KeyMap and indexes.
     * </p>
     *
     * @param keys the primary keys to delete
     * @return {@code true} if at least one key was deleted, {@code false} otherwise
     */
    default boolean delete(final Iterable<String> keys) {
        if (keys == null || !keys.iterator().hasNext()) {
            return false;
        }

        final var deletedMap = new ConcurrentHashMap<String, V>(1000);
        if (!hasKeyMap()) {
            return CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
                try (final var shared = openDb()) {
                    CHRONICLE_UTILS.parallelIterable(keys, Integer.MAX_VALUE, key -> {
                        final var deleted = shared.map.remove(key);
                        if (deleted != null) {
                            deletedMap.put(key, deleted);
                        }
                    });
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }

                return !deletedMap.isEmpty();
            });
        }

        // Pre-calculate hashes ONCE before all operations
        final var keyHashMap = CHRONICLE_UTILS.preCalculateKeyHashMap(keys);
        // STEP 0: Group keys by file using pre-calculated hashes
        final var grouped = getDbFiles(keyHashMap.values());
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
                            }
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
                tasks.add(() -> CHRONICLE_UTILS.removeFromIndex(name(), dataPath(), indexFileNames(), deletedMap,
                        averageValue().getClass(), keyHashMap));
                CHRONICLE_UTILS.processInParallel(tasks);
                Logger.info("Deleted [{}] records at [{}].", deletedMap.size(), dataPath());
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

    /**
     * Checks if the current data file is full and rotates to a new file if needed.
     * <p>
     * When rotation occurs:
     * <ol>
     * <li>Populates the KeyMap with all keys from the current file (first rotation
     * only)</li>
     * <li>Closes the current file</li>
     * <li>Creates a new data file</li>
     * <li>Updates the file state cache</li>
     * </ol>
     * </p>
     *
     * @param shared the current data file handle
     * @return the same handle if not full, or a new handle to the rotated file
     */
    private SharedChronicleMap<String, V> checkAndRotate(final SharedChronicleMap<String, V> shared) {
        if (shared.map.size() >= entries()) {
            final var dataFileState = getDataFileState();
            final String newFile = "data-" + (dataFileState.fileNames().size() + 1);
            // Update key map using the provided ChronicleMap
            try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath(), entries())) {
                CHRONICLE_UTILS.safeForEachEntry(shared, entry -> {
                    final var key = entry.key().get();
                    sharedKeyMap.map.put(CHRONICLE_UTILS.to128BitHash(key),
                            new KeyMapValue(key, dataFileState.currentFile()));
                });
            }
            shared.close();
            dataFileState.fileNames().add(newFile);
            Logger.info("Rotated file [{}] at [{}].", dataFileState.currentFile(), dataPath());
            DATA_FILE_CACHE.put(dataPath(), dataFileState.withCurrentFile(newFile));
            return openDb(newFile); // open new db
        }
        return shared;
    }

    /**
     * Checks if the current data file is full and rotates to a new file if needed.
     * <p>
     * Optimized version for batch operations that maintains a pending KeyMap update
     * buffer.
     * When rotation occurs, flushes the pending updates to the KeyMap before
     * rotating.
     * </p>
     *
     * @param shared       the current data file handle
     * @param keyMapUpdate buffer of key-to-file mappings pending KeyMap update
     *                     (cleared after flush)
     * @param keyHashMap   map of keys to their pre-calculated 128-bit hashes
     * @return the same handle if not full, or a new handle to the rotated file
     */
    private SharedChronicleMap<String, V> checkAndRotate(final SharedChronicleMap<String, V> shared,
            final Map<String, String> keyMapUpdate, final Map<String, byte[]> keyHashMap) {
        if (shared.map.size() >= entries()) {
            final var dataFileState = getDataFileState();
            final String newFile = "data-" + (dataFileState.fileNames().size() + 1);
            // add the new keys to map
            addAllToKeyMap(keyMapUpdate, keyHashMap);
            shared.close();
            dataFileState.fileNames().add(newFile);
            Logger.info("Rotated file [{}] at [{}].", dataFileState.currentFile(), dataPath());
            DATA_FILE_CACHE.put(dataPath(), dataFileState.withCurrentFile(newFile));
            keyMapUpdate.clear();
            return openDb(newFile); // open new db
        }
        return shared;
    }

    /**
     * Inserts or updates a single entity (upsert operation).
     * <p>
     * If the key exists, updates the value and returns {@link PutStatus#UPDATED}.
     * If the key doesn't exist, inserts it and returns {@link PutStatus#INSERTED}.
     * Handles file rotation automatically when the current file is full.
     * </p>
     * <p>
     * For single-file DAOs without indexes, skips hash calculation and KeyMap
     * operations.
     * For multi-file or indexed DAOs, updates KeyMap and indexes accordingly.
     * </p>
     *
     * @param key            the primary key
     * @param value          the entity to store
     * @param indexFileNames the index fields to update (use
     *                       {@link #indexFileNames()})
     * @return {@link PutStatus#INSERTED}, {@link PutStatus#UPDATED}, or
     *         {@link PutStatus#FAILED}
     */
    default PutStatus put(final String key, final V value, final Set<String> indexFileNames) {
        if (key == null) {
            return PutStatus.FAILED;
        }

        // Pre-calculate hash ONCE before all operations
        final byte[] keyHash = CHRONICLE_UTILS.to128BitHash(key);

        if (!hasKeyMap()) {
            return CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
                var shared = openDb(); // no try with resource here for rotation
                try {
                    if (!shared.map.containsKey(key)) {
                        shared = checkAndRotate(shared);
                    }

                    final var prevValue = shared.map.put(key, value);
                    if (hasKeyMap()) {
                        addToKeyMap(key, getDataFileState().currentFile(), keyHash);
                    }
                    // since initially hasKeyMap was false, it means there was no indexes to update
                    // even if rotataion happened.
                    if (prevValue != null) {
                        Logger.info("Updated using key [{}] at [{}].", key, dataPath());
                        return PutStatus.UPDATED;
                    }

                    Logger.info("Inserted using key [{}] at [{}].", key, dataPath());
                    return PutStatus.INSERTED;
                } finally {
                    shared.close();
                }
            });
        }

        final var keyHashMap = Map.of(key, keyHash);

        // 1. Prepare Index (OUTSIDE LOCK)
        final var preparedIndex = CHRONICLE_UTILS.prepareIndexAdditions(indexFileNames, Map.of(key, value),
                averageValue().getClass(), indexExclusions(), keyHashMap);

        // 2. PRE-LOCK: Look in MapDB using pre-calculated hash
        final var initialFile = getDbFile(keyHash);

        // 3. Lock short time
        return CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
            String file;
            String capturedFile = null;
            // If key wasnt found and another thread beat current one in the insert
            if (initialFile == null) {
                file = getDbFile(keyHash);
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
     * Inserts or updates a single entity using default indexes.
     *
     * @param key   the primary key
     * @param value the entity to store
     * @return {@link PutStatus#INSERTED}, {@link PutStatus#UPDATED}, or
     *         {@link PutStatus#FAILED}
     * @see #put(String, Object, Set)
     */
    default PutStatus put(final String key, final V value) {
        return put(key, value, indexFileNames());
    }

    /**
     * Updates an existing entity without file rotation.
     * <p>
     * Unlike {@link #put}, this method requires the key to already exist and will
     * fail if the key is not found. Does not perform file rotation checks.
     * </p>
     * <p>
     * For single-file DAOs without indexes, skips hash calculation and KeyMap
     * lookup.
     * For multi-file or indexed DAOs, looks up the file location and updates
     * indexes.
     * </p>
     *
     * @param key            the primary key (must already exist)
     * @param value          the new value
     * @param indexFileNames the index fields to update
     * @return {@link PutStatus#UPDATED} on success, {@link PutStatus#FAILED} if key
     *         not found
     */
    default PutStatus update(final String key, final V value, final Set<String> indexFileNames) {
        if (key == null) {
            return PutStatus.FAILED;
        }

        if (!hasKeyMap()) {
            return CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
                try (final var shared = openDb()) {
                    if (shared.map.containsKey(key)) {
                        shared.map.put(key, value);
                        Logger.info("Updated using key [{}] at [{}].", key, dataPath());
                        return PutStatus.UPDATED;
                    }
                }

                return PutStatus.FAILED;
            });
        }

        // Pre-calculate hash ONCE before all operations
        final byte[] keyHash = CHRONICLE_UTILS.to128BitHash(key);
        final var keyHashMap = Map.of(key, keyHash);

        // 1. Prepare Index (OUTSIDE LOCK)
        final var preparedIndex = CHRONICLE_UTILS.prepareIndexAdditions(indexFileNames, Map.of(key, value),
                averageValue().getClass(), indexExclusions(), keyHashMap);

        // 2. PRE-LOCK: Look in MapDB using pre-calculated hash
        final var file = getDbFile(keyHash);
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
     * Updates an existing entity using default indexes.
     *
     * @param key   the primary key (must already exist)
     * @param value the new value
     * @return {@link PutStatus#UPDATED} on success, {@link PutStatus#FAILED} if key
     *         not found
     * @see #update(String, Object, Set)
     */
    default PutStatus update(final String key, final V value) {
        return update(key, value, indexFileNames());
    }

    /**
     * Inserts a new entity (fails if key already exists).
     * <p>
     * Unlike {@link #put}, this method requires the key to NOT exist and will
     * fail if the key is found. Handles file rotation automatically when needed.
     * </p>
     * <p>
     * For single-file DAOs without indexes, skips KeyMap operations.
     * For multi-file or indexed DAOs, updates KeyMap and indexes.
     * </p>
     *
     * @param key            the primary key (must NOT already exist)
     * @param value          the entity to store
     * @param indexFileNames the index fields to update
     * @return {@link PutStatus#INSERTED} on success, {@link PutStatus#FAILED} if
     *         key exists
     */
    default PutStatus insert(final String key, final V value, final Set<String> indexFileNames) {
        if (key == null) {
            return PutStatus.FAILED;
        }

        // Pre-calculate hash ONCE before all operations
        final byte[] keyHash = CHRONICLE_UTILS.to128BitHash(key);
        if (!hasKeyMap()) {
            return CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
                var shared = openDb();
                try {
                    if (!shared.map.containsKey(key)) {
                        shared = checkAndRotate(shared);
                        shared.map.put(key, value);
                        if (hasKeyMap()) {
                            addToKeyMap(key, getDataFileState().currentFile(), keyHash);
                        }
                        Logger.info("Inserted using key [{}] at [{}].", key, dataPath());
                        return PutStatus.INSERTED;
                    }
                    return PutStatus.FAILED;
                } finally {
                    shared.close();
                }
            });
        }

        final var keyHashMap = Map.of(key, keyHash);

        // 1. Prepare Index (OUTSIDE LOCK)
        final var preparedIndex = CHRONICLE_UTILS.prepareIndexAdditions(indexFileNames, Map.of(key, value),
                averageValue().getClass(), indexExclusions(), keyHashMap);

        return CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
            final var file = getDbFile(keyHash);
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
     * Inserts a new entity using default indexes.
     *
     * @param key   the primary key (must NOT already exist)
     * @param value the entity to store
     * @return {@link PutStatus#INSERTED} on success, {@link PutStatus#FAILED} if
     *         key exists
     * @see #insert(String, Object, Set)
     */
    default PutStatus insert(final String key, final V value) {
        return insert(key, value, indexFileNames());
    }

    /**
     * Inserts or updates multiple entities in batch (upsert operation).
     * <p>
     * Separates existing keys (for update) from new keys (for insert) to optimize
     * processing. Handles file rotation automatically for inserts.
     * </p>
     * <p>
     * For single-file DAOs without indexes, skips hash calculation and index
     * operations.
     * For multi-file or indexed DAOs, updates KeyMap and indexes in parallel.
     * </p>
     *
     * @param map the key-value pairs to store
     * @return {@link PutStatus#INSERTED} if all were new, {@link PutStatus#UPDATED}
     *         if any existed,
     *         {@link PutStatus#FAILED} if map was empty or null
     */
    default PutStatus put(final Map<String, V> map) {
        if (map == null || map.isEmpty()) {
            return PutStatus.FAILED;
        }

        final int putSize = map.size();
        final var prevValues = new ConcurrentHashMap<String, V>(putSize);
        final Set<String> keysToInsert = new HashSet<>(map.keySet());
        final var keyMapUpdate = new ConcurrentHashMap<String, String>(putSize);

        if (!hasKeyMap()) {
            final var exists = existsList(keysToInsert);
            return CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
                var status = PutStatus.INSERTED;
                var shared = openDb();
                try {
                    // Separate updates from inserts
                    final var sharedForUpdate = shared; // capture for this lambda
                    try {
                        CHRONICLE_UTILS.parallelIterable(exists, Integer.MAX_VALUE, key -> {
                            if (sharedForUpdate.map.containsKey(key)) {
                                prevValues.put(key, sharedForUpdate.map.put(key, map.get(key)));
                            }
                        });
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }

                    if (!prevValues.isEmpty()) {
                        status = PutStatus.UPDATED;
                        keysToInsert.removeAll(prevValues.keySet());
                    }

                    // Insert new keys with rotation handling
                    if (!keysToInsert.isEmpty()) {
                        final var batches = new ArrayList<>(keysToInsert);
                        int startIndex = 0;

                        while (startIndex < batches.size()) {
                            final long remainingEntries = entries() - shared.map.size();
                            final int endIndex = (int) Math.min(startIndex + remainingEntries, batches.size());

                            // Process this batch (sublist)
                            final var batch = batches.subList(startIndex, endIndex);
                            final String currentFileSnap = getDataFileState().currentFile();
                            final var currentShared = shared; // capture for this lambda
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
                                shared = checkAndRotate(shared);
                                keyMapUpdate.clear();
                            }
                        }
                    }

                    // Only update KeyMap if rotation happened (hasKeyMap becomes true)
                    // No indexes since it was !hasKeyMap()
                    if (hasKeyMap() && !keyMapUpdate.isEmpty()) {
                        final var keyHashMap = CHRONICLE_UTILS.preCalculateKeyHashMap(keyMapUpdate.keySet());
                        addAllToKeyMap(keyMapUpdate, keyHashMap);
                    }

                    if (!prevValues.isEmpty()) {
                        Logger.info("Updated [{}] records at [{}].", prevValues.size(), dataPath());
                    }
                    if (!keysToInsert.isEmpty()) {
                        Logger.info("Inserted [{}] records at [{}].", keysToInsert.size(), dataPath());
                    }

                    return status;
                } finally {
                    shared.close();
                }
            });
        }

        // 0. Pre-calculate key hashes ONCE (OUTSIDE LOCK)
        final var keyHashMap = CHRONICLE_UTILS.preCalculateKeyHashMap(keysToInsert);

        // 1. Prepare index additions (OUTSIDE LOCK)
        final var preparedIndex = CHRONICLE_UTILS.prepareIndexAdditions(indexFileNames(), map,
                averageValue().getClass(), indexExclusions(), keyHashMap);

        try (final var grouped = getDbFiles(keyHashMap.values())) {
            return CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
                var status = PutStatus.INSERTED;

                // 2. PHASE 1: Updates (Inside Lock)
                CHRONICLE_UTILS.processInParallel(grouped.fileGroups().entrySet(), entry -> {
                    final var file = entry.getKey();
                    try (final var shared = openDb(file)) {
                        CHRONICLE_UTILS.parallelIterable(entry.getValue(), Integer.MAX_VALUE, key -> {
                            if (shared.map.containsKey(key)) {
                                prevValues.put(key, shared.map.put(key, map.get(key)));
                            }
                        });
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                });

                if (!prevValues.isEmpty()) {
                    status = PutStatus.UPDATED;
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

                if (!prevValues.isEmpty()) {
                    Logger.info("Updated [{}] records at [{}].", prevValues.size(), dataPath());
                }
                if (!keysToInsert.isEmpty()) {
                    Logger.info("Inserted [{}] records at [{}].", keysToInsert.size(), dataPath());
                }
                return status;
            });
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Updates multiple existing entities in batch.
     * <p>
     * Only updates keys that already exist. Unlike {@link #put(Map)}, this method
     * never inserts new records and does not affect database size.
     * </p>
     * <p>
     * For single-file DAOs without indexes, skips hash calculation and index
     * operations.
     * For multi-file or indexed DAOs, groups keys by file and updates indexes in
     * parallel.
     * </p>
     *
     * @param map the key-value pairs to update (all keys must exist)
     * @return {@link PutStatus#UPDATED} if all succeeded, {@link PutStatus#PARTIAL}
     *         if some were missing,
     *         {@link PutStatus#FAILED} if all were missing or map was empty
     */
    default PutStatus update(final Map<String, V> map) {
        if (map == null || map.isEmpty()) {
            return PutStatus.FAILED;
        }

        final var mapSize = map.size();
        final var prevValues = new ConcurrentHashMap<String, V>(mapSize);
        final var keys = map.keySet();

        if (!hasKeyMap()) {
            return CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
                try (final var shared = openDb()) {
                    CHRONICLE_UTILS.parallelIterable(keys, Integer.MAX_VALUE, key -> {
                        if (shared.map.containsKey(key)) {
                            prevValues.put(key, shared.map.put(key, map.get(key)));
                        }
                    });
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }

                final var prevValueSize = prevValues.size();
                if (prevValueSize == 0) {
                    Logger.error("All [{}] values do not exist during update at [{}]", mapSize, dataPath());
                    return PutStatus.FAILED;
                }

                var status = PutStatus.UPDATED;
                if (prevValueSize != mapSize) {
                    final var missingKeys = new HashSet<>(keys);
                    missingKeys.removeAll(prevValues.keySet());
                    Logger.error("{} keys missing during update at [{}]. Missing: {}.",
                            missingKeys.size(), dataPath(), missingKeys);
                    status = PutStatus.PARTIAL;
                }

                Logger.info("Updated [{}] records at [{}].", prevValues.size(), dataPath());
                return status;
            });
        }

        // 0. Pre-calculate key hashes ONCE (OUTSIDE LOCK)
        final var keyHashMap = CHRONICLE_UTILS.preCalculateKeyHashMap(keys);

        // 1. PHASE 1: Preparation (OUTSIDE LOCK)
        // Identify exactly which files hold these keys before blocking other writers.
        // Also prepare index additions (heavy reflection & hashing)
        final var preparedIndex = CHRONICLE_UTILS.prepareIndexAdditions(indexFileNames(), map,
                averageValue().getClass(), indexExclusions(), keyHashMap);

        try (final var grouped = getDbFiles(keyHashMap.values())) {
            // 2. PHASE 2: Data Update (INSIDE LOCK)
            return CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
                CHRONICLE_UTILS.processInParallel(grouped.fileGroups().entrySet(), entry -> {
                    final var file = entry.getKey();
                    try (final var shared = openDb(file)) {
                        CHRONICLE_UTILS.parallelIterable(entry.getValue(), Integer.MAX_VALUE, key -> {
                            // STRICT UPDATE: Only put if it actually exists in this file
                            if (shared.map.containsKey(key)) {
                                prevValues.put(key, shared.map.put(key, map.get(key)));
                            }
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
     * Inserts multiple new entities in batch (skips existing keys).
     * <p>
     * Only inserts keys that don't already exist. Unlike {@link #put(Map)}, this
     * method
     * never updates existing records. Handles file rotation automatically.
     * </p>
     * <p>
     * Race condition handling: Checks for existing keys outside the lock, then
     * re-checks
     * inside the lock to handle concurrent insertions.
     * </p>
     * <p>
     * For single-file DAOs without indexes, skips hash calculation and index
     * operations.
     * For multi-file or indexed DAOs, updates KeyMap and indexes in parallel.
     * </p>
     *
     * @param map the key-value pairs to insert (only new keys will be inserted)
     * @return {@link PutStatus#INSERTED} on success, {@link PutStatus#FAILED} if
     *         all keys exist
     */
    default PutStatus insert(final Map<String, V> map) {
        if (map == null || map.isEmpty()) {
            return PutStatus.FAILED;
        }

        final var keyMapUpdate = new ConcurrentHashMap<String, String>();

        if (!hasKeyMap()) {
            final var notExists = notExistsList(map.keySet());
            if (notExists.isEmpty()) {
                Logger.error("No new records found (all exist) during insert at [{}].", dataPath());
                return PutStatus.FAILED;
            }
            return CHRONICLE_UTILS.doWithLock(WRITE_LOCKS, dataPath(), () -> {
                final var raceConditionKeys = existsList(notExists);

                if (!raceConditionKeys.isEmpty()) {
                    // Race detected! Another thread inserted these keys while we were preparing.
                    notExists.removeAll(raceConditionKeys);
                }

                if (notExists.isEmpty()) {
                    Logger.error("No new records found (all exist) during insert at [{}].", dataPath());
                    return PutStatus.FAILED;
                }

                var shared = openDb();
                try {
                    // Insert in batches
                    int startIndex = 0;
                    final var insertSize = notExists.size();

                    while (startIndex < insertSize) {
                        final long remainingEntries = entries() - shared.map.size();
                        final int endIndex = (int) Math.min(startIndex + remainingEntries, insertSize);
                        final var batch = notExists.subList(startIndex, endIndex);
                        final var currentFileSnap = getDataFileState().currentFile();
                        final var currentShared = shared;

                        try {
                            CHRONICLE_UTILS.parallelIterable(batch, Integer.MAX_VALUE, key -> {
                                currentShared.map.put(key, map.get(key));
                                keyMapUpdate.put(key, currentFileSnap);
                            });
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }

                        startIndex = endIndex;
                        if (startIndex < insertSize) {
                            shared = checkAndRotate(shared); // Simple rotation - creates KeyMap
                            keyMapUpdate.clear(); // Keys now in KeyMap from rotation
                        }
                    }

                    // Only update KeyMap if rotation happened
                    if (hasKeyMap() && !keyMapUpdate.isEmpty()) {
                        final var keyHashMap = CHRONICLE_UTILS.preCalculateKeyHashMap(keyMapUpdate.keySet());
                        addAllToKeyMap(keyMapUpdate, keyHashMap);
                    }

                    Logger.info("Inserted [{}] records at [{}].", insertSize, dataPath());
                    return PutStatus.INSERTED;
                } finally {
                    shared.close();
                }
            });
        }

        // 0. Pre-calculate key hashes ONCE (OUTSIDE LOCK)
        final var keyHashMap = CHRONICLE_UTILS.preCalculateKeyHashMap(map.keySet());
        final var dataToInsert = new HashMap<>(map);

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
                    Logger.error("No new records found (all exist) during insert at [{}].", dataPath());
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

    /**
     * Searches for records matching all filters within a set of keys.
     * <p>
     * Applies filters sequentially to each record. All filters must match for a
     * record
     * to be included. Processing stops once the limit is reached.
     * </p>
     * <p>
     * For single-file DAOs, iterates directly through the data file.
     * For multi-file DAOs, groups keys by file for efficient lookup.
     * </p>
     *
     * @param keys    the candidate keys to search within
     * @param filters the search criteria (all must match)
     * @param limit   maximum number of matching records to return
     * @return map of matching key-value pairs (up to limit)
     * @throws InterruptedException if the parallel processing is interrupted
     */
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
        final var hashes = CHRONICLE_UTILS.preCalculateKeyHashes(keys);
        try (final var grouped = getDbFiles(hashes)) {
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

    /**
     * Searches for records matching all filters and returns as CSV format.
     * <p>
     * Similar to {@link #search(Iterable, List, int)} but returns results as CSV.
     * Uses memory-efficient {@code getUsing()} pattern.
     * </p>
     *
     * @param keys    the candidate keys to search within
     * @param filters the search criteria (all must match)
     * @param limit   maximum number of matching records to return
     * @return {@link CsvObject} containing headers and matching rows (up to limit)
     * @throws InterruptedException if the parallel processing is interrupted
     */
    default CsvObject searchCsv(final Iterable<String> keys, final List<Search> filters, final int limit)
            throws InterruptedException {
        if (keys == null || !keys.iterator().hasNext() || filters.isEmpty()) {
            return CsvObject.empty();
        }

        Logger.debug("Querying CSV filtered keys at [{}] with [{}] remaining filters.", dataPath(), filters.size());
        final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);
        final var headers = new AtomicReference<>(new String[0]);

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

                            if (headers.get().length == 0) {
                                headers.set(CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                            }
                            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
                            return true;
                        });
            }
            return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
        }

        final AtomicInteger count = new AtomicInteger();
        final var hashes = CHRONICLE_UTILS.preCalculateKeyHashes(keys);
        try (final var grouped = getDbFiles(hashes)) {
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

                        if (headers.get().length == 0) {
                            headers.set(CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                        }
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
     * Searches for records matching all filters and returns specified fields only.
     * <p>
     * Combines search filtering with field projection. Uses memory-efficient
     * {@code getUsing()} pattern.
     * </p>
     *
     * @param keys    the candidate keys to search within
     * @param filters the search criteria (all must match)
     * @param fields  the field names to include in the result
     * @param limit   maximum number of matching records to return
     * @return map of key to field-value map for matching records (up to limit)
     * @throws InterruptedException if the parallel processing is interrupted
     */
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
        final var hashes = CHRONICLE_UTILS.preCalculateKeyHashes(keys);
        try (final var grouped = getDbFiles(hashes)) {
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

    /**
     * Searches for records matching all filters and returns specified fields as CSV
     * format.
     * <p>
     * Combines search filtering, field projection, and CSV formatting. Uses
     * memory-efficient
     * {@code getUsing()} pattern.
     * </p>
     *
     * @param keys    the candidate keys to search within
     * @param filters the search criteria (all must match)
     * @param fields  the field names to include
     * @param limit   maximum number of matching records to return
     * @return {@link CsvObject} containing headers and matching rows (up to limit)
     * @throws InterruptedException if the parallel processing is interrupted
     */
    default CsvObject searchSubsetCsv(final Iterable<String> keys, final List<Search> filters, final String[] fields,
            final int limit) throws InterruptedException {
        if (keys == null || !keys.iterator().hasNext()) {
            return CsvObject.empty();
        }

        Logger.debug("Querying subset CSV filtered keys at [{}] with [{}] remaining filters.", dataPath(),
                filters.size());
        final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
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
        final var hashes = CHRONICLE_UTILS.preCalculateKeyHashes(keys);
        try (final var grouped = getDbFiles(hashes)) {
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

    /**
     * Searches for keys matching all filters and adds them to the results
     * collection.
     * <p>
     * Internal helper method that populates a provided collection with matching
     * keys.
     * Uses memory-efficient {@code getUsing()} pattern.
     * </p>
     *
     * @param keys    the candidate keys to search within
     * @param filters the search criteria (all must match)
     * @param results the collection to add matching keys to
     * @throws InterruptedException if the parallel processing is interrupted
     */
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

        final var hashes = CHRONICLE_UTILS.preCalculateKeyHashes(keys);
        try (final var grouped = getDbFiles(hashes)) {
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

    /**
     * Searches for records matching all filters with key exclusion.
     * <p>
     * Combines search filtering with key exclusion. Excluded keys are skipped
     * before applying filters.
     * </p>
     *
     * @param keys         the candidate keys to search within
     * @param filters      the search criteria (all must match)
     * @param excludedKeys keys to skip during search
     * @param limit        maximum number of matching records to return
     * @return map of matching key-value pairs (up to limit, excluding specified
     *         keys)
     * @throws InterruptedException if the parallel processing is interrupted
     */
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
        final var hashes = CHRONICLE_UTILS.preCalculateKeyHashes(keys);
        try (final var grouped = getDbFiles(hashes)) {
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

    /**
     * Searches for records matching all filters as CSV format with key exclusion.
     * <p>
     * Combines search filtering, CSV formatting, and key exclusion.
     * Uses memory-efficient {@code getUsing()} pattern.
     * </p>
     *
     * @param keys         the candidate keys to search within
     * @param filters      the search criteria (all must match)
     * @param excludedKeys keys to skip during search
     * @param limit        maximum number of matching records to return
     * @return {@link CsvObject} containing headers and matching rows (up to limit)
     * @throws InterruptedException if the parallel processing is interrupted
     */
    default CsvObject searchCsv(final Iterable<String> keys, final List<Search> filters,
            final Set<String> excludedKeys, final int limit) throws InterruptedException {
        if (keys == null || !keys.iterator().hasNext()) {
            return CsvObject.empty();
        }

        Logger.debug("Querying filtered keys at [{}] with [{}] remaining filters and [{}] excluded keys.", dataPath(),
                filters.size(), excludedKeys.size());
        final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);
        final var headers = new AtomicReference<>(new String[0]);

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

                            if (headers.get().length == 0) {
                                headers.set(CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                            }
                            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
                            return true;
                        });
            }
            return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
        }

        final AtomicInteger count = new AtomicInteger();
        final var hashes = CHRONICLE_UTILS.preCalculateKeyHashes(keys);
        try (final var grouped = getDbFiles(hashes)) {
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

                                    if (headers.get().length == 0) {
                                        headers.set(CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                                    }
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

    /**
     * Searches for records matching all filters with field projection and key
     * exclusion.
     * <p>
     * Combines search filtering, field projection, and key exclusion.
     * Uses memory-efficient {@code getUsing()} pattern.
     * </p>
     *
     * @param keys         the candidate keys to search within
     * @param filters      the search criteria (all must match)
     * @param excludedKeys keys to skip during search
     * @param limit        maximum number of matching records to return
     * @param fields       the field names to include in the result
     * @return map of key to field-value map for matching records (up to limit)
     * @throws InterruptedException if the parallel processing is interrupted
     */
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
        final var hashes = CHRONICLE_UTILS.preCalculateKeyHashes(keys);
        try (final var grouped = getDbFiles(hashes)) {
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

    /**
     * Searches for records matching all filters as CSV format with field projection
     * and key exclusion.
     * <p>
     * Combines search filtering, field projection, CSV formatting, and key
     * exclusion.
     * Uses memory-efficient {@code getUsing()} pattern.
     * </p>
     *
     * @param keys         the candidate keys to search within
     * @param filters      the search criteria (all must match)
     * @param excludedKeys keys to skip during search
     * @param fields       the field names to include
     * @param limit        maximum number of matching records to return
     * @return {@link CsvObject} containing headers and matching rows (up to limit)
     * @throws InterruptedException if the parallel processing is interrupted
     */
    default CsvObject searchSubsetCsv(final Iterable<String> keys, final List<Search> filters,
            final Set<String> excludedKeys, final String[] fields, final int limit) throws InterruptedException {
        if (keys == null || !keys.iterator().hasNext()) {
            return CsvObject.empty();
        }

        Logger.debug("Querying subset CSV filtered keys at [{}] with [{}] remaining filters and [{}] excluded keys.",
                dataPath(), filters.size(), excludedKeys.size());
        final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
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
        final var hashes = CHRONICLE_UTILS.preCalculateKeyHashes(keys);
        try (final var grouped = getDbFiles(hashes)) {
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

    /**
     * Counts records matching all filters within a set of keys.
     * <p>
     * More efficient than {@link #search(Iterable, List, int)} when only the count
     * is needed.
     * Uses memory-efficient {@code getUsing()} pattern.
     * </p>
     *
     * @param keys    the candidate keys to search within
     * @param filters the search criteria (all must match)
     * @return the count of matching records
     * @throws InterruptedException if the parallel processing is interrupted
     */
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
        final var hashes = CHRONICLE_UTILS.preCalculateKeyHashes(keys);
        try (final var grouped = getDbFiles(hashes)) {
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

    /**
     * Searches using pre-calculated 128-bit hashes with filters.
     * <p>
     * Optimized for cases where hashes were already computed (e.g., from an index
     * lookup).
     * Avoids the overhead of re-calculating hashes from keys.
     * </p>
     *
     * @param hashes  pre-calculated 128-bit hashes
     * @param filters the search criteria (all must match)
     * @param limit   maximum number of matching records to return
     * @return map of matching key-value pairs (up to limit)
     * @throws InterruptedException if the parallel processing is interrupted
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
        try (final var grouped = getDbFiles(hashes)) {
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
     * Searches using pre-calculated hashes with filters and field projection.
     * <p>
     * Combines hash-based lookup, search filtering, and field projection.
     * Optimized for cases where hashes were already computed.
     * </p>
     *
     * @param hashes  pre-calculated 128-bit hashes
     * @param filters the search criteria (all must match)
     * @param fields  the field names to include in the result
     * @param limit   maximum number of matching records to return
     * @return map of key to field-value map for matching records (up to limit)
     * @throws InterruptedException if the parallel processing is interrupted
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
        try (final var grouped = getDbFiles(hashes)) {
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
     * Searches using pre-calculated hashes with filters and returns as CSV format.
     * <p>
     * Combines hash-based lookup, search filtering, and CSV formatting.
     * Optimized for cases where hashes were already computed.
     * </p>
     *
     * @param hashes  pre-calculated 128-bit hashes
     * @param filters the search criteria (all must match)
     * @param limit   maximum number of matching records to return
     * @return {@link CsvObject} containing headers and matching rows (up to limit)
     * @throws InterruptedException if the parallel processing is interrupted
     */
    default CsvObject searchCsvFromHashes(final Iterable<byte[]> hashes, final List<Search> filters, final int limit)
            throws InterruptedException {
        if (hashes == null || !hashes.iterator().hasNext() || filters.isEmpty()) {
            return CsvObject.empty();
        }

        Logger.debug("Searching CSV by hashes at [{}] with [{}] filters.", dataPath(), filters.size());
        final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);
        final var headers = new AtomicReference<>(new String[0]);

        final AtomicInteger count = new AtomicInteger();
        try (final var grouped = getDbFiles(hashes)) {
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

                                if (headers.get().length == 0) {
                                    headers.set(CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                                }
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
     * Searches using pre-calculated hashes with filters, field projection, and CSV
     * format.
     * <p>
     * Combines hash-based lookup, search filtering, field projection, and CSV
     * formatting.
     * </p>
     *
     * @param hashes  pre-calculated 128-bit hashes
     * @param filters the search criteria (all must match)
     * @param fields  the field names to include
     * @param limit   maximum number of matching records to return
     * @return {@link CsvObject} containing headers and matching rows (up to limit)
     * @throws InterruptedException if the parallel processing is interrupted
     */
    default CsvObject searchSubsetCsvFromHashes(final Iterable<byte[]> hashes, final List<Search> filters,
            final String[] fields, final int limit) throws InterruptedException {
        if (hashes == null || !hashes.iterator().hasNext()) {
            return CsvObject.empty();
        }

        Logger.debug("Searching subset CSV by hashes at [{}] with [{}] filters.", dataPath(), filters.size());
        final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);
        final var csvHeaders = CHRONICLE_UTILS.getCsvHeaders(fields);

        final AtomicInteger count = new AtomicInteger();
        try (final var grouped = getDbFiles(hashes)) {
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
     * Searches for keys matching filters using pre-calculated hashes.
     * <p>
     * Returns only keys (not values) for memory efficiency.
     * Uses memory-efficient {@code getUsing()} pattern.
     * </p>
     *
     * @param hashes  pre-calculated 128-bit hashes
     * @param filters the search criteria (all must match)
     * @return set of matching keys
     * @throws InterruptedException if the parallel processing is interrupted
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

        try (final var grouped = getDbFiles(hashes)) {
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
     * Searches for keys matching filters using pre-calculated hashes, returning as
     * list.
     * <p>
     * Similar to {@link #searchKeysFromHashes(Iterable, List)} but returns a
     * {@link List}.
     * </p>
     *
     * @param hashes  pre-calculated 128-bit hashes
     * @param filters the search criteria (all must match)
     * @return list of matching keys (order is not guaranteed)
     * @throws InterruptedException if the parallel processing is interrupted
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

        try (final var grouped = getDbFiles(hashes)) {
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
     * Counts records matching filters using pre-calculated hashes.
     * <p>
     * More efficient than searching when only the count is needed.
     * Uses memory-efficient {@code getUsing()} pattern.
     * </p>
     *
     * @param hashes  pre-calculated 128-bit hashes
     * @param filters the search criteria (all must match)
     * @return the count of matching records
     * @throws InterruptedException if the parallel processing is interrupted
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

        try (final var grouped = getDbFiles(hashes)) {
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

    /**
     * Searches using pre-calculated hashes with filters and key exclusion.
     * <p>
     * Combines hash-based lookup, search filtering, and key exclusion.
     * Excluded keys are filtered out during file grouping for efficiency.
     * </p>
     *
     * @param hashes       pre-calculated 128-bit hashes
     * @param filters      the search criteria (all must match)
     * @param excludedKeys keys to skip during search
     * @param limit        maximum number of matching records to return
     * @return map of matching key-value pairs (up to limit, excluding specified
     *         keys)
     * @throws InterruptedException if the parallel processing is interrupted
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

        try (final var grouped = getDbFiles(hashes, Integer.MAX_VALUE, excludedKeys)) {
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
     * Searches using pre-calculated hashes with filters and key exclusion,
     * returning as CSV.
     * <p>
     * Combines hash-based lookup, search filtering, key exclusion, and CSV
     * formatting.
     * </p>
     *
     * @param hashes       pre-calculated 128-bit hashes
     * @param filters      the search criteria (all must match)
     * @param excludedKeys keys to skip during search
     * @param limit        maximum number of matching records to return
     * @return {@link CsvObject} containing headers and matching rows (up to limit)
     * @throws InterruptedException if the parallel processing is interrupted
     */
    default CsvObject searchCsvFromHashes(final Iterable<byte[]> hashes, final List<Search> filters,
            final Set<String> excludedKeys, final int limit) throws InterruptedException {
        if (hashes == null || !hashes.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.debug("Searching CSV by hashes at [{}] with [{}] filters and excluded keys.", dataPath(),
                filters.size());
        final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());
        final var headers = new AtomicReference<>(new String[0]);
        final AtomicInteger count = new AtomicInteger();
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        try (final var grouped = getDbFiles(hashes, Integer.MAX_VALUE, excludedKeys)) {
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

                                if (headers.get().length == 0) {
                                    headers.set(CHRONICLE_UTILS.getHeadersFromObject(classData, value));
                                }
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
     * Searches using pre-calculated hashes with filters, field projection, and key
     * exclusion.
     * <p>
     * Combines hash-based lookup, search filtering, field projection, and key
     * exclusion.
     * </p>
     *
     * @param hashes       pre-calculated 128-bit hashes
     * @param filters      the search criteria (all must match)
     * @param excludedKeys keys to skip during search
     * @param fields       the field names to include in the result
     * @param limit        maximum number of matching records to return
     * @return map of key to field-value map for matching records (up to limit)
     * @throws InterruptedException if the parallel processing is interrupted
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

        try (final var grouped = getDbFiles(hashes, Integer.MAX_VALUE, excludedKeys)) {
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
     * Searches using pre-calculated hashes with filters, field projection, key
     * exclusion, and CSV format.
     * <p>
     * Combines all search optimizations: hash-based lookup, filtering, field
     * projection,
     * key exclusion, and CSV formatting.
     * </p>
     *
     * @param hashes       pre-calculated 128-bit hashes
     * @param filters      the search criteria (all must match)
     * @param excludedKeys keys to skip during search
     * @param fields       the field names to include
     * @param limit        maximum number of matching records to return
     * @return {@link CsvObject} containing headers and matching rows (up to limit)
     * @throws InterruptedException if the parallel processing is interrupted
     */
    default CsvObject searchSubsetCsvFromHashes(final Iterable<byte[]> hashes, final List<Search> filters,
            final Set<String> excludedKeys, final String[] fields, final int limit) throws InterruptedException {
        if (hashes == null || !hashes.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.debug("Searching subset CSV by hashes at [{}] with [{}] filters and excluded keys.", dataPath(),
                filters.size());
        final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
        final var classData = CHRONICLE_UTILS.getClassData(averageValue().getClass());
        final var csvHeaders = CHRONICLE_UTILS.getCsvHeaders(fields);
        final AtomicInteger count = new AtomicInteger();
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        try (final var grouped = getDbFiles(hashes, Integer.MAX_VALUE, excludedKeys)) {
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
     * Searches within a single data file and adds matching records to the result
     * map.
     * <p>
     * Internal helper for full table scan operations. Uses
     * {@code safeForEachEntryWhile}
     * for safe iteration with early termination.
     * </p>
     *
     * @param shared  the open data file handle
     * @param filters the search criteria (all must match)
     * @param limit   maximum number of records to find
     * @param result  the map to add matching key-value pairs to
     * @param counter shared counter for limit enforcement across parallel calls
     */
    private void search(final SharedChronicleMap<String, V> shared, final List<Search> filters, final int limit,
            final Map<String, V> result, final AtomicInteger counter) {
        Logger.debug("Searching DB at [{}] for {} filters.", dataPath(), filters.size());
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        CHRONICLE_UTILS.safeForEachEntryWhile(shared, entry -> {
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

    /**
     * Searches within a single data file and adds matching records as CSV rows.
     * <p>
     * Internal helper for full table scan CSV operations.
     * </p>
     *
     * @param shared   the open data file handle
     * @param filters  the search criteria (all must match)
     * @param limit    maximum number of records to find
     * @param rowQueue the queue to add CSV rows to
     * @param counter  shared counter for limit enforcement
     * @param headers  atomic reference for CSV headers (set from first match)
     */
    private void searchCsv(final SharedChronicleMap<String, V> shared, final List<Search> filters, final int limit,
            final ConcurrentLinkedQueue<Object[]> rowQueue, final AtomicInteger counter,
            final AtomicReference<String[]> headers) {
        Logger.debug("Searching DB at [{}] for {} filters.", dataPath(), filters.size());
        final var averageValueClass = averageValue().getClass();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        CHRONICLE_UTILS.safeForEachEntryWhile(shared, entry -> {
            final String key = entry.key().get();
            final V value = entry.value().getUsing(using());
            for (final var search : preparedFilters) {
                if (!CHRONICLE_UTILS.search(search, key, value)) {
                    return true;
                }
            }

            if (headers.get().length == 0) {
                headers.set(CHRONICLE_UTILS.getHeadersFromObject(classData, value));
            }
            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
            return counter.incrementAndGet() < limit;
        });
    }

    /**
     * Searches within a single data file with field projection.
     * <p>
     * Internal helper for full table scan with field projection.
     * </p>
     *
     * @param shared  the open data file handle
     * @param filters the search criteria (all must match)
     * @param limit   maximum number of records to find
     * @param result  the map to add matching field-value maps to
     * @param counter shared counter for limit enforcement
     * @param fields  the field names to include in the result
     */
    private void searchSubset(final SharedChronicleMap<String, V> shared, final List<Search> filters, final int limit,
            final Map<String, Map<String, Object>> result, final AtomicInteger counter,
            final String[] fields) {
        Logger.debug("Subset searching DB at [{}] for {} filters.", dataPath(), filters.size());
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);

        CHRONICLE_UTILS.safeForEachEntryWhile(shared, entry -> {
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

    /**
     * Searches within a single data file with field projection and CSV output.
     * <p>
     * Internal helper for full table scan with field projection and CSV formatting.
     * </p>
     *
     * @param shared   the open data file handle
     * @param filters  the search criteria (all must match)
     * @param limit    maximum number of records to find
     * @param rowQueue the queue to add CSV rows to
     * @param counter  shared counter for limit enforcement
     * @param fields   the field names to include
     */
    private void searchSubsetCsv(final SharedChronicleMap<String, V> shared, final List<Search> filters,
            final int limit,
            final ConcurrentLinkedQueue<Object[]> rowQueue, final AtomicInteger counter, final String[] fields) {
        Logger.debug("Subset CSV searching DB at [{}] for {} filters.", dataPath(), filters.size());
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);

        CHRONICLE_UTILS.safeForEachEntryWhile(shared, entry -> {
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

    /**
     * Searches within a single data file and collects matching keys only.
     * <p>
     * Internal helper for key-only full table scan. More memory-efficient
     * when values are not needed.
     * </p>
     *
     * @param shared  the open data file handle
     * @param filters the search criteria (all must match)
     * @param results the collection to add matching keys to
     */
    private void searchKeys(final SharedChronicleMap<String, V> shared, final List<Search> filters,
            final Collection<String> results) {
        Logger.debug("Searching DB keys at [{}] for {} filters.", dataPath(), filters.size());
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        CHRONICLE_UTILS.safeForEachEntry(shared, entry -> {
            final String key = entry.key().get();
            final V value = entry.value().getUsing(using());
            for (final var search : preparedFilters) {
                if (!CHRONICLE_UTILS.search(search, key, value)) {
                    return; // Skip to next entry (like continue)
                }
            }
            results.add(key);
        });
    }

    /**
     * Searches within a single data file with key exclusion.
     * <p>
     * Internal helper for full table scan with key exclusion.
     * </p>
     *
     * @param shared       the open data file handle
     * @param filters      the search criteria (all must match)
     * @param excludedKeys keys to skip during search
     * @param limit        maximum number of records to find
     * @param result       the map to add matching key-value pairs to
     * @param counter      shared counter for limit enforcement
     */
    private void search(final SharedChronicleMap<String, V> shared, final List<Search> filters,
            final Set<String> excludedKeys,
            final int limit, final Map<String, V> result, final AtomicInteger counter) {
        Logger.debug("Searching DB at [{}] using [{}] filters and [{}] excluded keys. Limit [{}]",
                dataPath(), filters.size(), excludedKeys.size(), limit);
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        CHRONICLE_UTILS.safeForEachEntryWhile(shared, entry -> {
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

    /**
     * Searches within a single data file with key exclusion and CSV output.
     * <p>
     * Internal helper for full table scan with key exclusion and CSV formatting.
     * </p>
     *
     * @param shared       the open data file handle
     * @param filters      the search criteria (all must match)
     * @param excludedKeys keys to skip during search
     * @param limit        maximum number of records to find
     * @param rowQueue     the queue to add CSV rows to
     * @param counter      shared counter for limit enforcement
     * @param headers      atomic reference for CSV headers
     */
    private void searchCsv(final SharedChronicleMap<String, V> shared, final List<Search> filters,
            final Set<String> excludedKeys, final int limit, final ConcurrentLinkedQueue<Object[]> rowQueue,
            final AtomicInteger counter, final AtomicReference<String[]> headers) {
        Logger.debug("Searching DB at [{}] using [{}] filters and [{}] excluded keys. Limit [{}]",
                dataPath(), filters.size(), excludedKeys.size(), limit);
        final var averageValueClass = averageValue().getClass();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        CHRONICLE_UTILS.safeForEachEntryWhile(shared, entry -> {
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

            if (headers.get().length == 0) {
                headers.set(CHRONICLE_UTILS.getHeadersFromObject(classData, value));
            }
            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(classData, key, value));
            return counter.incrementAndGet() < limit;
        });
    }

    /**
     * Searches within a single data file with key exclusion and field projection.
     * <p>
     * Internal helper for full table scan with key exclusion and field projection.
     * </p>
     *
     * @param shared       the open data file handle
     * @param filters      the search criteria (all must match)
     * @param excludedKeys keys to skip during search
     * @param limit        maximum number of records to find
     * @param result       the map to add matching field-value maps to
     * @param counter      shared counter for limit enforcement
     * @param fields       the field names to include
     */
    private void searchSubset(final SharedChronicleMap<String, V> shared, final List<Search> filters,
            final Set<String> excludedKeys, final int limit, final Map<String, Map<String, Object>> result,
            final AtomicInteger counter, final String[] fields) {
        Logger.debug("Searching DB at [{}] using [{}] filters and [{}] excluded keys. Limit [{}]",
                dataPath(), filters.size(), excludedKeys.size(), limit);
        final var averageValueClass = averageValue().getClass();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        CHRONICLE_UTILS.safeForEachEntryWhile(shared, entry -> {
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

    /**
     * Searches within a single data file with key exclusion, field projection, and
     * CSV output.
     * <p>
     * Internal helper combining key exclusion, field projection, and CSV
     * formatting.
     * </p>
     *
     * @param shared       the open data file handle
     * @param filters      the search criteria (all must match)
     * @param excludedKeys keys to skip during search
     * @param limit        maximum number of records to find
     * @param rowQueue     the queue to add CSV rows to
     * @param counter      shared counter for limit enforcement
     * @param fields       the field names to include
     */
    private void searchSubsetCsv(final SharedChronicleMap<String, V> shared, final List<Search> filters,
            final Set<String> excludedKeys, final int limit, final ConcurrentLinkedQueue<Object[]> rowQueue,
            final AtomicInteger counter, final String[] fields) {
        Logger.debug("Subset CSV searching DB at [{}] using [{}] filters and [{}] excluded keys. Limit [{}]",
                dataPath(), filters.size(), excludedKeys.size(), limit);
        final var averageValueClass = averageValue().getClass();
        final var classData = CHRONICLE_UTILS.getClassData(averageValueClass);
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        CHRONICLE_UTILS.safeForEachEntryWhile(shared, entry -> {
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

    /**
     * Searches within a single data file with key exclusion, collecting keys only.
     * <p>
     * Internal helper for key-only full table scan with key exclusion.
     * </p>
     *
     * @param shared       the open data file handle
     * @param filters      the search criteria (all must match)
     * @param excludedKeys keys to skip during search
     * @param results      the collection to add matching keys to
     */
    private void searchKeys(final SharedChronicleMap<String, V> shared, final List<Search> filters,
            final Set<String> excludedKeys, final Collection<String> results) {
        Logger.debug("Searching DB keys at [{}] using [{}] filters and [{}] excluded keys.",
                dataPath(), filters.size(), excludedKeys.size());
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        CHRONICLE_UTILS.safeForEachEntry(shared, entry -> {
            final String key = entry.key().get();
            if (excludedKeys.contains(key)) {
                return;
            }

            final V value = entry.value().getUsing(using());
            for (final var search : preparedFilters) {
                if (!CHRONICLE_UTILS.search(search, key, value)) {
                    return;
                }
            }

            results.add(key);
        });
    }

    /**
     * Counts matching records within a single data file.
     * <p>
     * Internal helper for count operations without returning data.
     * </p>
     *
     * @param shared  the open data file handle
     * @param filters the search criteria (all must match)
     * @return the count of matching records
     */
    private int searchCount(final SharedChronicleMap<String, V> shared, final List<Search> filters) {
        Logger.debug("Counting DB at [{}] for {} filters.", dataPath(), filters.size());
        final var count = new AtomicInteger();
        final var averageValueClass = averageValue().getClass();
        final var preparedFilters = filters.stream()
                .map(s -> CHRONICLE_UTILS.prepareSearch(s, averageValueClass))
                .toList();

        CHRONICLE_UTILS.safeForEachEntry(shared, entry -> {
            final String key = entry.key().get();
            final V value = entry.value().getUsing(using());
            for (final var search : preparedFilters) {
                if (!CHRONICLE_UTILS.search(search, key, value)) {
                    return;
                }
            }
            count.incrementAndGet();
        });

        return count.get();
    }

    /**
     * Filters an in-memory map using search criteria.
     * <p>
     * Useful for post-processing results that are already loaded in memory.
     * Processes entries in parallel for efficiency.
     * </p>
     *
     * @param db      the in-memory key-value map to filter
     * @param filters the search criteria (all must match)
     * @return filtered key-value pairs
     */
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
     * Performs an indexed search using a pre-opened index.
     * <p>
     * Uses the B+ tree index structure for efficient lookups without scanning all
     * records.
     * Supports various search types: EQUAL, NOT_EQUAL, LESS, GREATER, LIKE,
     * STARTS_WITH,
     * ENDS_WITH, IN, NOT_IN, BETWEEN, etc.
     * </p>
     *
     * @param search the search criteria including field, type, and term
     * @param index  the opened index NavigableSet
     * @return {@link SearchResult} containing matching 128-bit hashes
     */
    default SearchResult indexedSearch(final Search search, final NavigableSet<byte[]> index) {
        Logger.debug("Index searching at [{}] for {}.", dataPath(), search.field());
        if (index == null || index.isEmpty()) {
            return new SearchResult(Collections.emptyList());
        }

        final SearchType searchType = search.searchType();
        final String searchTerm = CHRONICLE_UTILS.toStringOptimized(search.searchTerm());
        final Set<String> searchTermSet = (searchType == SearchType.IN || searchType == SearchType.NOT_IN
                || searchType == SearchType.IN_FULL_SCAN)
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
            case IN_FULL_SCAN -> MAP_DB.getInIndexSearchFullScan(index, searchTermSet, search.limit());
            case NOT_IN -> MAP_DB.getNotInIndexSearch(index, searchTermSet, search.limit());
            case BETWEEN -> MAP_DB.getBetweenIndexSearch(index, searchTermBetween.get(0).toString(),
                    searchTermBetween.get(1).toString(), search.limit());
            default -> throw new UnsupportedOperationException("Search type not supported: " + searchType);
        };
    }

    /**
     * Performs an indexed search with hash exclusion.
     * <p>
     * Similar to {@link #indexedSearch(Search, NavigableSet)} but filters out
     * results matching the excluded hashes.
     * </p>
     *
     * @param search         the search criteria
     * @param index          the opened index NavigableSet
     * @param excludedHashes hashes to exclude from results
     * @return {@link SearchResult} containing matching hashes (excluding specified)
     */
    default SearchResult indexedSearch(final Search search, final NavigableSet<byte[]> index,
            final Set<byte[]> excludedHashes) {
        Logger.debug("Index searching at [{}] with [{}] excluded keys for {}.", dataPath(), excludedHashes.size(),
                search.field());
        if (index == null || index.isEmpty()) {
            return new SearchResult(Collections.emptyList());
        }

        final SearchType searchType = search.searchType();
        final String searchTerm = CHRONICLE_UTILS.toStringOptimized(search.searchTerm());
        final Set<String> searchTermSet = (searchType == SearchType.IN || searchType == SearchType.NOT_IN
                || searchType == SearchType.IN_FULL_SCAN)
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
            case IN_FULL_SCAN ->
                MAP_DB.getInIndexSearchFullScan(index, searchTermSet, search.limit(), excludedHashes);
            case NOT_IN -> MAP_DB.getNotInIndexSearch(index, searchTermSet, search.limit(), excludedHashes);
            case BETWEEN -> MAP_DB.getBetweenIndexSearch(index, searchTermBetween.get(0).toString(),
                    searchTermBetween.get(1).toString(), search.limit(), excludedHashes);
            default -> throw new UnsupportedOperationException("Search type not supported: " + searchType);
        };
    }

    /**
     * Checks if an iterable result is empty.
     *
     * @param result the iterable to check
     * @return {@code true} if null or has no elements
     */
    private <T> boolean isResultEmpty(final Iterable<T> result) {
        return result == null || !result.iterator().hasNext();
    }

    /**
     * Performs an indexed search and returns full entities.
     * <p>
     * Opens the index, performs the search, then retrieves the actual records
     * using the resulting hashes.
     * </p>
     *
     * @param search the search criteria (field must be indexed)
     * @return map of matching key-value pairs
     */
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

    /**
     * Performs an indexed search and returns only matching keys.
     * <p>
     * More memory-efficient than {@link #indexedSearch(Search)} when values are not
     * needed.
     * </p>
     *
     * @param search the search criteria (field must be indexed)
     * @return set of matching keys
     */
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

    /**
     * Performs an indexed search and returns matching keys as a list.
     * <p>
     * Similar to {@link #indexedSearchKeys(Search)} but returns a {@link List}.
     * </p>
     *
     * @param search the search criteria (field must be indexed)
     * @return list of matching keys (order is not guaranteed)
     */
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

    /**
     * Performs an indexed search with key exclusion and returns full entities.
     * <p>
     * Pre-calculates excluded key hashes for efficient filtering during index
     * search.
     * </p>
     *
     * @param search       the search criteria (field must be indexed)
     * @param excludedKeys keys to exclude from results
     * @return map of matching key-value pairs (excluding specified keys)
     */
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

    /**
     * Performs an indexed search with key exclusion, returning only keys.
     * <p>
     * Combines indexed search efficiency with key exclusion and memory-efficient
     * key-only results.
     * </p>
     *
     * @param search       the search criteria (field must be indexed)
     * @param excludedKeys keys to exclude from results
     * @return set of matching keys (excluding specified keys)
     */
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

    /**
     * Performs an indexed search with key exclusion, returning keys as a list.
     * <p>
     * Similar to {@link #indexedSearchKeys(Search, Set)} but returns a
     * {@link List}.
     * </p>
     *
     * @param search       the search criteria (field must be indexed)
     * @param excludedKeys keys to exclude from results
     * @return list of matching keys (excluding specified keys, order not
     *         guaranteed)
     */
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

    /**
     * Searches within a set of pre-filtered keys using a single filter.
     *
     * @param matchingKeys the keys to search within (from a previous operation)
     * @param search       the search criterion to apply
     * @return matching key-value pairs
     * @throws InterruptedException if the parallel processing is interrupted
     */
    default Map<String, V> searchMatching(final Collection<String> matchingKeys, final Search search)
            throws InterruptedException {
        if (matchingKeys == null || matchingKeys.isEmpty()) {
            return Collections.emptyMap();
        }

        return search(matchingKeys, List.of(search), matchingKeys.size());
    }

    /**
     * Filters an in-memory map using a single search criterion.
     *
     * @param db     the in-memory key-value map to filter
     * @param search the search criterion to apply
     * @return filtered key-value pairs
     */
    default Map<String, V> search(final Map<String, V> db, final Search search) {
        if (db == null || db.isEmpty()) {
            return Collections.emptyMap();
        }
        return search(db, List.of(search));
    }

    /**
     * Full table scan search using a single criterion.
     * <p>
     * Scans all data files in parallel. Use indexed search methods when
     * searching on indexed fields for better performance.
     * </p>
     *
     * @param search the search criterion (limit can be set via
     *               {@link Search#limit()})
     * @return matching key-value pairs (up to limit or {@link #HARD_LIMIT})
     */
    default Map<String, V> search(final Search search) {
        final int limit = search.limit() == -1 ? HARD_LIMIT : search.limit();
        final Map<String, V> result = new ConcurrentHashMap<>(limit);
        final AtomicInteger counter = new AtomicInteger(0);

        CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
            if (counter.get() >= limit) {
                return;
            }
            try (final var shared = openDb(file)) {
                search(shared, List.of(search), limit, result, counter);
            }
        });

        return result;
    }

    /**
     * Full table scan returning only keys matching all criteria.
     * <p>
     * More memory-efficient than {@link #search(Search)} when you only need keys.
     * Scans all data files in parallel.
     * </p>
     *
     * @param searches the search criteria (all must match)
     * @return set of matching keys
     */
    default Set<String> searchKeys(final List<Search> searches) {
        final var results = ConcurrentHashMap.<String>newKeySet();
        CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
            try (final var shared = openDb(file)) {
                searchKeys(shared, searches, results);
            }
        });
        return results;
    }

    /**
     * Full table scan returning only keys matching all criteria as a list.
     *
     * @param searches the search criteria (all must match)
     * @return list of matching keys (order is not guaranteed)
     */
    default List<String> searchKeysList(final List<Search> searches) {
        final var result = new ConcurrentLinkedQueue<String>();
        CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
            try (final var shared = openDb(file)) {
                searchKeys(shared, searches, result);
            }
        });
        return new ArrayList<>(result);
    }

    /**
     * Full table scan search with key exclusion.
     * <p>
     * Similar to {@link #search(Search)} but skips keys in the exclusion set.
     * </p>
     *
     * @param search       the search criterion
     * @param excludedKeys keys to skip during search
     * @return matching key-value pairs (excluding specified keys)
     */
    default Map<String, V> search(final Search search, final Set<String> excludedKeys) {
        final int limit = search.limit() == -1 ? HARD_LIMIT : search.limit();
        final Map<String, V> result = new ConcurrentHashMap<>(limit);
        final AtomicInteger counter = new AtomicInteger(0);

        CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
            if (counter.get() >= limit) {
                return;
            }
            try (final var shared = openDb(file)) {
                search(shared, List.of(search), excludedKeys, limit, result, counter);
            }
        });

        return result;
    }

    /**
     * Full table scan returning only keys matching all criteria with exclusion.
     * <p>
     * Combines full table key search with key exclusion.
     * </p>
     *
     * @param searches     the search criteria (all must match)
     * @param excludedKeys keys to skip during search
     * @return set of matching keys (excluding specified keys)
     */
    default Set<String> searchKeys(final List<Search> searches, final Set<String> excludedKeys) {
        final var results = ConcurrentHashMap.<String>newKeySet();
        CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
            try (final var shared = openDb(file)) {
                searchKeys(shared, searches, excludedKeys, results);
            }
        });
        return results;
    }

    /**
     * Full table scan returning keys as a list with exclusion.
     * <p>
     * Similar to {@link #searchKeys(List, Set)} but returns a {@link List}.
     * </p>
     *
     * @param searches     the search criteria (all must match)
     * @param excludedKeys keys to skip during search
     * @return list of matching keys (excluding specified keys, order not
     *         guaranteed)
     */
    default List<String> searchKeysList(final List<Search> searches, final Set<String> excludedKeys) {
        final var result = new ConcurrentLinkedQueue<String>();
        CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
            try (final var shared = openDb(file)) {
                searchKeys(shared, searches, excludedKeys, result);
            }
        });
        return new ArrayList<>(result);
    }

    /**
     * Searches for keys matching all filters within a candidate set.
     * <p>
     * Filters the provided keys based on search criteria.
     * </p>
     *
     * @param keys    the candidate keys to search within
     * @param filters the search criteria (all must match)
     * @return set of matching keys
     * @throws InterruptedException if the parallel processing is interrupted
     */
    default Set<String> searchKeys(final Iterable<String> keys, final List<Search> filters)
            throws InterruptedException {
        final var results = ConcurrentHashMap.<String>newKeySet();
        searchKeys(keys, filters, results);
        return new HashSet<>(results);
    }

    /**
     * Searches for keys matching all filters within a candidate set, returning as
     * list.
     * <p>
     * Similar to {@link #searchKeys(Iterable, List)} but returns a {@link List}.
     * </p>
     *
     * @param keys    the candidate keys to search within
     * @param filters the search criteria (all must match)
     * @return list of matching keys (order is not guaranteed)
     * @throws InterruptedException if the parallel processing is interrupted
     */
    default List<String> searchKeysList(final Iterable<String> keys, final List<Search> filters)
            throws InterruptedException {
        final var results = new ConcurrentLinkedQueue<String>();
        searchKeys(keys, filters, results);
        return new ArrayList<>(results);
    }

    /**
     * Performs a multi-criteria search with automatic index optimization and field
     * projection.
     * <p>
     * Intelligently separates indexed and non-indexed search criteria:
     * <ol>
     * <li>Identifies the first indexed search criterion</li>
     * <li>Executes indexed search to get candidate keys</li>
     * <li>Applies remaining filters to candidates</li>
     * <li>Returns only the specified fields</li>
     * </ol>
     * </p>
     *
     * @param searches the search criteria (all must match)
     * @param fields   the field names to include in the result
     * @return map of key to field-value map for matching records
     * @throws InterruptedException if the parallel processing is interrupted
     */
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
                        searchSubset(shared, searches, limit, result, counter, fields);
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

    /**
     * Performs a multi-criteria search with index optimization, field projection,
     * and CSV output.
     * <p>
     * Similar to {@link #multiSearchSubset(List, String[])} but returns results as
     * CSV format.
     * </p>
     *
     * @param searches the search criteria (all must match)
     * @param fields   the field names to include
     * @return {@link CsvObject} containing headers and matching rows
     * @throws InterruptedException if the parallel processing is interrupted
     */
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
                final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
                final var counter = new AtomicInteger();
                CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
                    if (counter.get() >= limit) {
                        return;
                    }
                    try (final var shared = openDb(file)) {
                        searchSubsetCsv(shared, searches, limit, rowQueue, counter, fields);
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

    /**
     * Performs a multi-criteria search with automatic index optimization.
     * <p>
     * Intelligently separates indexed and non-indexed search criteria, using the
     * first
     * indexed field to narrow results before applying remaining filters. For
     * single-criteria
     * searches, delegates directly to the indexed search path when available.
     * </p>
     * <p>
     * Algorithm:
     * <ol>
     * <li>Separates searches into first-indexed and remaining non-indexed
     * criteria</li>
     * <li>Computes the minimum limit across all search criteria</li>
     * <li>If an indexed search exists, performs B+ tree lookup first</li>
     * <li>Applies remaining filters to the indexed result set (or full scan if no
     * index)</li>
     * </ol>
     * </p>
     *
     * @param searches list of search criteria (all must match - AND semantics)
     * @return map of primary keys to matching values; empty map if searches is
     *         null/empty
     * @throws InterruptedException if parallel processing is interrupted
     */
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
                        search(shared, searches, limit, result, counter);
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

    /**
     * Performs a multi-criteria search with automatic index optimization, returning
     * CSV format.
     * <p>
     * Similar to {@link #multiSearch(List)} but returns results as a
     * {@link CsvObject}
     * containing headers and rows suitable for export or display.
     * </p>
     *
     * @param searches list of search criteria (all must match - AND semantics)
     * @return CSV object with headers and matching rows; empty CSV if searches is
     *         null/empty
     * @throws InterruptedException if parallel processing is interrupted
     * @see #multiSearch(List)
     */
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
                final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
                final var headers = new AtomicReference<>(new String[0]);
                final var counter = new AtomicInteger();
                CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
                    if (counter.get() >= limit) {
                        return;
                    }
                    try (final var shared = openDb(file)) {
                        searchCsv(shared, searches, limit, rowQueue, counter, headers);
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

    /**
     * Performs a multi-criteria search with automatic index optimization, returning
     * only keys.
     * <p>
     * Similar to {@link #multiSearch(List)} but returns only the primary keys of
     * matching
     * records, which is more efficient when values are not needed.
     * </p>
     *
     * @param searches list of search criteria (all must match - AND semantics)
     * @return set of primary keys for matching records; empty set if searches is
     *         null/empty
     * @throws InterruptedException if parallel processing is interrupted
     * @see #multiSearch(List)
     */
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

    /**
     * Performs a multi-criteria search with automatic index optimization, returning
     * keys as a list.
     * <p>
     * Similar to {@link #multiSearchKeys(List)} but returns results as a
     * {@link List}
     * instead of a {@link Set}, preserving potential ordering from indexed
     * searches.
     * </p>
     *
     * @param searches list of search criteria (all must match - AND semantics)
     * @return list of primary keys for matching records; empty list if searches is
     *         null/empty
     * @throws InterruptedException if parallel processing is interrupted
     * @see #multiSearchKeys(List)
     */
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

    /**
     * Performs a multi-criteria search with automatic index optimization, excluding
     * specified keys.
     * <p>
     * Similar to {@link #multiSearch(List)} but filters out any records matching
     * the excluded keys.
     * The exclusion is applied both during indexed searches (via hash exclusion)
     * and during
     * manual scanning.
     * </p>
     *
     * @param searches     list of search criteria (all must match - AND semantics)
     * @param excludedKeys set of primary keys to exclude from results
     * @return map of primary keys to matching values; empty map if searches is
     *         null/empty
     * @throws InterruptedException if parallel processing is interrupted
     * @see #multiSearch(List)
     */
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
            final Set<byte[]> excludedHashes = excludedKeys.stream()
                    .map(CHRONICLE_UTILS::to128BitHash)
                    .collect(Collectors.toSet());
            searchResult = indexedSearch(indexedSearch, sharedIndexSet.index, excludedHashes);

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
                        search(shared, searches, excludedKeys, limit, result, counter);
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

    /**
     * Performs a multi-criteria search with index optimization and key exclusion,
     * returning CSV format.
     * <p>
     * Combines the functionality of {@link #multiSearchCsv(List)} with key
     * exclusion support.
     * </p>
     *
     * @param searches     list of search criteria (all must match - AND semantics)
     * @param excludedKeys set of primary keys to exclude from results
     * @return CSV object with headers and matching rows; empty CSV if searches is
     *         null/empty
     * @throws InterruptedException if parallel processing is interrupted
     * @see #multiSearch(List, Set)
     */
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
            final Set<byte[]> excludedHashes = excludedKeys.stream()
                    .map(CHRONICLE_UTILS::to128BitHash)
                    .collect(Collectors.toSet());
            searchResult = indexedSearch(indexedSearch, sharedIndexSet.index, excludedHashes);
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
                final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
                final var headers = new AtomicReference<>(new String[0]);
                final var counter = new AtomicInteger(0);
                CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
                    if (counter.get() >= limit) {
                        return;
                    }
                    try (final var shared = openDb(file)) {
                        searchCsv(shared, searches, excludedKeys, limit, rowQueue, counter, headers);
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

    /**
     * Performs a multi-criteria search with index optimization, key exclusion, and
     * field projection.
     * <p>
     * Combines the functionality of {@link #multiSearchSubset(List, String[])} with
     * key exclusion support.
     * Returns only the specified fields from each matching record.
     * </p>
     *
     * @param searches     list of search criteria (all must match - AND semantics)
     * @param excludedKeys set of primary keys to exclude from results
     * @param fields       field names to include in the result maps
     * @return map of primary keys to field value maps; empty map if searches is
     *         null/empty
     * @throws InterruptedException if parallel processing is interrupted
     * @see #multiSearch(List, Set)
     */
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
            final Set<byte[]> excludedHashes = excludedKeys.stream()
                    .map(CHRONICLE_UTILS::to128BitHash)
                    .collect(Collectors.toSet());
            searchResult = indexedSearch(indexedSearch, sharedIndexSet.index, excludedHashes);
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
                        searchSubset(shared, searches, excludedKeys, limit, result, counter, fields);
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

    /**
     * Performs a multi-criteria search with index optimization, key exclusion,
     * field projection, and CSV output.
     * <p>
     * Combines the functionality of {@link #multiSearchSubsetCsv(List, String[])}
     * with key exclusion support.
     * Returns only the specified fields from each matching record in CSV format.
     * </p>
     *
     * @param searches     list of search criteria (all must match - AND semantics)
     * @param excludedKeys set of primary keys to exclude from results
     * @param fields       field names to include in the CSV columns
     * @return CSV object with headers and matching rows; empty CSV if searches is
     *         null/empty
     * @throws InterruptedException if parallel processing is interrupted
     * @see #multiSearchSubset(List, Set, String[])
     */
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
            final Set<byte[]> excludedHashes = excludedKeys.stream()
                    .map(CHRONICLE_UTILS::to128BitHash)
                    .collect(Collectors.toSet());
            searchResult = indexedSearch(indexedSearch, sharedIndexSet.index, excludedHashes);
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
                final var rowQueue = new ConcurrentLinkedQueue<Object[]>();
                final var counter = new AtomicInteger(0);
                CHRONICLE_UTILS.processInParallel(getDataFileState().fileNames(), file -> {
                    if (counter.get() >= limit) {
                        return;
                    }
                    try (final var shared = openDb(file)) {
                        searchSubsetCsv(shared, searches, excludedKeys, limit, rowQueue, counter, fields);
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

    /**
     * Performs a multi-criteria search over a pre-filtered set of keys.
     * <p>
     * Applies the search criteria only to records matching the given keys,
     * which is efficient when keys have already been narrowed by a prior operation.
     * </p>
     *
     * @param matchingKeys collection of primary keys to search within
     * @param searches     list of search criteria (all must match - AND semantics)
     * @return map of primary keys to matching values
     * @throws InterruptedException if parallel processing is interrupted
     */
    default Map<String, V> multiSearch(final Collection<String> matchingKeys, final List<Search> searches)
            throws InterruptedException {
        final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(HARD_LIMIT);

        return search(matchingKeys, searches, limit);
    }

    /**
     * Performs a multi-criteria search over a pre-filtered set of keys, returning
     * CSV format.
     * <p>
     * Similar to {@link #multiSearch(Collection, List)} but returns results as a
     * {@link CsvObject}.
     * </p>
     *
     * @param matchingKeys collection of primary keys to search within
     * @param searches     list of search criteria (all must match - AND semantics)
     * @return CSV object with headers and matching rows
     * @throws InterruptedException if parallel processing is interrupted
     */
    default CsvObject multiSearchCsv(final Collection<String> matchingKeys, final List<Search> searches)
            throws InterruptedException {
        final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(HARD_LIMIT);

        return searchCsv(matchingKeys, searches, limit);
    }

    /**
     * Performs a multi-criteria search over a pre-filtered set of keys with field
     * projection.
     * <p>
     * Similar to {@link #multiSearch(Collection, List)} but returns only the
     * specified fields
     * from each matching record.
     * </p>
     *
     * @param matchingKeys collection of primary keys to search within
     * @param searches     list of search criteria (all must match - AND semantics)
     * @param fields       field names to include in the result maps
     * @return map of primary keys to field value maps
     * @throws InterruptedException if parallel processing is interrupted
     */
    default Map<String, Map<String, Object>> multiSearchSubset(final Collection<String> matchingKeys,
            final List<Search> searches, final String[] fields) throws InterruptedException {
        final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(HARD_LIMIT);

        return searchSubset(matchingKeys, searches, fields, limit);
    }

    /**
     * Performs a multi-criteria search over a pre-filtered set of keys with field
     * projection, returning CSV.
     * <p>
     * Similar to {@link #multiSearchSubset(Collection, List, String[])} but returns
     * results
     * as a {@link CsvObject}.
     * </p>
     *
     * @param matchingKeys collection of primary keys to search within
     * @param searches     list of search criteria (all must match - AND semantics)
     * @param fields       field names to include in the CSV columns
     * @return CSV object with headers and matching rows
     * @throws InterruptedException if parallel processing is interrupted
     */
    default CsvObject multiSearchSubsetCsv(final Collection<String> matchingKeys, final List<Search> searches,
            final String[] fields) throws InterruptedException {
        final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(HARD_LIMIT);

        return searchSubsetCsv(matchingKeys, searches, fields, limit);
    }

    /**
     * Counts records matching multiple search criteria with automatic index
     * optimization.
     * <p>
     * Similar to {@link #multiSearch(List)} but returns only the count of matching
     * records,
     * which is more efficient when the actual values are not needed. Uses B+ tree
     * index
     * when available for fast counting.
     * </p>
     *
     * @param searches list of search criteria (all must match - AND semantics)
     * @return count of matching records; 0 if searches is null/empty
     * @throws InterruptedException if parallel processing is interrupted
     */
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
                        totalCount.add(searchCount(shared, searches));
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
     * Returns the total number of records in the database.
     * <p>
     * For single-file DAOs without a keymap, counts directly from the Chronicle
     * Map.
     * For multi-file DAOs, counts from the keymap which tracks all records across
     * files.
     * </p>
     *
     * @return total record count
     */
    default int size() {
        Logger.debug("Getting DB size at [{}].", dataPath());
        if (!hasKeyMap()) {
            try (final var shared = openDb()) {
                return shared.map.size();
            }
        }

        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            return sharedKeyMap.map.size();
        }
    }

    /**
     * Truncates the database, removing all data.
     * <p>
     * Creates a backup before deletion, then removes all data files, indexes, and
     * the keymap.
     * Also clears the data file cache for this DAO.
     * </p>
     * <p>
     * <strong>Warning:</strong> This operation is destructive but creates a backup
     * first.
     * </p>
     *
     * @throws IOException if backup or deletion fails
     */
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
        if (!hasKeyMap()) {
            try (final var shared = openDb()) {
                if (shared.map.isEmpty()) {
                    return false;
                }
                return shared.map.containsKey(key);
            }
        }

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
        if (!hasKeyMap()) {
            final var existsMap = new ConcurrentHashMap<String, Boolean>();
            try (final var shared = openDb()) {
                if (shared.map.isEmpty()) {
                    return existsMap;
                }
                keys.parallelStream().forEach(key -> existsMap.put(key, shared.map.containsKey(key)));
            }

            return existsMap;
        }

        return existsFromHashes(keys, CHRONICLE_UTILS.preCalculateKeyHashMap(keys));
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

        if (!hasKeyMap()) {
            final var existsList = new ConcurrentLinkedQueue<String>();
            try (final var shared = openDb()) {
                if (shared.map.isEmpty()) {
                    return new ArrayList<>();
                }
                keys.parallelStream().forEach(key -> {
                    if (shared.map.containsKey(key)) {
                        existsList.add(key);
                    }
                });
            }

            return new ArrayList<>(existsList);
        }

        return existsListFromHashes(keys, CHRONICLE_UTILS.preCalculateKeyHashMap(keys));
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
     * Returns only the keys that exist in the database.
     *
     * @param keys the keys to check
     * @return set of keys that exist
     */
    default Set<String> existsSet(final Collection<String> keys) {
        Logger.debug("Checking [{}] keys existence at [{}].", keys.size(), dataPath());
        if (!hasKeyMap()) {
            final Set<String> existsSet = ConcurrentHashMap.newKeySet();
            try (final var shared = openDb()) {
                if (shared.map.isEmpty()) {
                    return new HashSet<>();
                }
                keys.parallelStream().forEach(key -> {
                    if (shared.map.containsKey(key)) {
                        existsSet.add(key);
                    }
                });
            }

            return existsSet;
        }

        return existsSetFromHashes(keys, CHRONICLE_UTILS.preCalculateKeyHashMap(keys));
    }

    /**
     * Returns only the keys that exist, using pre-calculated hashes.
     * Avoids re-hashing when hashes are already available.
     *
     * @param keys       the keys to check
     * @param keyHashMap pre-calculated map of primary key to 16-byte hash
     * @return set of keys that exist
     */
    default Set<String> existsSetFromHashes(final Collection<String> keys, final Map<String, byte[]> keyHashMap) {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            return keys.parallelStream()
                    .filter(k -> sharedKeyMap.map.containsKey(keyHashMap.get(k)))
                    .collect(Collectors.toSet());
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
        if (!hasKeyMap()) {
            final Set<String> notExistsSet = ConcurrentHashMap.newKeySet();
            try (final var shared = openDb()) {
                if (shared.map.isEmpty()) {
                    return new HashSet<>(keys);
                }
                keys.parallelStream().forEach(key -> {
                    if (!shared.map.containsKey(key)) {
                        notExistsSet.add(key);
                    }
                });
            }

            return notExistsSet;
        }

        return notExistsFromHashes(keys, CHRONICLE_UTILS.preCalculateKeyHashMap(keys));
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

        if (!hasKeyMap()) {
            final var notExistsList = new ConcurrentLinkedQueue<String>();
            try (final var shared = openDb()) {
                if (shared.map.isEmpty()) {
                    return new ArrayList<>(keys);
                }
                keys.parallelStream().forEach(key -> {
                    if (!shared.map.containsKey(key)) {
                        notExistsList.add(key);
                    }
                });
            }

            return new ArrayList<>(notExistsList);
        }

        return notExistsListFromHashes(keys, CHRONICLE_UTILS.preCalculateKeyHashMap(keys));
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
