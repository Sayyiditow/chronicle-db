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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
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
 *
 * @param <V> Type of the single element
 */
@SuppressWarnings("unchecked")
public interface ChronicleDao<V> {
    static record DataFileState(Set<String> fileNames, String currentFile) {
        public DataFileState withCurrentFile(final String currentFile) {
            return new DataFileState(this.fileNames, currentFile);
        }
    }

    ConcurrentMap<String, Object> LOCKS = new ConcurrentHashMap<>();
    ConcurrentMap<String, DataFileState> DATA_FILE_CACHE = new ConcurrentHashMap<>();
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

    default SharedChronicleMap<String, V> openDb() {
        return CHRONICLE_DB.open(name(), entries(), averageKey(), averageValue(),
                dataPath() + DATA_DIR + getDataFileState().currentFile(), bloatFactor());
    }

    default SharedChronicleMap<String, V> openDb(final String fileName) {
        return CHRONICLE_DB.open(name(), entries(), averageKey(), averageValue(), dataPath() + DATA_DIR + fileName,
                bloatFactor());
    }

    default SharedChronicleMap<String, V> openDb(final String fileName, final long entries) {
        return CHRONICLE_DB.open(name(), entries, averageKey(), averageValue(), dataPath() + DATA_DIR + fileName,
                bloatFactor());
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
        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
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

        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
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
        final var dataFiles = CHRONICLE_UTILS.getFileList(dataPath);

        for (final var file : dataFiles) {
            Files.copy(Path.of(dataPath + file), Path.of(backupPath + file), REPLACE_EXISTING);
        }
    }

    default void deleteDataFiles() throws IOException {
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
    private void initIndex(final ChronicleMap<String, V> db, final Set<String> fields, final String indexDirPath) {
        CHRONICLE_UTILS.index(db, name(), fields, dataPath(), indexDirPath, averageValue().getClass(),
                indexExclusions());
    }

    /**
     * Only runs to initialize an index on the field first time
     * 
     * @param field the field of the V value object
     * 
     */
    private void initIndex(final Set<String> fields) {
        getDataFileState().fileNames().parallelStream().forEach(file -> {
            final Object lock = LOCKS.computeIfAbsent(dataPath() + file, k -> new Object());
            synchronized (lock) {
                try (final var shared = openDb(file)) {
                    initIndex(shared.map, fields, dataPath() + INDEX_DIR);
                }
            }
        });
    }

    /**
     * Delete and rerun all indexes. Faster when inserting a lot of records.
     * 
     */
    default void refreshIndexes() {
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

    default void refreshKeyMap() {
        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            final var dataFileState = getDataFileState();
            if (dataFileState.fileNames().size() > 1) {
                Logger.info("Refreshing KeyMap at [{}]", dataPath());
                try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
                    sharedKeyMap.map.clear();
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
    default void checkUtf8Exceptions(final String dataFileName) throws IOException {
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

    public record GroupedKeys(Map<String, Iterable<String>> fileGroups, AutoCloseable closer) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            closer.close();
        }

        public static final AutoCloseable NOOP = () -> {
        };
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
            try (final var shared = openDb()) {
                for (final String k : keys) {
                    if (shared.map.getUsing(k, using()) != null) {
                        keySet.add(k);
                    }
                }
            }
            final Map<String, Iterable<String>> singleGroup = Map.of(DATA_FILE, keySet);
            return new GroupedKeys(singleGroup, GroupedKeys.NOOP);
        }

        // Multi-file: use lazy grouping based on keyMap
        final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath());

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
                        final String mappedFile = sharedKeyMap.map.get(k);
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

        return new GroupedKeys(fileGroups, () -> sharedKeyMap.close());
    }

    private void removeFromKeyMap(final String key) {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            sharedKeyMap.map.remove(key);
        }
    }

    private void removeAllFromKeyMap(final Set<String> keys) {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            sharedKeyMap.map.keySet().removeAll(keys);
        }
    }

    private void addToKeyMap(final String key, final String file) {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            sharedKeyMap.map.put(key, file);
        }
    }

    private void addToKeyMap(final Map<String, String> map) {
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            sharedKeyMap.map.putAll(map);
        }
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

    private Map<String, Boolean> getKeyMapExists(final Set<String> keys) {
        final var containsMap = new HashMap<String, Boolean>();
        try (final var sharedKeyMap = MAP_DB.openMap(getKeyMapPath())) {
            for (final String k : keys) {
                containsMap.put(k, sharedKeyMap.map.containsKey(k));
            }
        }

        return containsMap;
    }

    private List<String> getKeyMapExistsList(final Set<String> keys) {
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

    private List<String> getKeyMapNotExistsList(final Set<String> keys) {
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

    /**
     * Fetches all records in the db
     * 
     * @return Map<String, V>
     */
    default Map<String, V> fetch(final int limit) {
        final Map<String, V> result = new HashMap<>(10_000);
        for (final String file : getDataFileState().fileNames()) {
            if (result.size() >= limit) {
                break;
            }
            try (final var shared = openDb(file)) {
                shared.map.forEachEntryWhile(entry -> {
                    if (result.size() >= limit) {
                        return false; // Early exit
                    }
                    final var key = entry.key().get();
                    result.put(key, shared.map.getUsing(key, using()));
                    return true;
                });
            }
        }
        Logger.info("Fetched [{}] entries at [{}].", result.size(), dataPath());
        return result;
    }

    default CsvObject fetchCsv(final int limit) {
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var averageValueClass = averageValue().getClass();
        final AtomicReference<String[]> headers = new AtomicReference<>(null);

        for (final String file : getDataFileState().fileNames()) {
            if (rowQueue.size() >= limit) {
                break;
            }
            try (final var shared = openDb(file)) {
                shared.map.forEachEntryWhile(entry -> {
                    if (rowQueue.size() >= limit) {
                        return false; // Early exit
                    }
                    final var key = entry.key().get();
                    final var value = shared.map.getUsing(key, using());
                    headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(averageValueClass, value));
                    rowQueue.add(CHRONICLE_UTILS.getRowFromObject(averageValueClass, key, value));
                    return true;
                });
            }
        }
        Logger.info("Fetched [{}] CSV entries at [{}].", rowQueue.size(), dataPath());

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    default Map<String, LinkedHashMap<String, Object>> fetchSubset(final String[] fields, final int limit) {
        final var result = new ConcurrentHashMap<String, LinkedHashMap<String, Object>>(10_000);
        for (final String file : getDataFileState().fileNames()) {
            if (result.size() >= limit) {
                break;
            }
            try (final var shared = openDb(file)) {
                shared.map.forEachEntryWhile(entry -> {
                    if (result.size() >= limit) {
                        return false; // Early exit
                    }

                    CHRONICLE_UTILS.subsetOfValues(fields, entry.key().get(), entry.value().get(), result, name(),
                            averageValue().getClass());
                    return true;
                });
            }
        }
        Logger.info("Fetched [{}] entries at [{}].", result.size(), dataPath());
        return result;
    }

    default CsvObject fetchSubsetCsv(final String[] fields, final int limit) {
        final String[] headers = CHRONICLE_UTILS.getCsvHeaders(fields);
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();

        for (final String file : getDataFileState().fileNames()) {
            if (rowQueue.size() >= limit) {
                break;
            }
            try (final var shared = openDb(file)) {
                shared.map.forEachEntryWhile(entry -> {
                    if (rowQueue.size() >= limit) {
                        return false; // Early exit
                    }

                    rowQueue.add(
                            CHRONICLE_UTILS.subsetOfValuesToRow(headers, entry.key().get(), entry.value().get(), name(),
                                    averageValue().getClass()));
                    return true;
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

    default Map<String, LinkedHashMap<String, Object>> fetchSubset(final String[] fields) {
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
     * @throws IOException
     */
    default V get(final String key) throws IOException {
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
        final var map = new ConcurrentHashMap<String, V>(1000);

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                for (final var key : keys) {
                    final var value = shared.map.getUsing(key, using());
                    if (value != null) {
                        map.put(key, value);
                    }
                }
            }
            return map;
        }

        try (var keyFiles = getDbFiles(keys, getDataFileState().fileNames())) {
            keyFiles.fileGroups.entrySet().parallelStream().forEach(entry -> {
                final var file = entry.getKey();

                try (final var shared = openDb(file)) {
                    for (final var key : entry.getValue()) {
                        final var value = shared.map.getUsing(key, using());
                        if (value != null) {
                            map.put(key, value);
                        }
                    }
                }
            });
        }

        return map;
    }

    default CsvObject getCsv(final Iterable<String> keys) throws Exception {
        if (keys == null || !keys.iterator().hasNext()) {
            return CsvObject.empty();
        }

        Logger.info("Querying multiple keys for CSV at [{}].", dataPath());
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var averageValueClass = averageValue().getClass();
        final AtomicReference<String[]> headers = new AtomicReference<>(null);

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                for (final var key : keys) {
                    final var value = shared.map.getUsing(key, using());
                    if (value != null) {
                        headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(averageValueClass, value));
                        rowQueue.add(CHRONICLE_UTILS.getRowFromObject(averageValueClass, key, value));
                    }
                }
            }
            return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
        }

        try (var keyFiles = getDbFiles(keys, getDataFileState().fileNames())) {
            keyFiles.fileGroups.entrySet().parallelStream().forEach(entry -> {
                final var file = entry.getKey();

                try (final var shared = openDb(file)) {
                    for (final var key : entry.getValue()) {
                        final var value = shared.map.getUsing(key, using());
                        if (value != null) {
                            headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(averageValueClass, value));
                            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(averageValueClass, key, value));
                        }
                    }
                }
            });
        }

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    default Map<String, LinkedHashMap<String, Object>> getSubset(final Iterable<String> keys, final String[] fields)
            throws Exception {
        if (keys == null || !keys.iterator().hasNext()) {
            return Collections.emptyMap();
        }

        Logger.info("Querying subset multiple keys at [{}].", dataPath());
        final var map = new ConcurrentHashMap<String, LinkedHashMap<String, Object>>(10_000);
        final var averageValueClass = averageValue().getClass();

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                for (final var key : keys) {
                    final var value = shared.map.getUsing(key, using());
                    if (value != null) {
                        CHRONICLE_UTILS.subsetOfValues(fields, key, value, map, name(), averageValueClass);
                    }
                }
            }
            return map;
        }

        try (var keyFiles = getDbFiles(keys, getDataFileState().fileNames())) {
            keyFiles.fileGroups.entrySet().parallelStream().forEach(entry -> {
                final var file = entry.getKey();

                try (final var shared = openDb(file)) {
                    for (final var key : entry.getValue()) {
                        final var value = shared.map.getUsing(key, using());
                        if (value != null) {
                            CHRONICLE_UTILS.subsetOfValues(fields, key, value, map, name(), averageValueClass);
                        }
                    }
                }
            });
        }

        return map;
    }

    default CsvObject getSubsetCsv(final Iterable<String> keys, final String[] fields) throws Exception {
        if (keys == null || !keys.iterator().hasNext()) {
            return CsvObject.empty();
        }

        Logger.info("Querying subset CSV multiple keys at [{}].", dataPath());
        final String[] headers = CHRONICLE_UTILS.getCsvHeaders(fields);
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var averageValueClass = averageValue().getClass();

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                for (final var key : keys) {
                    final var value = shared.map.getUsing(key, using());
                    if (value != null) {
                        rowQueue.add(
                                CHRONICLE_UTILS.subsetOfValuesToRow(headers, key, value, name(), averageValueClass));
                    }
                }
            }
            return new CsvObject(headers, new ArrayList<>(rowQueue));
        }

        try (var keyFiles = getDbFiles(keys, getDataFileState().fileNames())) {
            keyFiles.fileGroups.entrySet().parallelStream().forEach(entry -> {
                final var file = entry.getKey();

                try (final var shared = openDb(file)) {
                    for (final var key : entry.getValue()) {
                        final var value = shared.map.getUsing(key, using());
                        if (value != null) {
                            rowQueue.add(
                                    CHRONICLE_UTILS.subsetOfValuesToRow(headers, key, value, name(),
                                            averageValueClass));
                        }
                    }
                }
            });
        }

        return new CsvObject(headers, new ArrayList<>(rowQueue));
    }

    default Map<String, V> get(final Iterable<String> keys, final int limit) throws Exception {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return Collections.emptyMap();
        }

        Logger.info("Querying multiple keys at [{}] with limit [{}].", dataPath(), limit);
        final var map = new ConcurrentHashMap<String, V>(Math.min(10_000, limit));
        final var count = new AtomicInteger(0);

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                for (final var key : keys) {
                    if (count.get() >= limit)
                        break;
                    final var value = shared.map.getUsing(key, using());
                    if (value != null && count.incrementAndGet() <= limit) {
                        map.put(key, value);
                    }
                }
            }
            return map;
        }

        try (var keyFiles = getDbFiles(keys, getDataFileState().fileNames())) {
            outer: for (final var entry : keyFiles.fileGroups().entrySet()) {
                final var file = entry.getKey();

                try (final var shared = openDb(file)) {
                    for (final var key : entry.getValue()) {
                        if (count.get() >= limit)
                            break outer;
                        final var value = shared.map.getUsing(key, using());
                        if (value != null && count.incrementAndGet() <= limit) {
                            map.put(key, value);
                        }
                    }
                }
            }
        }

        return map;
    }

    default CsvObject getCsv(final Iterable<String> keys, final int limit) throws Exception {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.info("Querying multiple keys at [{}] with limit [{}].", dataPath(), limit);
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var averageValueClass = averageValue().getClass();
        final AtomicReference<String[]> headers = new AtomicReference<>(null);
        final var count = new AtomicInteger(0);

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                for (final var key : keys) {
                    if (count.get() >= limit)
                        break;
                    final var value = shared.map.getUsing(key, using());
                    if (value != null && count.incrementAndGet() <= limit) {
                        headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(averageValueClass, value));
                        rowQueue.add(CHRONICLE_UTILS.getRowFromObject(averageValueClass, key, value));
                    }
                }
            }
            return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
        }

        try (var keyFiles = getDbFiles(keys, getDataFileState().fileNames())) {
            outer: for (final var entry : keyFiles.fileGroups().entrySet()) {
                final var file = entry.getKey();

                try (final var shared = openDb(file)) {
                    for (final var key : entry.getValue()) {
                        if (count.get() >= limit)
                            break outer;
                        final var value = shared.map.getUsing(key, using());
                        if (value != null && count.incrementAndGet() <= limit) {
                            headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(averageValueClass, value));
                            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(averageValueClass, key, value));
                        }
                    }
                }
            }
        }

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    default Map<String, LinkedHashMap<String, Object>> getSubset(final Iterable<String> keys, final String[] fields,
            final int limit) throws Exception {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return Collections.emptyMap();
        }

        Logger.info("Querying subset multiple keys at [{}] with limit [{}].", dataPath(), limit);
        final var map = new ConcurrentHashMap<String, LinkedHashMap<String, Object>>(Math.min(10_000, limit));
        final var averageValueClass = averageValue().getClass();
        final var count = new AtomicInteger(0);

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                for (final var key : keys) {
                    if (count.get() >= limit)
                        break;
                    final var value = shared.map.getUsing(key, using());
                    if (value != null && count.incrementAndGet() <= limit) {
                        CHRONICLE_UTILS.subsetOfValues(fields, key, value, map, name(), averageValueClass);
                    }
                }
            }
            return map;
        }

        try (var keyFiles = getDbFiles(keys, getDataFileState().fileNames())) {
            outer: for (final var entry : keyFiles.fileGroups().entrySet()) {
                final var file = entry.getKey();

                try (final var shared = openDb(file)) {
                    for (final var key : entry.getValue()) {
                        if (count.get() >= limit)
                            break outer;
                        final var value = shared.map.getUsing(key, using());
                        if (value != null && count.incrementAndGet() <= limit) {
                            CHRONICLE_UTILS.subsetOfValues(fields, key, value, map, name(), averageValueClass);
                        }
                    }
                }
            }
        }

        return map;
    }

    default CsvObject getSubsetCsv(final Iterable<String> keys, final String[] fields, final int limit)
            throws Exception {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.info("Querying subset CSV multiple keys at [{}] with limit [{}].", dataPath(), limit);
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var headers = CHRONICLE_UTILS.getCsvHeaders(fields);
        final var averageValueClass = averageValue().getClass();
        final var count = new AtomicInteger(0);

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                for (final var key : keys) {
                    if (count.get() >= limit)
                        break;
                    final var value = shared.map.getUsing(key, using());
                    if (value != null && count.incrementAndGet() <= limit) {
                        rowQueue.add(
                                CHRONICLE_UTILS.subsetOfValuesToRow(headers, key, value, name(), averageValueClass));
                    }
                }
            }
            return new CsvObject(headers, new ArrayList<>(rowQueue));
        }

        try (var keyFiles = getDbFiles(keys, getDataFileState().fileNames())) {
            outer: for (final var entry : keyFiles.fileGroups().entrySet()) {
                final var file = entry.getKey();

                try (final var shared = openDb(file)) {
                    for (final var key : entry.getValue()) {
                        if (count.get() >= limit)
                            break outer;
                        final var value = shared.map.getUsing(key, using());
                        if (value != null && count.incrementAndGet() <= limit) {
                            rowQueue.add(CHRONICLE_UTILS.subsetOfValuesToRow(headers, key, value, name(),
                                    averageValueClass));
                        }
                    }
                }
            }
        }

        return new CsvObject(headers, new ArrayList<>(rowQueue));
    }

    default Map<String, V> get(final Iterable<String> keys, final Set<String> excludedKeys, final int limit)
            throws Exception {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return Collections.emptyMap();
        }

        Logger.info("Querying multiple keys at [{}] with limit [{}] and excluded keys.", dataPath(), limit);
        final var map = new ConcurrentHashMap<String, V>(Math.min(10_000, limit));
        final var count = new AtomicInteger(0);

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                for (final var key : keys) {
                    if (count.get() >= limit)
                        break;
                    if (!excludedKeys.contains(key)) {
                        final var value = shared.map.getUsing(key, using());
                        if (value != null && count.incrementAndGet() <= limit) {
                            map.put(key, value);
                        }
                    }
                }
            }
            return map;
        }

        try (var keyFiles = getDbFiles(keys, getDataFileState().fileNames())) {
            outer: for (final var entry : keyFiles.fileGroups().entrySet()) {
                final var file = entry.getKey();

                try (final var shared = openDb(file)) {
                    for (final var key : entry.getValue()) {
                        if (count.get() >= limit)
                            break outer;
                        if (!excludedKeys.contains(key)) {
                            final var value = shared.map.getUsing(key, using());
                            if (value != null && count.incrementAndGet() <= limit) {
                                map.put(key, value);
                            }
                        }
                    }
                }
            }
        }

        return map;
    }

    default CsvObject getCsv(final Iterable<String> keys, final Set<String> excludedKeys, final int limit)
            throws Exception {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.info("Querying multiple keys at [{}] with limit [{}] and excluded keys.", dataPath(), limit);
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var averageValueClass = averageValue().getClass();
        final AtomicReference<String[]> headers = new AtomicReference<>(null);
        final var count = new AtomicInteger(0);

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                for (final var key : keys) {
                    if (count.get() >= limit)
                        break;
                    if (!excludedKeys.contains(key)) {
                        final var value = shared.map.getUsing(key, using());
                        if (value != null && count.incrementAndGet() <= limit) {
                            headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(averageValueClass, value));
                            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(averageValueClass, key, value));
                        }
                    }
                }
            }
            return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
        }

        try (var keyFiles = getDbFiles(keys, getDataFileState().fileNames())) {
            outer: for (final var entry : keyFiles.fileGroups().entrySet()) {
                final var file = entry.getKey();

                try (final var shared = openDb(file)) {
                    for (final var key : entry.getValue()) {
                        if (count.get() >= limit)
                            break outer;
                        if (!excludedKeys.contains(key)) {
                            final var value = shared.map.getUsing(key, using());
                            if (value != null && count.incrementAndGet() <= limit) {
                                headers.compareAndSet(null,
                                        CHRONICLE_UTILS.getHeadersFromObject(averageValueClass, value));
                                rowQueue.add(CHRONICLE_UTILS.getRowFromObject(averageValueClass, key, value));
                            }
                        }
                    }
                }
            }
        }

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    default Map<String, LinkedHashMap<String, Object>> getSubset(final Iterable<String> keys,
            final Set<String> excludedKeys, final String[] fields, final int limit)
            throws Exception {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return Collections.emptyMap();
        }

        Logger.info("Querying subset multiple keys at [{}] with limit [{}] and excluded keys.", dataPath(), limit);
        final var map = new ConcurrentHashMap<String, LinkedHashMap<String, Object>>(Math.min(10_000, limit));
        final var count = new AtomicInteger(0);
        final var averageValueClass = averageValue().getClass();

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                for (final var key : keys) {
                    if (count.get() >= limit)
                        break;
                    if (!excludedKeys.contains(key)) {
                        final var value = shared.map.getUsing(key, using());
                        if (value != null && count.incrementAndGet() <= limit) {
                            CHRONICLE_UTILS.subsetOfValues(fields, key, value, map, name(), averageValueClass);
                        }
                    }
                }
            }
            return map;
        }

        try (var keyFiles = getDbFiles(keys, getDataFileState().fileNames())) {
            outer: for (final var entry : keyFiles.fileGroups().entrySet()) {
                final var file = entry.getKey();

                try (final var shared = openDb(file)) {
                    for (final var key : entry.getValue()) {
                        if (count.get() >= limit)
                            break outer;
                        if (!excludedKeys.contains(key)) {
                            final var value = shared.map.getUsing(key, using());
                            if (value != null && count.incrementAndGet() <= limit) {
                                CHRONICLE_UTILS.subsetOfValues(fields, key, value, map, name(), averageValueClass);
                            }
                        }
                    }
                }
            }
        }

        return map;
    }

    default CsvObject getSubsetCsv(final Iterable<String> keys, final Set<String> excludedKeys, final String[] fields,
            final int limit) throws Exception {
        if (keys == null || !keys.iterator().hasNext() || limit <= 0) {
            return CsvObject.empty();
        }

        Logger.info("Querying subset CSV multiple keys at [{}] with limit [{}] and excluded keys.", dataPath(), limit);
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final var headers = CHRONICLE_UTILS.getCsvHeaders(fields);
        final var count = new AtomicInteger(0);
        final var averageValueClass = averageValue().getClass();

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                for (final var key : keys) {
                    if (count.get() >= limit)
                        break;
                    if (!excludedKeys.contains(key)) {
                        final var value = shared.map.getUsing(key, using());
                        if (value != null && count.incrementAndGet() <= limit) {
                            rowQueue.add(CHRONICLE_UTILS.subsetOfValuesToRow(headers, key, value, name(),
                                    averageValueClass));
                        }
                    }
                }
            }
            return new CsvObject(headers, new ArrayList<>(rowQueue));
        }

        try (var keyFiles = getDbFiles(keys, getDataFileState().fileNames())) {
            outer: for (final var entry : keyFiles.fileGroups().entrySet()) {
                final var file = entry.getKey();

                try (final var shared = openDb(file)) {
                    for (final var key : entry.getValue()) {
                        if (count.get() >= limit)
                            break outer;
                        if (!excludedKeys.contains(key)) {
                            final var value = shared.map.getUsing(key, using());
                            if (value != null && count.incrementAndGet() <= limit) {
                                rowQueue.add(CHRONICLE_UTILS.subsetOfValuesToRow(headers, key, value, name(),
                                        averageValueClass));
                            }
                        }
                    }
                }
            }
        }

        return new CsvObject(headers, new ArrayList<>(rowQueue));
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
        final var file = getDbFile(key, getDataFileState().fileNames());
        if (file == null) {
            return false;
        }

        final var keyLock = LOCKS.computeIfAbsent(dataPath() + key, k -> new Object());
        V value = null;
        synchronized (keyLock) {
            try (final var shared = openDb(file)) {
                value = shared.map.remove(key);
            }
        }

        if (value == null) {
            return false;
        }

        if (getDataFileState().fileNames().size() > 1) {
            removeFromKeyMap(key);
        }

        final var indexFileNames = indexFileNames();
        CHRONICLE_UTILS.removeFromIndex(name(), dataPath(), indexFileNames, Map.of(key, value),
                averageValue().getClass());
        return true;
    }

    private void removeFromIndex(final Map<String, V> deletedMap) throws IOException {
        Logger.info("{} record(s) deleted at [{}].", deletedMap.size(), dataPath());
        CHRONICLE_UTILS.removeFromIndex(name(), dataPath(), indexFileNames(), deletedMap, averageValue().getClass());
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

        if (getDataFileState().fileNames().size() <= 1) {
            final Object lock = LOCKS.computeIfAbsent(dataPath() + DATA_FILE, k -> new Object());
            synchronized (lock) {
                try (final var shared = openDb()) {
                    for (final String key : keys) {
                        final var deleted = shared.map.remove(key);
                        if (deleted != null) {
                            deletedMap.put(key, deleted);
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

        try (var keyFiles = getDbFiles(keys, getDataFileState().fileNames())) {
            if (keyFiles.fileGroups().isEmpty()) {
                return false;
            }

            for (final var entry : keyFiles.fileGroups().entrySet()) {
                final var file = entry.getKey();
                final Object lock = LOCKS.computeIfAbsent(dataPath() + file, k -> new Object());

                synchronized (lock) {
                    try (final var shared = openDb(file)) {
                        for (final String key : entry.getValue()) {
                            deletedMap.put(key, shared.map.remove(key));
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

    default void resizeDb(final String fileName, final long newSize) throws IOException {
        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
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

    private SharedChronicleMap<String, V> checkAndRotate(final SharedChronicleMap<String, V> shared)
            throws IOException {
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
            final Map<String, String> keyMapUpdate)
            throws IOException {
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
        }

        var status = PutStatus.INSERTED;
        if (prevValue != null) {
            CHRONICLE_UTILS.updateIndex(name(), dataPath(), indexFileNames, Map.of(key, value),
                    Map.of(key, prevValue), averageValue().getClass(), indexExclusions());
            status = PutStatus.UPDATED;
        } else {
            if (getDataFileState().fileNames().size() > 1) {
                addToKeyMap(key, getDataFileState().currentFile());
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
            final var file = getDbFile(key, getDataFileState().fileNames());
            try (final var shared = openDb(file)) {
                if (shared.map.containsKey(key)) {
                    status = PutStatus.UPDATED;
                    prevValue = shared.map.put(key, value);
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
     * Add a value, will not return failed if it exists
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

        if (exists(key)) {
            return PutStatus.FAILED;
        }

        var file = DATA_FILE;
        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            final var dataFileState = getDataFileState();
            file = getDbFile(key, dataFileState.fileNames());

            if (file != null && !DATA_FILE.equals(file)) {
                return PutStatus.FAILED; // already exists
            }

            if (file == null) {
                file = dataFileState.currentFile();
            }

            var shared = openDb();
            try {
                shared = checkAndRotate(shared);
                shared.map.put(key, value);
            } finally {
                shared.close();
            }
        }

        if (getDataFileState().fileNames().size() > 1) {
            addToKeyMap(key, getDataFileState().currentFile());
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
            try (var grouped = getDbFiles(keysToInsert, getDataFileState().fileNames())) {
                for (final var entry : grouped.fileGroups().entrySet()) {
                    final var file = entry.getKey();
                    try (final var shared = openDb(file)) {
                        for (final String key : entry.getValue()) {
                            final V prevValue = shared.map.put(key, map.get(key));
                            if (prevValue != null) {
                                prevValues.put(key, prevValue);
                            }
                        }
                    }
                }
            }

            if (!prevValues.isEmpty()) {
                status = PutStatus.UPDATED;
                keysToInsert.removeAll(prevValues.keySet());
            }
            if (!keysToInsert.isEmpty()) {
                // Insert new records (only keys in keysToInsert)
                var shared = openDb();
                try {
                    for (final String key : keysToInsert) {
                        final V value = map.get(key);
                        shared = checkAndRotate(shared, keyMapUpdate);
                        shared.map.put(key, value);
                        if (getDataFileState().fileNames().size() > 1) {
                            keyMapUpdate.put(key, getDataFileState().currentFile);
                        }
                    }
                } finally {
                    shared.close();
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
            try (var grouped = getDbFiles(map.keySet(), getDataFileState().fileNames())) {
                for (final var entry : grouped.fileGroups().entrySet()) {
                    final var file = entry.getKey();
                    try (final var shared = openDb(file)) {
                        for (final String key : entry.getValue()) {
                            prevValues.put(key, shared.map.put(key, map.get(key)));
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
     * Add multiple values into the db, this will skip existing values
     * 
     * @param map the map to add
     * @throws IOException
     */
    default PutStatus insert(final Map<String, V> map) throws IOException {
        if (map == null || map.isEmpty()) {
            return PutStatus.FAILED;
        }

        // only work with new keys
        final var existingKeys = existsList(map.keySet());
        map.keySet().removeAll(existingKeys);

        final int putSize = map.size();
        final var keyMapUpdate = new HashMap<String, String>();

        final Object lock = LOCKS.computeIfAbsent(dataPath(), k -> new Object());
        synchronized (lock) {
            var shared = openDb();
            try {
                for (final var entry : map.entrySet()) {
                    final String key = entry.getKey();
                    final V value = entry.getValue();
                    shared = checkAndRotate(shared, keyMapUpdate);
                    shared.map.put(key, value);
                    if (getDataFileState().fileNames().size() > 1) {
                        keyMapUpdate.put(key, getDataFileState().currentFile());
                    }
                }
            } finally {
                shared.close();
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

    default Map<String, V> search(final Iterable<String> keys, final List<Search> filters, final int limit)
            throws Throwable {
        if (keys == null || !keys.iterator().hasNext() || filters.isEmpty()) {
            return Collections.emptyMap();
        }

        Logger.info("Querying filtered keys at [{}] with [{}] remaining filters.", dataPath(), filters.size());
        final var map = new ConcurrentHashMap<String, V>(10_000);
        final var valueClass = averageValue().getClass();

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, limit,
                        key -> {
                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : filters) {
                                try {
                                    if (!CHRONICLE_UTILS.search(search, key, value, valueClass)) {
                                        return false;
                                    }
                                } catch (final Throwable e) {
                                    Logger.error("Search failed for key [{}]: {}", key);
                                    Logger.error(e);
                                }
                            }

                            map.put(key, value);
                            return true;
                        });
            }
            return map;
        }

        final AtomicInteger count = new AtomicInteger();
        try (var grouped = getDbFiles(keys, getDataFileState().fileNames())) {
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

                        for (final var search : filters) {
                            try {
                                if (!CHRONICLE_UTILS.search(search, key, value, valueClass)) {
                                    return false;
                                }
                            } catch (final Throwable e) {
                                Logger.error("Search failed for key [{}]: {}", key, e.toString());
                            }
                        }

                        map.put(key, value);
                        return true;
                    });
                }
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
        final AtomicReference<String[]> headers = new AtomicReference<>(null);

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, limit,
                        key -> {
                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : filters) {
                                try {
                                    if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                        return false;
                                    }
                                } catch (final Throwable e) {
                                    Logger.error("Search failed for key [{}]: {}", key);
                                    Logger.error(e);
                                }
                            }

                            headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(averageValueClass, value));
                            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(averageValueClass, key, value));
                            return true;
                        });
            }
            return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
        }

        final AtomicInteger count = new AtomicInteger();
        try (var grouped = getDbFiles(keys, getDataFileState().fileNames())) {
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

                        for (final var search : filters) {
                            try {
                                if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                    return false;
                                }
                            } catch (final Throwable e) {
                                Logger.error("Search failed for key [{}]: {}", key, e.toString());
                            }
                        }

                        headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(averageValueClass, value));
                        rowQueue.add(CHRONICLE_UTILS.getRowFromObject(averageValueClass, key, value));
                        return true;
                    });
                }
            }
        }

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    default Map<String, LinkedHashMap<String, Object>> searchSubset(final Iterable<String> keys,
            final List<Search> filters, final String[] fields, final int limit) throws Throwable {
        if (keys == null || !keys.iterator().hasNext()) {
            return Collections.emptyMap();
        }

        Logger.info("Querying subset filtered keys at [{}] with [{}] remaining filters.", dataPath(), filters.size());
        final var map = new ConcurrentHashMap<String, LinkedHashMap<String, Object>>(10_000);
        final var averageValueClass = averageValue().getClass();

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, limit,
                        key -> {
                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : filters) {
                                try {
                                    if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                        return false;
                                    }
                                } catch (final Throwable e) {
                                    Logger.error("Search failed for key [{}]: {}", key);
                                    Logger.error(e);
                                }
                            }

                            CHRONICLE_UTILS.subsetOfValues(fields, key, value, map, name(), averageValueClass);
                            return true;
                        });
            }
            return map;
        }

        final AtomicInteger count = new AtomicInteger();
        try (var grouped = getDbFiles(keys, getDataFileState().fileNames())) {
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

                        for (final var search : filters) {
                            try {
                                if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                    return false;
                                }
                            } catch (final Throwable e) {
                                Logger.error("Search failed for key [{}]: {}", key, e.toString());
                            }
                        }

                        CHRONICLE_UTILS.subsetOfValues(fields, key, value, map, name(), averageValueClass);
                        return true;
                    });
                }
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

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, limit,
                        key -> {
                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : filters) {
                                try {
                                    if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                        return false;
                                    }
                                } catch (final Throwable e) {
                                    Logger.error("Search failed for key [{}]: {}", key);
                                    Logger.error(e);
                                }
                            }

                            rowQueue.add(CHRONICLE_UTILS.subsetOfValuesToRow(headers, key, value, name(),
                                    averageValueClass));
                            return true;
                        });
            }
            return new CsvObject(headers, new ArrayList<>(rowQueue));
        }

        final AtomicInteger count = new AtomicInteger();
        try (var grouped = getDbFiles(keys, getDataFileState().fileNames())) {
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

                        for (final var search : filters) {
                            try {
                                if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                    return false;
                                }
                            } catch (final Throwable e) {
                                Logger.error("Search failed for key [{}]: {}", key, e.toString());
                            }
                        }

                        rowQueue.add(
                                CHRONICLE_UTILS.subsetOfValuesToRow(headers, key, value, name(), averageValueClass));
                        return true;
                    });
                }
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
        final var valueClass = averageValue().getClass();

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, Integer.MAX_VALUE,
                        key -> {
                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : filters) {
                                try {
                                    if (!CHRONICLE_UTILS.search(search, key, value, valueClass)) {
                                        return false;
                                    }
                                } catch (final Throwable e) {
                                    Logger.error("Search failed for key [{}]: {}", key);
                                    Logger.error(e);
                                }
                            }

                            results.add(key);
                            return true;
                        });
            }
            return;
        }

        try (var grouped = getDbFiles(keys, getDataFileState().fileNames())) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                final var file = entry.getKey();
                final var keysForFile = entry.getValue(); // Iterable<String>

                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(keysForFile, Integer.MAX_VALUE, key -> {
                        final V value = shared.map.getUsing(key, using());
                        if (value == null)
                            return false;

                        for (final var search : filters) {
                            try {
                                if (!CHRONICLE_UTILS.search(search, key, value, valueClass)) {
                                    return false;
                                }
                            } catch (final Throwable e) {
                                Logger.error("Search failed for key [{}]: {}", key, e.toString());
                            }
                        }

                        results.add(key);
                        return true;
                    });
                }
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
        final var map = new ConcurrentHashMap<String, V>(10_000);
        final var valueClass = averageValue().getClass();

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
                                try {
                                    if (!CHRONICLE_UTILS.search(search, key, value, valueClass)) {
                                        return false;
                                    }
                                } catch (final Throwable e) {
                                    Logger.error("Search failed for key [{}]: {}", key);
                                    Logger.error(e);
                                }
                            }

                            map.put(key, value);
                            return true;
                        });
            }
            return map;
        }

        final AtomicInteger count = new AtomicInteger();
        try (var grouped = getDbFiles(keys, getDataFileState().fileNames())) {
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

                                for (final var search : filters) {
                                    try {
                                        if (!CHRONICLE_UTILS.search(search, key, value, valueClass)) {
                                            return false;
                                        }
                                    } catch (final Throwable e) {
                                        Logger.error("Search failed for key [{}]: {}", key, e.toString());
                                    }
                                }

                                map.put(key, value);
                                return true;
                            });
                }
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
                                try {
                                    if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                        return false;
                                    }
                                } catch (final Throwable e) {
                                    Logger.error("Search failed for key [{}]: {}", key);
                                    Logger.error(e);
                                }
                            }

                            headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(averageValueClass, value));
                            rowQueue.add(CHRONICLE_UTILS.getRowFromObject(averageValueClass, key, value));
                            return true;
                        });
            }
            return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
        }

        final AtomicInteger count = new AtomicInteger();
        try (var grouped = getDbFiles(keys, getDataFileState().fileNames())) {
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

                                for (final var search : filters) {
                                    try {
                                        if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                            return false;
                                        }
                                    } catch (final Throwable e) {
                                        Logger.error("Search failed for key [{}]: {}", key, e.toString());
                                    }
                                }

                                headers.compareAndSet(null,
                                        CHRONICLE_UTILS.getHeadersFromObject(averageValueClass, value));
                                rowQueue.add(CHRONICLE_UTILS.getRowFromObject(averageValueClass, key, value));
                                return true;
                            });
                }
            }
        }

        return new CsvObject(headers.get(), new ArrayList<>(rowQueue));
    }

    default Map<String, LinkedHashMap<String, Object>> searchSubset(final Iterable<String> keys,
            final List<Search> filters, final Set<String> excludedKeys, final int limit, final String[] fields)
            throws Throwable {
        if (keys == null || !keys.iterator().hasNext()) {
            return Collections.emptyMap();
        }

        Logger.info("Querying subset filtered keys at [{}] with [{}] remaining filters and [{}] excluded keys.",
                dataPath(), filters.size(), excludedKeys.size());
        final var map = new ConcurrentHashMap<String, LinkedHashMap<String, Object>>(Math.min(10_000, limit));
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
                                try {
                                    if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                        return false;
                                    }
                                } catch (final Throwable e) {
                                    Logger.error("Search failed for key [{}]: {}", key);
                                    Logger.error(e);
                                }
                            }

                            CHRONICLE_UTILS.subsetOfValues(fields, key, value, map, name(), averageValueClass);
                            return true;
                        });
            }
            return map;
        }

        final AtomicInteger count = new AtomicInteger();
        try (var grouped = getDbFiles(keys, getDataFileState().fileNames())) {
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

                                for (final var search : filters) {
                                    try {
                                        if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                            return false;
                                        }
                                    } catch (final Throwable e) {
                                        Logger.error("Search failed for key [{}]: {}", key, e.toString());
                                    }
                                }

                                CHRONICLE_UTILS.subsetOfValues(fields, key, value, map, name(), averageValueClass);
                                return true;
                            });
                }
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
                                try {
                                    if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                        return false;
                                    }
                                } catch (final Throwable e) {
                                    Logger.error("Search failed for key [{}]: {}", key);
                                    Logger.error(e);
                                }
                            }

                            rowQueue.add(CHRONICLE_UTILS.subsetOfValuesToRow(headers, key, value, name(),
                                    averageValueClass));
                            return true;
                        });
            }
            return new CsvObject(headers, new ArrayList<>(rowQueue));
        }

        final AtomicInteger count = new AtomicInteger();
        try (var grouped = getDbFiles(keys, getDataFileState().fileNames())) {
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

                                for (final var search : filters) {
                                    try {
                                        if (!CHRONICLE_UTILS.search(search, key, value, averageValueClass)) {
                                            return false;
                                        }
                                    } catch (final Throwable e) {
                                        Logger.error("Search failed for key [{}]: {}", key, e.toString());
                                    }
                                }

                                rowQueue.add(CHRONICLE_UTILS.subsetOfValuesToRow(headers, key, value, name(),
                                        averageValueClass));
                                return true;
                            });
                }
            }
        }

        return new CsvObject(headers, new ArrayList<>(rowQueue));
    }

    default long searchCount(final Iterable<String> keys, final List<Search> filters, final int limit)
            throws Throwable {
        if (keys == null || !keys.iterator().hasNext()) {
            return 0;
        }

        Logger.info("Counting filtered keys at [{}] with [{}] remaining filters.", dataPath(), filters.size());

        // Determine minimum positive limit across all filters
        final var count = new LongAdder();
        final var valueClass = averageValue().getClass();

        if (getDataFileState().fileNames().size() <= 1) {
            try (final var shared = openDb()) {
                CHRONICLE_UTILS.parallelIterable(keys, limit,
                        key -> {
                            final V value = shared.map.getUsing(key, using());
                            if (value == null)
                                return false;

                            for (final var search : filters) {
                                try {
                                    if (!CHRONICLE_UTILS.search(search, key, value, valueClass)) {
                                        return false;
                                    }
                                } catch (final Throwable e) {
                                    Logger.error("Search failed for key [{}]: {}", key);
                                    Logger.error(e);
                                }
                            }

                            count.increment();
                            return true;
                        });
            }
            return count.sum();
        }

        final AtomicInteger counter = new AtomicInteger(0);
        try (var grouped = getDbFiles(keys, getDataFileState().fileNames())) {
            for (final var entry : grouped.fileGroups().entrySet()) {
                if (count.sum() >= limit)
                    break;

                final var file = entry.getKey();
                final var keysForFile = entry.getValue(); // Iterable<String>

                try (final var shared = openDb(file)) {
                    CHRONICLE_UTILS.parallelIterable(keysForFile, limit - counter.get(), counter,
                            key -> {
                                final V value = shared.map.getUsing(key, using());
                                if (value == null)
                                    return false;

                                for (final var search : filters) {
                                    try {
                                        if (!CHRONICLE_UTILS.search(search, key, value, valueClass)) {
                                            return false;
                                        }
                                    } catch (final Throwable e) {
                                        Logger.error("Search failed for key [{}]: {}", key, e.toString());
                                    }
                                }

                                count.increment();
                                return true;
                            });
                }
            }
        }

        return count.sum();
    }

    private void search(final ChronicleMap<String, V> db, final List<Search> filters, final int limit,
            final Map<String, V> result, final AtomicInteger counter) {
        Logger.info("Searching DB at [{}] for {} filters.", dataPath(), filters.size());

        db.forEachEntryWhile(entry -> {
            try {
                if (counter.get() >= limit) {
                    return false;
                }

                final String key = entry.key().get();
                final V value = entry.value().get();
                if (value == null) {
                    return true;
                }

                for (final Search search : filters) {
                    if (!CHRONICLE_UTILS.search(search, key, value, averageValue().getClass())) {
                        return true;
                    }
                }

                if (counter.incrementAndGet() <= limit) {
                    result.put(key, db.getUsing(key, using()));
                }
            } catch (final Throwable e) {
                Logger.error("Search failed during multi-filter scan.");
                Logger.error(e);
            }
            return true;
        });
    }

    private void searchCsv(final ChronicleMap<String, V> db, final List<Search> filters, final int limit,
            final ConcurrentLinkedQueue<Object[]> rowQueue, final AtomicInteger counter,
            final AtomicReference<String[]> headers) {
        Logger.info("Searching DB at [{}] for {} filters.", dataPath(), filters.size());
        final var averageValueClass = averageValue().getClass();

        db.forEachEntryWhile(entry -> {
            try {
                if (counter.get() >= limit) {
                    return false;
                }

                final String key = entry.key().get();
                final V value = entry.value().get();
                if (value == null) {
                    return true;
                }

                for (final Search search : filters) {
                    if (!CHRONICLE_UTILS.search(search, key, value, averageValue().getClass())) {
                        return true;
                    }
                }

                if (counter.incrementAndGet() <= limit) {
                    headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(averageValueClass, value));
                    rowQueue.add(CHRONICLE_UTILS.getRowFromObject(averageValueClass, key, value));
                }
            } catch (final Throwable e) {
                Logger.error("Search failed during multi-filter scan.");
                Logger.error(e);
            }
            return true;
        });
    }

    private void searchSubset(final ChronicleMap<String, V> db, final List<Search> filters, final int limit,
            final Map<String, LinkedHashMap<String, Object>> result, final AtomicInteger counter,
            final String[] fields) {
        Logger.info("Subset searching DB at [{}] for {} filters.", dataPath(), filters.size());

        db.forEachEntryWhile(entry -> {
            try {
                if (counter.get() >= limit) {
                    return false;
                }

                final String key = entry.key().get();
                final V value = entry.value().get();
                if (value == null) {
                    return true;
                }

                for (final Search search : filters) {
                    if (!CHRONICLE_UTILS.search(search, key, value, averageValue().getClass())) {
                        return true;
                    }
                }

                if (counter.incrementAndGet() <= limit) {
                    CHRONICLE_UTILS.subsetOfValues(fields, key, value, result, name(), averageValue().getClass());
                }
            } catch (final Throwable e) {
                Logger.error("Search subset failed during multi-filter scan.");
                Logger.error(e);
            }
            return true;
        });
    }

    private void searchSubsetCsv(final ChronicleMap<String, V> db, final List<Search> filters, final int limit,
            final ConcurrentLinkedQueue<Object[]> rowQueue, final AtomicInteger counter, final String[] fields) {
        Logger.info("Subset CSV searching DB at [{}] for {} filters.", dataPath(), filters.size());
        final var headers = CHRONICLE_UTILS.getCsvHeaders(fields);

        db.forEachEntryWhile(entry -> {
            try {
                if (counter.get() >= limit) {
                    return false;
                }

                final String key = entry.key().get();
                final V value = entry.value().get();
                if (value == null) {
                    return true;
                }

                for (final Search search : filters) {
                    if (!CHRONICLE_UTILS.search(search, key, value, averageValue().getClass())) {
                        return true;
                    }
                }

                if (counter.incrementAndGet() <= limit) {
                    rowQueue.add(CHRONICLE_UTILS.subsetOfValuesToRow(headers, key, value, name(),
                            averageValue().getClass()));
                }
            } catch (final Throwable e) {
                Logger.error("Search subset failed during multi-filter scan.");
                Logger.error(e);
            }
            return true;
        });
    }

    private void searchKeys(final ChronicleMap<String, V> db, final List<Search> filters,
            final Collection<String> result) {
        Logger.info("Searching DB keys at [{}] for {} filters.", dataPath(), filters.size());

        db.forEachEntryWhile(entry -> {
            try {
                final String key = entry.key().get();
                final V value = entry.value().get();
                if (value == null) {
                    return true;
                }

                for (final Search search : filters) {
                    if (!CHRONICLE_UTILS.search(search, key, value, averageValue().getClass())) {
                        return true;
                    }
                }

                result.add(key);
            } catch (final Throwable e) {
                Logger.error("Search failed during key scan.");
                Logger.error(e);
            }
            return true;
        });
    }

    private void search(final ChronicleMap<String, V> db, final List<Search> filters, final Set<String> excludedKeys,
            final int limit, final Map<String, V> result, final AtomicInteger counter) {
        Logger.info("Searching DB at [{}] using [{}] filters and [{}] excluded keys. Limit [{}]",
                dataPath(), filters.size(), excludedKeys.size(), limit);

        db.forEachEntryWhile(entry -> {
            try {
                if (counter.get() >= limit) {
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
                    if (!CHRONICLE_UTILS.search(search, key, value, averageValue().getClass())) {
                        return true;
                    }
                }

                if (counter.incrementAndGet() <= limit) {
                    result.put(key, db.getUsing(key, using()));
                }
            } catch (final Throwable e) {
                Logger.error("Search failed during multi-filter scan.");
                Logger.error(e);
            }
            return true;
        });
    }

    private void searchCsv(final ChronicleMap<String, V> db, final List<Search> filters, final Set<String> excludedKeys,
            final int limit, final ConcurrentLinkedQueue<Object[]> rowQueue, final AtomicInteger counter,
            final AtomicReference<String[]> headers) {
        Logger.info("Searching DB at [{}] using [{}] filters and [{}] excluded keys. Limit [{}]",
                dataPath(), filters.size(), excludedKeys.size(), limit);
        final var averageValueClass = averageValue().getClass();

        db.forEachEntryWhile(entry -> {
            try {
                if (counter.get() >= limit) {
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
                    if (!CHRONICLE_UTILS.search(search, key, value, averageValue().getClass())) {
                        return true;
                    }
                }

                if (counter.incrementAndGet() <= limit) {
                    headers.compareAndSet(null, CHRONICLE_UTILS.getHeadersFromObject(averageValueClass, value));
                    rowQueue.add(CHRONICLE_UTILS.getRowFromObject(averageValueClass, key, value));
                }
            } catch (final Throwable e) {
                Logger.error("Search failed during multi-filter scan.");
                Logger.error(e);
            }
            return true;
        });
    }

    private void searchSubset(final ChronicleMap<String, V> db, final List<Search> filters,
            final Set<String> excludedKeys, final int limit, final Map<String, LinkedHashMap<String, Object>> result,
            final AtomicInteger counter, final String[] fields) {
        Logger.info("Searching DB at [{}] using [{}] filters and [{}] excluded keys. Limit [{}]",
                dataPath(), filters.size(), excludedKeys.size(), limit);

        db.forEachEntryWhile(entry -> {
            try {
                if (counter.get() >= limit) {
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
                    if (!CHRONICLE_UTILS.search(search, key, value, averageValue().getClass())) {
                        return true;
                    }
                }

                if (counter.incrementAndGet() <= limit) {
                    CHRONICLE_UTILS.subsetOfValues(fields, key, db.getUsing(key, using()), result, name(),
                            averageValue().getClass());
                }
            } catch (final Throwable e) {
                Logger.error("Search failed during multi-filter scan.");
                Logger.error(e);
            }
            return true;
        });
    }

    private void searchSubsetCsv(final ChronicleMap<String, V> db, final List<Search> filters,
            final Set<String> excludedKeys, final int limit, final ConcurrentLinkedQueue<Object[]> rowQueue,
            final AtomicInteger counter, final String[] fields) {
        Logger.info("Subset CSV searching DB at [{}] using [{}] filters and [{}] excluded keys. Limit [{}]",
                dataPath(), filters.size(), excludedKeys.size(), limit);
        final var headers = CHRONICLE_UTILS.getCsvHeaders(fields);

        db.forEachEntryWhile(entry -> {
            try {
                if (counter.get() >= limit) {
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
                    if (!CHRONICLE_UTILS.search(search, key, value, averageValue().getClass())) {
                        return true;
                    }
                }

                if (counter.incrementAndGet() <= limit) {
                    rowQueue.add(CHRONICLE_UTILS.subsetOfValuesToRow(headers, key, value, name(),
                            averageValue().getClass()));
                }
            } catch (final Throwable e) {
                Logger.error("Search failed during multi-filter scan.");
                Logger.error(e);
            }
            return true;
        });
    }

    private void searchKeys(final ChronicleMap<String, V> db, final List<Search> filters,
            final Set<String> excludedKeys, final Collection<String> result) {
        Logger.info("Searching DB keys at [{}] using [{}] filters and [{}] excluded keys.",
                dataPath(), filters.size(), excludedKeys.size());

        db.forEachEntryWhile(entry -> {
            try {
                final String key = entry.key().get();
                if (excludedKeys.contains(key)) {
                    return true;
                }

                final V value = entry.value().get();
                if (value == null) {
                    return true;
                }

                for (final Search search : filters) {
                    if (!CHRONICLE_UTILS.search(search, key, value, averageValue().getClass())) {
                        return true;
                    }
                }

                result.add(key);
            } catch (final Throwable e) {
                Logger.error("Search failed during multi-filter scan.");
                Logger.error(e);
            }
            return true;
        });
    }

    private int searchCount(final ChronicleMap<String, V> db, final List<Search> filters, final int limit) {
        Logger.info("Counting DB at [{}] for {} filters.", dataPath(), filters.size());
        final var count = new AtomicInteger();

        db.forEachEntryWhile(entry -> {
            try {
                final String key = entry.key().get();
                final V value = entry.value().get();
                if (value == null) {
                    return true;
                }

                for (final Search search : filters) {
                    if (!CHRONICLE_UTILS.search(search, key, value, averageValue().getClass())) {
                        return true;
                    }
                }

                if (count.incrementAndGet() >= limit) {
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
        if (filters.isEmpty() || db.isEmpty()) {
            return Collections.emptyMap();
        }

        final var size = db.size();
        Logger.info("Searching in-memory map of [{}] with [{}] filters.", size, filters.size());
        final Map<String, V> result = new ConcurrentHashMap<>(Math.min(size, 10_000));

        db.entrySet().parallelStream().forEach(entry -> {
            final String key = entry.getKey();
            final V value = entry.getValue();

            if (value == null) {
                return;
            }

            boolean match = true;
            for (final Search search : filters) {
                try {
                    if (!CHRONICLE_UTILS.search(search, key, value, averageValue().getClass())) {
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

    default Map<String, V> indexedSearch(final Search search) throws Exception {
        final String indexPath = getIndexPath(search.field());
        try (final var sharedIndexMap = MAP_DB.openIndex(indexPath)) {
            final var searchResult = indexedSearch(search, sharedIndexMap.index);
            if (isResultEmpty(searchResult.results())) {
                return Collections.emptyMap();
            }
            return get(searchResult.results());
        }
    }

    default Set<String> indexedSearchKeys(final Search search) throws Exception {
        final String indexPath = getIndexPath(search.field());
        try (final var sharedIndexMap = MAP_DB.openIndex(indexPath)) {
            final var searchResult = indexedSearch(search, sharedIndexMap.index);
            if (isResultEmpty(searchResult.results())) {
                return Collections.emptySet();
            }
            return toSetOfKeys(searchResult.results());
        }
    }

    default List<String> indexedSearchKeysList(final Search search) throws Exception {
        final String indexPath = getIndexPath(search.field());
        try (final var sharedIndexMap = MAP_DB.openIndex(indexPath)) {
            final var searchResult = indexedSearch(search, sharedIndexMap.index);
            if (isResultEmpty(searchResult.results())) {
                return Collections.emptyList();
            }
            return toListOfKeys(searchResult.results());
        }
    }

    default Map<String, V> indexedSearch(final Search search, final Set<String> excludedKeys) throws Exception {
        final String indexPath = getIndexPath(search.field());
        try (final var sharedIndexMap = MAP_DB.openIndex(indexPath)) {
            final var searchResult = indexedSearch(search, sharedIndexMap.index, excludedKeys);
            if (isResultEmpty(searchResult.results())) {
                return Collections.emptyMap();
            }
            return get(searchResult.results());
        }
    }

    default Set<String> indexedSearchKeys(final Search search, final Set<String> excludedKeys) throws Exception {
        final String indexPath = getIndexPath(search.field());
        try (final var sharedIndexMap = MAP_DB.openIndex(indexPath)) {
            final var searchResult = indexedSearch(search, sharedIndexMap.index, excludedKeys);
            if (isResultEmpty(searchResult.results())) {
                return Collections.emptySet();
            }
            return toSetOfKeys(searchResult.results());
        }
    }

    default List<String> indexedSearchKeysList(final Search search, final Set<String> excludedKeys) throws Exception {
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

    default Map<String, V> search(final Map<String, V> db, final Search search) throws IOException {
        if (db == null || db.isEmpty()) {
            return Collections.emptyMap();
        }
        return search(db, List.of(search));
    }

    default Map<String, V> search(final Search search) throws IOException {
        final Map<String, V> result = new ConcurrentHashMap<>();
        final int limit = search.limit() == -1 ? HARD_LIMIT : search.limit();
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

    default Set<String> searchKeys(final List<Search> searches) throws IOException {
        final var results = ConcurrentHashMap.<String>newKeySet();
        getDataFileState().fileNames().parallelStream()
                .forEach(file -> {
                    try (final var shared = openDb(file)) {
                        searchKeys(shared.map, searches, results);
                    }
                });
        return results;
    }

    default List<String> searchKeysList(final List<Search> searches) throws IOException {
        final var result = new ConcurrentLinkedQueue<String>();
        getDataFileState().fileNames().parallelStream()
                .forEach(file -> {
                    try (final var shared = openDb(file)) {
                        searchKeys(shared.map, searches, result);
                    }
                });
        return new ArrayList<>(result);
    }

    default Map<String, V> search(final Search search, final Set<String> excludedKeys) throws IOException {
        final Map<String, V> result = new ConcurrentHashMap<>();
        final int limit = search.limit() == -1 ? HARD_LIMIT : search.limit();
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

    default Set<String> searchKeys(final List<Search> searches, final Set<String> excludedKeys) throws IOException {
        final var results = ConcurrentHashMap.<String>newKeySet();
        getDataFileState().fileNames().parallelStream()
                .forEach(file -> {
                    try (final var shared = openDb(file)) {
                        searchKeys(shared.map, searches, excludedKeys, results);
                    }
                });
        return results;
    }

    default List<String> searchKeysList(final List<Search> searches, final Set<String> excludedKeys)
            throws IOException {
        final var result = new ConcurrentLinkedQueue<String>();
        getDataFileState().fileNames().parallelStream().forEach(file -> {
            try (final var shared = openDb(file)) {
                searchKeys(shared.map, searches, excludedKeys, result);
            }
        });
        return new ArrayList<>(result);
    }

    default Map<String, LinkedHashMap<String, Object>> multiSearchSubset(final List<Search> searches,
            final String[] fields) throws Throwable {
        if (searches == null || searches.isEmpty()) {
            return Collections.emptyMap();
        }

        final Set<String> indexFileNames = indexFileNames();

        // Step 1: Separate searches into first-indexed and remaining
        Search indexedSearch = null;
        final List<Search> remainingSearches = new ArrayList<>();

        for (final Search s : searches) {
            if (!s.skipIndex() && indexedSearch == null &&
                    indexFileNames.contains(s.field())) {
                indexedSearch = s;
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min()
                .orElse(HARD_LIMIT);

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
                final var result = new ConcurrentHashMap<String, LinkedHashMap<String, Object>>(10_000);
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
        final List<Search> remainingSearches = new ArrayList<>();

        for (final Search s : searches) {
            if (!s.skipIndex() && indexedSearch == null &&
                    indexFileNames.contains(s.field())) {
                indexedSearch = s;
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min()
                .orElse(HARD_LIMIT);

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
        final List<Search> remainingSearches = new ArrayList<>();

        for (final Search s : searches) {
            if (!s.skipIndex() && indexedSearch == null && indexFileNames.contains(s.field())) {
                indexedSearch = s;
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min()
                .orElse(HARD_LIMIT);

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
                final Map<String, V> result = new ConcurrentHashMap<>();
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
        final List<Search> remainingSearches = new ArrayList<>();

        for (final Search s : searches) {
            if (!s.skipIndex() && indexedSearch == null && indexFileNames.contains(s.field())) {
                indexedSearch = s;
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min()
                .orElse(HARD_LIMIT);

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
        final List<Search> remainingSearches = new ArrayList<>();

        for (final Search s : searches) {
            if (!s.skipIndex() && indexedSearch == null && indexFileNames.contains(s.field())) {
                indexedSearch = s;
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(HARD_LIMIT);

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
                final Map<String, V> result = new ConcurrentHashMap<>();
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
        final List<Search> remainingSearches = new ArrayList<>();

        for (final Search s : searches) {
            if (!s.skipIndex() && indexedSearch == null && indexFileNames.contains(s.field())) {
                indexedSearch = s;
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(HARD_LIMIT);

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

    default Map<String, LinkedHashMap<String, Object>> multiSearchSubset(final List<Search> searches,
            final Set<String> excludedKeys, final String[] fields) throws Throwable {
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
        final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(HARD_LIMIT);

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
                final var result = new ConcurrentHashMap<String, LinkedHashMap<String, Object>>(
                        Math.min(10_000, limit));
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
        final List<Search> remainingSearches = new ArrayList<>();

        for (final Search s : searches) {
            if (!s.skipIndex() && indexedSearch == null && indexFileNames.contains(s.field())) {
                indexedSearch = s;
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(HARD_LIMIT);

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

    default Map<String, LinkedHashMap<String, Object>> multiSearchSubset(final Set<String> matchingKeys,
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
        final List<Search> remainingSearches = new ArrayList<>();

        for (final Search s : searches) {
            if (indexedSearch == null && indexFileNames.contains(s.field())) {
                indexedSearch = s;
            } else {
                remainingSearches.add(s);
            }
        }
        final int limit = searches.stream().mapToInt(Search::limit).filter(l -> l > 0).min().orElse(Integer.MAX_VALUE);

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
                    return MAP_DB.fastCount(searchResult.results(), limit);
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
                                return searchCount(shared.map, searches, limit);
                            }
                        })
                        .sum();
            } else {
                return searchCount(searchResult.results(), remainingSearches, limit);
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
     * @throws IOException
     */
    default int size() throws IOException {
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
    }

    default boolean exists(final String key) throws IOException {
        Logger.info("Checking [{}] existence at [{}].", key, dataPath());

        if (getDataFileState().fileNames().size() > 1) {
            return getKeyMapExists(key);
        }

        try (var shared = openDb()) {
            return shared.map.containsKey(key);
        }
    }

    default Map<String, Boolean> existsMultiple(final Set<String> keys) throws IOException {
        Logger.info("Checking [{}] keys existence at [{}].", keys.size(), dataPath());

        if (getDataFileState().fileNames().size() > 1) {
            return getKeyMapExists(keys);
        }

        final var containsMap = new HashMap<String, Boolean>();
        try (var shared = openDb()) {
            for (final var key : keys) {
                containsMap.put(key, shared.map.containsKey(key));
            }
        }

        return containsMap;
    }

    /**
     * Returns only the keys that exist
     */
    default List<String> existsList(final Set<String> keys) throws IOException {
        Logger.info("Checking [{}] keys existence at [{}].", keys.size(), dataPath());

        if (getDataFileState().fileNames().size() > 1) {
            return getKeyMapExistsList(keys);
        }

        final var list = new ArrayList<String>(keys.size());
        try (var shared = openDb()) {
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
    default List<String> notExistsList(final Set<String> keys) throws IOException {
        Logger.info("Checking [{}] keys non-existence at [{}].", keys.size(), dataPath());

        if (getDataFileState().fileNames().size() > 1) {
            return getKeyMapNotExistsList(keys);
        }

        final var list = new ArrayList<String>(keys.size());
        try (var shared = openDb()) {
            for (final var key : keys) {
                if (!shared.map.containsKey(key)) {
                    list.add(key);
                }
            }
        }

        return list;
    }
}
