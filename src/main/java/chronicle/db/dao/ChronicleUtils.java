package chronicle.db.dao;

import static chronicle.db.service.MapDb.MAP_DB;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.mapdb.DBException;
import org.tinylog.Logger;

import chronicle.db.entity.Search;
import chronicle.db.entity.Search.SearchType;
import chronicle.db.service.MapDb.SharedIndexMap;
import net.openhft.chronicle.map.ChronicleMap;

/**
 * Utility class providing core functionality for ChronicleDao operations.
 * <p>
 * This singleton contains helper methods for:
 * <ul>
 * <li><b>Reflection & Field Access:</b> High-performance field access using
 * VarHandles and MethodHandles</li>
 * <li><b>Search Operations:</b> Manual filtering logic for non-indexed
 * searches</li>
 * <li><b>Index Management:</b> Building, updating, and removing secondary
 * indexes</li>
 * <li><b>CSV Operations:</b> Converting entities to CSV rows and headers</li>
 * <li><b>Data Migration:</b> Moving records between different entity
 * versions</li>
 * <li><b>File Operations:</b> File management utilities</li>
 * <li><b>Type Conversion:</b> Converting between types (enums, numbers,
 * strings)</li>
 * </ul>
 * </p>
 * 
 * <p>
 * <b>Performance Optimizations:</b>
 * <ul>
 * <li>Uses VarHandles for fast field access (faster than reflection)</li>
 * <li>Caches MethodHandles and field metadata per class</li>
 * <li>Parallel processing for batch operations</li>
 * <li>Thread-local StringBuilder for string concatenation</li>
 * </ul>
 * </p>
 * 
 * <p>
 * <b>Internal Use:</b> This class is primarily used internally by ChronicleDao.
 * Most applications should not need to interact with it directly.
 * </p>
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public final class ChronicleUtils {
    public static final ChronicleUtils CHRONICLE_UTILS = new ChronicleUtils();
    private static final int processors = Runtime.getRuntime().availableProcessors();

    private static class FieldData {
        final Field field;
        final VarHandle varHandle;

        FieldData(final Field field, final VarHandle varHandle) {
            this.field = field;
            this.varHandle = varHandle;
        }
    }

    private static class ClassData {
        final Map<String, FieldData> fields = new ConcurrentHashMap<>();
        final MethodHandle headerHandle;
        final MethodHandle rowHandle;
        final MethodHandle subsetHandle;
        final MethodHandle subsetRowHandle;

        ClassData(final Class<?> clazz) {
            try {
                final MethodHandles.Lookup lookup = MethodHandles.lookup();
                this.headerHandle = lookup.findVirtual(clazz, "header", MethodType.methodType(String[].class));
                this.rowHandle = lookup.findVirtual(clazz, "row", MethodType.methodType(Object[].class, String.class));
                this.subsetHandle = lookup.findVirtual(clazz, "subset",
                        MethodType.methodType(Map.class, String[].class));
                this.subsetRowHandle = lookup.findVirtual(clazz, "subsetRow",
                        MethodType.methodType(Object[].class, String.class, String[].class));
            } catch (NoSuchMethodException | IllegalAccessException e) {
                throw new RuntimeException("Failed to initialize MethodHandles for " + clazz.getSimpleName(), e);
            }
        }
    }

    private static final Map<Class<?>, ClassData> CLASS_DATA_CACHE = new ConcurrentHashMap<>();

    /**
     * Retrieves or creates cached ClassData for the given class.
     * ClassData contains MethodHandles for optimized reflection.
     *
     * @param clazz The class to retrieve data for.
     * @return The cached ClassData instance.
     */
    public ClassData getClassData(final Class<?> clazz) {
        return CLASS_DATA_CACHE.computeIfAbsent(clazz, ClassData::new);
    }

    /**
     * Retrieves or creates cached FieldData for a specific field in a class.
     * FieldData contains the Field object and its VarHandle.
     *
     * @param clazz     The class containing the field.
     * @param fieldName The name of the field.
     * @return The FieldData instance, or null if the field does not exist.
     */
    private FieldData getFieldData(final Class<?> clazz, final String fieldName) {
        final ClassData classData = getClassData(clazz);
        return classData.fields.computeIfAbsent(fieldName, f -> {
            try {
                final Field field = clazz.getField(f);
                final VarHandle varHandle = MethodHandles.lookup().unreflectVarHandle(field);
                return new FieldData(field, varHandle);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                Logger.warn("No such field [{}] in class [{}].", f, clazz.getSimpleName());
                return null;
            }
        });
    }

    /**
     * Retrieve a list of files in a dirPath and throw an exception is dirPath is
     * null
     *
     * @param dirPath dirPath to retrieve files from
     * @return a list of files
     */
    public List<String> getFileList(final String dirPath) throws IOException {
        try (final var stream = Files.list(Path.of(dirPath))) {
            return stream.map(Path::getFileName).map(Path::toString).collect(Collectors.toList());
        }
    }

    /**
     * Compares two objects. Handles Numbers specifically as Doubles, otherwise uses
     * String comparison.
     *
     * @param obj1 The first object.
     * @param obj2 The second object.
     * @return A negative integer, zero, or a positive integer as the first argument
     *         is less than, equal to, or greater than the second.
     */
    public int compare(final Object obj1, final Object obj2) {
        if (obj1 instanceof final Number n1 && obj2 instanceof final Number n2) {
            return Double.compare(n1.doubleValue(), n2.doubleValue());
        }
        return String.valueOf(obj1).compareTo(String.valueOf(obj2));
    }

    /**
     * Checks if a string representation of an object contains a search term,
     * ignoring case.
     *
     * @param str        The object to search within.
     * @param searchTerm The term to search for.
     * @return true if the string contains the search term, false otherwise.
     */
    public boolean containsIgnoreCase(final Object str, final Object searchTerm) {
        final String strValue = String.valueOf(str);
        final String termValue = String.valueOf(searchTerm);
        final int termLen = termValue.length();

        if (termLen == 0)
            return true;

        for (int i = 0; i <= strValue.length() - termLen; i++) {
            if (strValue.regionMatches(true, i, termValue, 0, termLen)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if a string representation of an object starts with a search term,
     * ignoring case.
     *
     * @param value      The object to check.
     * @param searchTerm The prefix to search for.
     * @return true if the string starts with the search term, false otherwise.
     */
    public boolean startsWithIgnoreCase(final Object value, final Object searchTerm) {
        final String str = String.valueOf(value);
        final String term = String.valueOf(searchTerm);
        return str.regionMatches(true, 0, term, 0, term.length());
    }

    /**
     * Checks if a string representation of an object ends with a search term,
     * ignoring case.
     *
     * @param value      The object to check.
     * @param searchTerm The suffix to search for.
     * @return true if the string ends with the search term, false otherwise.
     */
    public boolean endsWithIgnoreCase(final Object value, final Object searchTerm) {
        final String str = String.valueOf(value);
        final String term = String.valueOf(searchTerm);
        return str.regionMatches(true, str.length() - term.length(), term, 0, term.length());
    }

    /**
     * Converts an object value to a specific Enum type.
     *
     * @param enumClass The Enum class.
     * @param value     The value to convert (usually a String).
     * @return The Enum constant, or null if conversion fails.
     */
    public Enum toEnum(final Class<?> enumClass, final Object value) {
        try {
            return Enum.valueOf((Class<Enum>) enumClass, value.toString());
        } catch (final Exception e) {
            return null;
        }
    }

    /**
     * Prepares a set of search terms for non-indexed search, converting types if
     * necessary.
     *
     * @param searchTerms The list of raw search terms.
     * @param fieldClass  The target field type.
     * @return A Set of converted search terms.
     */
    private Set<Object> setSearchTermNonIndexed(final List<Object> searchTerms, final Class<?> fieldClass) {
        final Set<Object> searchTermSet = new HashSet<>(1000);
        searchTermSet.clear();
        for (final Object searchTerm : searchTerms) {
            if (fieldClass.isEnum() && searchTerm instanceof String) {
                searchTermSet.add(toEnum(fieldClass, searchTerm));
            } else if (fieldClass == long.class
                    && (searchTerm instanceof String || searchTerm instanceof Integer)) {
                searchTermSet.add(Long.parseLong(searchTerm.toString()));
            } else {
                searchTermSet.add(searchTerm);
            }
        }
        return searchTermSet;
    }

    /**
     * Prepares a single search term for non-indexed search, converting types if
     * necessary.
     *
     * @param searchTerm The raw search term.
     * @param fieldClass The target field type.
     * @return The converted search term.
     */
    private Object setSearchTermNonIndexed(final Object searchTerm, final Class<?> fieldClass) {
        if (fieldClass.isEnum() && (searchTerm instanceof String)) {
            return toEnum(fieldClass, searchTerm);
        } else if (fieldClass == long.class && (searchTerm instanceof String || searchTerm instanceof Integer)) {
            return Long.parseLong(searchTerm.toString());
        }

        return searchTerm;
    }

    /**
     * Checks if an array contains any element from a search set.
     *
     * @param array     The array to check.
     * @param searchSet The set of elements to look for.
     * @return true if any element is found, false otherwise.
     */
    private boolean containsAny(final Object[] array, final Set<Object> searchSet) {
        for (final Object obj : array) {
            if (searchSet.contains(obj)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if an array contains none of the elements from a search set.
     *
     * @param array     The array to check.
     * @param searchSet The set of elements to avoid.
     * @return true if none of the elements are found, false otherwise.
     */
    private boolean containsNone(final Object[] array, final Set<Object> searchSet) {
        for (final Object obj : array) {
            if (searchSet.contains(obj)) {
                return false; // Found one, so NOT "none"
            }
        }
        return true;
    }

    /**
     * Evaluates if a value matches a search criteria based on the search type.
     *
     * @param currentValue      The value to check.
     * @param searchTerm        The single search term (for EQUAL, LIKE, etc.).
     * @param searchTermSet     The set of search terms (for IN, CONTAINS, etc.).
     * @param searchTermBetween The range of search terms (for BETWEEN).
     * @param searchType        The type of search operation.
     * @return true if the value matches the criteria.
     */
    private boolean matchesSearch(final Object currentValue, final Object searchTerm,
            final Set<Object> searchTermSet, final List<Object> searchTermBetween,
            final SearchType searchType) {
        return switch (searchType) {
            case EQUAL -> Objects.equals(currentValue, searchTerm);
            case NOT_EQUAL -> !Objects.equals(currentValue, searchTerm);
            case LESS -> compare(currentValue, searchTerm) < 0;
            case GREATER -> compare(currentValue, searchTerm) > 0;
            case LESS_OR_EQUAL -> compare(currentValue, searchTerm) <= 0;
            case GREATER_OR_EQUAL -> compare(currentValue, searchTerm) >= 0;
            case LIKE -> containsIgnoreCase(currentValue, searchTerm);
            case NOT_LIKE -> !containsIgnoreCase(currentValue, searchTerm);
            case CONTAINS -> containsAny((Object[]) currentValue, searchTermSet);
            case NOT_CONTAINS -> containsNone((Object[]) currentValue, searchTermSet);
            case STARTS_WITH -> startsWithIgnoreCase(currentValue, searchTerm);
            case ENDS_WITH -> endsWithIgnoreCase(currentValue, searchTerm);
            case IN -> searchTermSet.contains(currentValue);
            case NOT_IN -> !searchTermSet.contains(currentValue);
            case BETWEEN -> compare(currentValue, searchTermBetween.get(0)) >= 0
                    && compare(currentValue, searchTermBetween.get(1)) <= 0;
            default -> false;
        };
    }

    /**
     * Safely adds an entry to a shared index, handling potential corruption by
     * resetting the index.
     *
     * @param sharedIndexMap The shared index map.
     * @param add            The byte array to add.
     * @param indexPath      The path to the index file (for recovery).
     * @return true if successful, false if the index was corrupted and reset.
     */
    private boolean safeIndexAdd(final SharedIndexMap sharedIndexMap, final byte[] add, final String indexPath) {
        try {
            sharedIndexMap.index.add(add);
            return true;
        } catch (final DBException.PointerChecksumBroken e) {
            Logger.warn("PointerChecksumBroken on index {} at [{}] when adding. Refreshing.",
                    Arrays.toString(MAP_DB.extractIndexValueAndKey(add)), indexPath);
            MAP_DB.closeIndex(indexPath);
            deleteFileIfExists(indexPath);
            return false;
        }
    }

    public static class PreparedSearch {
        final List<FieldData> fields;
        final SearchType searchType;
        final Object searchTerm;
        final Set<Object> searchTermSet;
        final List<Object> searchTermBetween;

        PreparedSearch(final List<FieldData> fields, final SearchType searchType, final Object searchTerm,
                final Set<Object> searchTermSet, final List<Object> searchTermBetween) {
            this.fields = fields;
            this.searchType = searchType;
            this.searchTerm = searchTerm;
            this.searchTermSet = searchTermSet;
            this.searchTermBetween = searchTermBetween;
        }
    }

    public PreparedSearch prepareSearch(final Search search, final Class<?> valueClass) {
        final String[] fieldNames = search.field().split("\\|");
        final List<FieldData> fields = new ArrayList<>(fieldNames.length);
        Class<?> fieldType = String.class;

        for (final String fieldName : fieldNames) {
            final FieldData fd = getFieldData(valueClass, fieldName);
            if (fd != null) {
                fields.add(fd);
                fieldType = fd.field.getType();
            }
        }

        final SearchType searchType = search.searchType();
        Object searchTerm = null;
        Set<Object> searchTermSet = null;
        List<Object> searchTermBetween = null;

        if (searchType == SearchType.IN || searchType == SearchType.NOT_IN) {
            searchTermSet = setSearchTermNonIndexed((List<Object>) search.searchTerm(), fieldType);
        } else if (searchType == SearchType.BETWEEN) {
            searchTermBetween = (List<Object>) search.searchTerm();
        } else {
            searchTerm = setSearchTermNonIndexed(search.searchTerm(), fieldType);
        }

        return new PreparedSearch(fields, searchType, searchTerm, searchTermSet, searchTermBetween);
    }

    public <V> boolean search(final PreparedSearch search, final String key, final V value) {
        for (final FieldData fieldData : search.fields) {
            final Object currentValue = fieldData.varHandle.get(value);
            if (matchesSearch(currentValue, search.searchTerm, search.searchTermSet, search.searchTermBetween,
                    search.searchType)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Safely removes an entry from a shared index, handling potential corruption by
     * resetting the index.
     *
     * @param sharedIndexMap The shared index map.
     * @param remove         The byte array to remove.
     * @param indexPath      The path to the index file (for recovery).
     * @return true if successful, false if the index was corrupted and reset.
     */
    private boolean safeIndexRemove(final SharedIndexMap sharedIndexMap, final byte[] remove, final String indexPath) {
        try {
            sharedIndexMap.index.remove(remove);
            return true;
        } catch (final DBException.PointerChecksumBroken e) {
            Logger.warn("PointerChecksumBroken on index {} at [{}] when removing. Refreshing.",
                    Arrays.toString(MAP_DB.extractIndexValueAndKey(remove)), indexPath);
            MAP_DB.closeIndex(indexPath);
            deleteFileIfExists(indexPath);
            return false;
        }
    }

    /**
     * Index the db so that joins for 1 to many are efficient.
     * 
     * @param db     the db object being indexed
     * @param dbName the db name
     * @param field  the field enum from the value object
     * @return boolean true/false if indexed
     * @throws IOException
     * 
     */
    public <V> void index(final ChronicleMap<String, V> db, final String dbName, final Set<String> fields,
            final String dataPath, final Class<?> valueClass, final Map<String, Set<String>> exclusions) {
        final int BATCH_SIZE = 100_000;
        final var indexDirPath = dataPath + ChronicleDao.INDEX_DIR;

        final Map<String, List<FieldData>> indexFieldMap = new HashMap<>();
        for (final String rawField : fields) {
            final String[] parts = rawField.split("\\+");
            final List<FieldData> getters = new ArrayList<>();
            for (final String part : parts) {
                getters.add(getFieldData(valueClass, part));
            }
            if (!getters.isEmpty()) {
                indexFieldMap.put(rawField, getters);
            }
        }

        final Map<String, SharedIndexMap> openIndexes = new ConcurrentHashMap<>();
        for (final String field : indexFieldMap.keySet()) {
            final String indexPath = indexDirPath + field;
            try {
                openIndexes.put(indexPath, MAP_DB.openIndex(indexPath));
            } catch (final RuntimeException e) {
                Logger.warn("Skipping indexing for [{}]: {}", indexPath, e.getMessage());
            }
        }

        if (db.isEmpty()) {
            Logger.info("DB is empty. Index files created at [{}].", indexDirPath);
            openIndexes.forEach((indexPath, sharedIndexMap) -> {
                sharedIndexMap.close();
            });
            return;
        }

        try {
            final Map<String, Set<byte[]>> fieldBatches = new ConcurrentHashMap<>();
            final AtomicInteger recordCount = new AtomicInteger(0);

            for (final String field : indexFieldMap.keySet()) {
                final String indexPath = indexDirPath + field;
                if (openIndexes.containsKey(indexPath)) {
                    fieldBatches.put(field, ConcurrentHashMap.newKeySet(BATCH_SIZE));
                }
            }

            final StringBuilder sb = new StringBuilder();
            db.forEachEntry(entry -> {
                final var key = entry.key().get();
                final V value = entry.value().get();

                try {
                    for (final Map.Entry<String, List<FieldData>> fieldEntry : indexFieldMap.entrySet()) {
                        final String compoundField = fieldEntry.getKey();
                        final Set<byte[]> batch = fieldBatches.get(compoundField);
                        if (batch == null)
                            continue;

                        final Set<String> excluded = exclusions.getOrDefault(compoundField, Collections.emptySet());
                        final List<FieldData> fieldDataList = fieldEntry.getValue();
                        sb.setLength(0);
                        boolean shouldSkip = false;

                        for (final FieldData fd : fieldDataList) {
                            final var val = String.valueOf(fd.varHandle.get(value));
                            if (excluded.contains(val)) {
                                shouldSkip = true;
                                break;
                            }
                            sb.append(val);
                        }

                        if (!shouldSkip) {
                            final byte[] indexKey = MAP_DB.createIndexKey(sb.toString(), key.toString());
                            batch.add(indexKey);
                        }
                    }

                    if (recordCount.incrementAndGet() % BATCH_SIZE == 0) {
                        fieldBatches.entrySet().parallelStream().forEach(batchEntry -> {
                            final String field = batchEntry.getKey();
                            final Set<byte[]> batch = batchEntry.getValue();
                            if (!batch.isEmpty()) {
                                final var indexPath = indexDirPath + field;
                                final var sharedIndexMap = openIndexes.get(indexPath);
                                batch.parallelStream().forEach(add -> {
                                    safeIndexAdd(sharedIndexMap, add, indexPath);
                                });
                                sharedIndexMap.commit();
                                batch.clear();
                            }
                        });
                    }
                } catch (final Throwable e) {
                    Logger.error("Error processing key [{}] for fields {}", key, fields);
                    Logger.error(e);
                }
            });

            // Flush remaining
            fieldBatches.entrySet().parallelStream().forEach(batchEntry -> {
                final String field = batchEntry.getKey();
                final Set<byte[]> batch = batchEntry.getValue();
                if (!batch.isEmpty()) {
                    final var indexPath = indexDirPath + field;
                    final var sharedIndexMap = openIndexes.get(indexPath);
                    batch.parallelStream().forEach(add -> {
                        safeIndexAdd(sharedIndexMap, add, indexPath);
                    });
                    sharedIndexMap.commit();
                }
            });
            Logger.info("Indexed [{}] records for fields: {} at [{}]", recordCount.get(), indexFieldMap.keySet(),
                    dataPath);
        } finally {
            openIndexes.forEach((indexPath, sharedIndexMap) -> {
                sharedIndexMap.close();
            });
        }
    }

    /**
     * Removes entries from secondary indexes.
     *
     * @param <V>            The type of the value object.
     * @param dbName         The name of the database.
     * @param dataPath       The path to the data directory.
     * @param indexFileNames The set of index file names to update.
     * @param values         The map of key-value pairs to remove from the index.
     * @param valueClass     The class of the value object.
     */
    public <V> void removeFromIndex(final String dbName, final String dataPath, final Set<String> indexFileNames,
            final Map<String, V> values, final Class<?> valueClass) {
        if (values.isEmpty() || indexFileNames.isEmpty()) {
            return;
        }

        final Map<String, SharedIndexMap> openIndexes = new ConcurrentHashMap<>();
        try {
            // Step 1: Parse all field getters (supporting compound fields)
            final Map<String, List<FieldData>> indexFieldMap = new HashMap<>();
            for (final String indexName : indexFileNames) {
                final String[] parts = indexName.split("\\+");
                final List<FieldData> getters = new ArrayList<>();

                for (final String part : parts) {
                    getters.add(getFieldData(valueClass, part));
                }
                indexFieldMap.put(indexName, getters);
                final String indexPath = dataPath + ChronicleDao.INDEX_DIR + indexName;
                try {
                    openIndexes.put(indexPath, MAP_DB.openIndex(indexPath));
                } catch (final RuntimeException e) {
                    Logger.warn("Skipping index removal for [{}]: {}", indexPath, e.getMessage());
                }
            }

            // Step 2: Remove from each index
            indexFieldMap.entrySet().parallelStream().forEach(entry -> {
                final String indexName = entry.getKey();
                final List<FieldData> fieldGetters = entry.getValue();
                final String indexPath = dataPath + ChronicleDao.INDEX_DIR + indexName;
                final var sharedIndexMap = openIndexes.get(indexPath);

                final ThreadLocal<StringBuilder> sbThreadLocal = ThreadLocal.withInitial(() -> new StringBuilder());
                final var failed = new AtomicBoolean(false);
                final var recordCount = new AtomicInteger();

                values.entrySet().parallelStream().forEach(e -> {
                    if (failed.get()) {
                        return; // Exit early if already failed
                    }

                    final var key = e.getKey();
                    final V value = e.getValue();

                    final var sb = sbThreadLocal.get();
                    sb.setLength(0);
                    for (final FieldData fd : fieldGetters) {
                        final var val = String.valueOf(fd.varHandle.get(value));
                        sb.append(val);
                    }

                    if (!safeIndexRemove(sharedIndexMap, MAP_DB.createIndexKey(sb.toString(), key.toString()),
                            indexPath)) {
                        failed.set(true);
                        return;
                    }
                    recordCount.incrementAndGet();
                });

                if (!failed.get() && recordCount.get() != 0) {
                    sharedIndexMap.commit();
                    Logger.info("Removed [{}] records from index at [{}]", recordCount.get(), indexPath);
                }
            });
        } finally {
            openIndexes.forEach((path, sharedIndexMap) -> {
                sharedIndexMap.close();
            });
        }
    }

    /**
     * Updates secondary indexes when records are modified.
     * Handles removing old index entries and adding new ones.
     *
     * @param <V>            The type of the value object.
     * @param dbName         The name of the database.
     * @param dataPath       The path to the data directory.
     * @param indexFileNames The set of index file names to update.
     * @param values         The map of new key-value pairs.
     * @param previousValues The map of previous key-value pairs (for comparison).
     * @param valueClass     The class of the value object.
     * @param exclusions     A map of values to exclude from indexing for specific
     *                       fields.
     */
    public <V> void updateIndex(final String dbName, final String dataPath, final Set<String> indexFileNames,
            final Map<String, V> values, final Map<String, V> previousValues, final Class<?> valueClass,
            final Map<String, Set<String>> exclusions) {
        if (values.isEmpty() || indexFileNames.isEmpty()) {
            return;
        }

        final var indexDirPath = dataPath + ChronicleDao.INDEX_DIR;
        final Map<String, SharedIndexMap> openIndexes = new ConcurrentHashMap<>();

        try {
            // Step 1: Parse field getters
            final Map<String, List<FieldData>> indexFieldMap = new HashMap<>();
            for (final String indexName : indexFileNames) {
                final String[] parts = indexName.split("\\+");
                final List<FieldData> getters = new ArrayList<>();

                for (final String part : parts) {
                    getters.add(getFieldData(valueClass, part));
                }
                indexFieldMap.put(indexName, getters);
                final String indexPath = indexDirPath + indexName;
                try {
                    openIndexes.put(indexPath, MAP_DB.openIndex(indexPath));
                } catch (final RuntimeException e) {
                    Logger.warn("Skipping index update for [{}]: {}", indexPath, e.getMessage());
                }
            }

            // Step 2: Update indexes
            indexFieldMap.entrySet().parallelStream().forEach(entry -> {
                final String indexName = entry.getKey();
                final List<FieldData> fieldGetters = entry.getValue();
                final String indexPath = indexDirPath + indexName;
                final var sharedIndexMap = openIndexes.get(indexPath);
                final var recordCount = new AtomicInteger();

                final Set<String> excluded = exclusions.getOrDefault(indexName, Collections.emptySet());
                final ThreadLocal<StringBuilder> sbThreadLocal = ThreadLocal.withInitial(() -> new StringBuilder());
                final var failed = new AtomicBoolean(false);

                values.entrySet().parallelStream().forEach(valEntry -> {
                    if (failed.get()) {
                        return; // Exit early if already failed
                    }

                    final var key = valEntry.getKey();
                    final V newVal = valEntry.getValue();
                    final V prevVal = previousValues.get(key);

                    final var sb = sbThreadLocal.get();
                    sb.setLength(0);
                    boolean skipAdd = false;

                    for (final FieldData fd : fieldGetters) {
                        final var value = String.valueOf(fd.varHandle.get(newVal));
                        if (excluded.contains(value)) {
                            skipAdd = true;
                        }
                        sb.append(value);
                    }
                    final var newValStr = sb.toString();

                    if (prevVal != null) {
                        sb.setLength(0);
                        for (final FieldData fd : fieldGetters) {
                            final var value = String.valueOf(fd.varHandle.get(prevVal));
                            sb.append(value);
                        }
                        final var oldValStr = sb.toString();

                        if (!Objects.equals(oldValStr, newValStr)) {
                            // Always remove if changed (regardless of exclusion)
                            if (!safeIndexRemove(sharedIndexMap, MAP_DB.createIndexKey(oldValStr, key.toString()),
                                    indexPath)) {
                                failed.set(true);
                                return;
                            }
                            // Add new value only if not excluded and not empty
                            if (!skipAdd) {
                                if (!safeIndexAdd(sharedIndexMap, MAP_DB.createIndexKey(newValStr, key.toString()),
                                        indexPath)) {
                                    failed.set(true);
                                    return;
                                }
                            }
                            recordCount.incrementAndGet();
                        }
                    } else {
                        // Add new value only if not excluded
                        if (!skipAdd) {
                            if (!safeIndexAdd(sharedIndexMap, MAP_DB.createIndexKey(newValStr, key.toString()),
                                    indexPath)) {
                                failed.set(true);
                                return;
                            }
                            recordCount.incrementAndGet();
                        }
                    }
                });

                if (!failed.get() && recordCount.get() != 0) {
                    sharedIndexMap.commit();
                    Logger.info("Updated [{}] records for index: [{}]", recordCount.get(), indexName);
                }
            });
        } finally {
            openIndexes.forEach((path, sharedIndexMap) -> {
                sharedIndexMap.close();
            });
        }
    }

    /**
     * Retrieves CSV headers from an object using reflection (via MethodHandle).
     *
     * @param <V>         The type of the value object.
     * @param classData   The cached ClassData for the object type.
     * @param sampleValue A sample object instance.
     * @return An array of header strings.
     */
    public <V> String[] getHeadersFromObject(final ClassData classData, final V sampleValue) {
        try {
            final MethodHandle headersMethod = classData.headerHandle;
            return (String[]) headersMethod.invoke(sampleValue);
        } catch (final Throwable e) {
            // should not happen
            Logger.error("Error when getting headers from object.");
            Logger.error(e);
            return new String[0];
        }
    }

    /**
     * Retrieves a CSV row from an object using reflection (via MethodHandle).
     *
     * @param <V>         The type of the value object.
     * @param classData   The cached ClassData for the object type.
     * @param key         The key associated with the object.
     * @param sampleValue The object instance.
     * @return An array of objects representing the row data.
     */
    public <V> Object[] getRowFromObject(final ClassData classData, final String key, final V sampleValue) {
        try {
            final MethodHandle rowMethod = classData.rowHandle;
            return (Object[]) rowMethod.invoke(sampleValue, key);
        } catch (final Throwable e) {
            // should not happen
            Logger.error("Error when getting row from object.");
            Logger.error(e);
            return new Object[0];
        }
    }

    /**
     * Retrieves a subset of fields from an object as a Map.
     *
     * @param <V>         The type of the value object.
     * @param classData   The cached ClassData for the object type.
     * @param fields      The list of fields to retrieve.
     * @param sampleValue The object instance.
     * @return A Map containing the requested fields.
     */
    public <V> Map<String, Object> getSubsetFromObject(final ClassData classData, final String[] fields,
            final V sampleValue) {
        try {
            final MethodHandle rowMethod = classData.subsetHandle;
            return (Map<String, Object>) rowMethod.invoke(sampleValue, fields);
        } catch (final Throwable e) {
            // should not happen
            Logger.error("Error when getting subset from object.");
            Logger.error(e);
            return Collections.emptyMap();
        }
    }

    /**
     * Retrieves a subset of fields from an object as a row array.
     *
     * @param <V>         The type of the value object.
     * @param classData   The cached ClassData for the object type.
     * @param key         The key associated with the object.
     * @param fields      The list of fields to retrieve.
     * @param sampleValue The object instance.
     * @return An array of objects representing the subset row data.
     */
    public <V> Object[] getSubsetRowFromObject(final ClassData classData, final String key, final String[] fields,
            final V sampleValue) {
        try {
            final MethodHandle rowMethod = classData.subsetRowHandle;
            return (Object[]) rowMethod.invoke(sampleValue, key, fields);
        } catch (final Throwable e) {
            // should not happen
            Logger.error("Error when getting subset row from object [{}].", sampleValue.getClass().getSimpleName());
            Logger.error(e);
            return new Object[0];
        }
    }

    /**
     * Generates CSV headers including an "ID" column.
     *
     * @param fields The list of field names.
     * @return An array of header strings starting with "ID".
     */
    public String[] getCsvHeaders(final String[] fields) {
        final String[] headers = new String[fields.length + 1];
        headers[0] = "ID";
        System.arraycopy(fields, 0, headers, 1, fields.length);
        return headers;
    }

    /**
     * Sets a non-enum value on an object field using VarHandle.
     *
     * @param <V>        The type of the object.
     * @param object     The object instance.
     * @param fieldName  The name of the field to set.
     * @param fieldValue The value to set.
     * @throws Throwable If the field access fails.
     */
    public <V> void setNonEnumValue(final V object, final String fieldName, final Object fieldValue)
            throws Throwable {
        final var fieldData = getFieldData(object.getClass(), fieldName);
        if (fieldData != null)
            fieldData.varHandle.set(object, fieldValue);
    }

    /**
     * Sets a value on an object field, handling Enum conversion if necessary.
     *
     * @param <V>        The type of the object.
     * @param object     The object instance.
     * @param fieldName  The name of the field to set.
     * @param fieldValue The value to set.
     * @throws Throwable If the field access fails.
     */
    public <V> void setObjectValue(final V object, final String fieldName, final Object fieldValue)
            throws Throwable {
        final var fieldData = getFieldData(object.getClass(), fieldName);

        if (fieldData != null) {
            final var type = fieldData.field.getType();
            if (type.isEnum())
                fieldData.varHandle.set(object, toEnum(type, fieldValue));
            else
                fieldData.varHandle.set(object, fieldValue);
        }
    }

    /**
     * Appends a string value to an existing string field on an object.
     *
     * @param <V>        The type of the object.
     * @param object     The object instance.
     * @param fieldName  The name of the field to update.
     * @param fieldValue The string value to append.
     * @throws Throwable If the field access fails.
     */
    public <V> void concatenateObjectValue(final V object, final String fieldName, final String fieldValue)
            throws Throwable {
        final var fieldData = getFieldData(object.getClass(), fieldName);
        if (fieldData != null) {
            final var value = (String) fieldData.varHandle.get(object);
            fieldData.varHandle.set(object, value + fieldValue);
        }
    }

    /**
     * Replaces a substring within a string field on an object.
     *
     * @param <V>        The type of the object.
     * @param object     The object instance.
     * @param fieldName  The name of the field to update.
     * @param fieldValue The replacement string.
     * @param toReplace  The substring to be replaced.
     * @throws Throwable If the field access fails.
     */
    public <V> void replaceObjectValue(final V object, final String fieldName, final String fieldValue,
            final String toReplace) throws Throwable {
        final var fieldData = getFieldData(object.getClass(), fieldName);
        if (fieldData != null) {
            final var value = ((String) fieldData.varHandle.get(object)).replace(toReplace, fieldValue);
            fieldData.varHandle.set(object, value);
        }
    }

    /**
     * Deletes a file if it exists, logging a message if it does not.
     *
     * @param filePath The path to the file.
     */
    public void deleteFileIfExists(final String filePath) {
        try {
            Files.delete(Path.of(filePath));
        } catch (final IOException e) {
            Logger.info("File for deletion does not exist [{}].", filePath);
        }
    }

    /**
     * Moves a file from source to destination, replacing existing files.
     *
     * @param source The source path.
     * @param dest   The destination path.
     */
    public void move(final Path source, final Path dest) {
        try {
            Files.move(source, dest, REPLACE_EXISTING);
        } catch (final IOException e) {
            Logger.error("Error moving from [{}]  to [{}]. {}", source, dest, e);
        }
    }

    /**
     * Moves contents of a directory that start with a specific prefix to a
     * destination directory.
     *
     * @param src        The source directory.
     * @param dest       The destination directory.
     * @param filePrefix The file name prefix to filter by.
     * @throws IOException If an I/O error occurs.
     */
    public void moveDirContentsStartsWith(final Path src, final Path dest, final String filePrefix)
            throws IOException {
        Files.walk(src).filter(path -> !path.equals(src))
                .filter(path -> path.getFileName().toString().startsWith(filePrefix))
                .forEach(source -> move(source, dest.resolve(src.relativize(source))));
    }

    /**
     * Migrate records from one object version to another
     * Usefule when adding/removing fields
     * 
     * @throws InvocationTargetException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws SecurityException
     * @throws NoSuchMethodException
     * @throws ClassNotFoundException
     */
    public <V> Map<String, Object> moveRecords(final ChronicleMap<String, V> currentValues,
            final String fromObjectClass, final String toObjectClass, final Map<String, String> move,
            final Map<String, Object> def)
            throws SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, ClassNotFoundException {
        if (currentValues.isEmpty())
            return new HashMap<>(); // Early exit

        final Map<String, Object> map = new HashMap<>(currentValues.size()); // Pre-size map
        final Class<?> sourceCls = Class.forName(fromObjectClass);
        final Class<?> cls = Class.forName(toObjectClass);
        final Constructor<?> constructor = cls.getConstructor();
        final Field[] fields = sourceCls.getDeclaredFields();
        final Field[] newFields = cls.getDeclaredFields();
        final Set<Field> newFieldsSet = new HashSet<>(Arrays.asList(newFields)); // Faster lookup
        newFieldsSet.removeAll(Arrays.asList(fields));

        // --- Optimization: Pre-compute field mappings with VarHandles ---
        class FieldTransfer {
            final VarHandle srcHandle;
            final VarHandle destHandle;
            final Class<?> destType;
            final Object defValue;
            final boolean isDestEnum;

            FieldTransfer(final Field src, final Field dest, final Object defValue) throws IllegalAccessException {
                this.srcHandle = MethodHandles.lookup().unreflectVarHandle(src);
                this.destHandle = MethodHandles.lookup().unreflectVarHandle(dest);
                this.destType = dest.getType();
                this.defValue = defValue;
                this.isDestEnum = destType.isEnum();
            }
        }

        class DefaultTransfer {
            final VarHandle destHandle;
            final Class<?> destType;
            final Object value;

            DefaultTransfer(final Field dest, final Object value) throws IllegalAccessException {
                this.destHandle = MethodHandles.lookup().unreflectVarHandle(dest);
                this.destType = dest.getType();
                this.value = destType.isEnum() ? toEnum(destType, value) : value;
            }
        }

        final List<FieldTransfer> transfers = new ArrayList<>(fields.length);
        for (final Field field : fields) {
            final String fieldName = field.getName();
            final String destFieldName = move.getOrDefault(fieldName, fieldName);
            final Object defValue = def.get(fieldName);

            try {
                final Field destField = cls.getDeclaredField(destFieldName);
                transfers.add(new FieldTransfer(field, destField, defValue));
            } catch (final NoSuchFieldException e) {
                // Destination field doesn't exist, skip
            }
        }

        final List<DefaultTransfer> defaults = new ArrayList<>();
        for (final Field field : newFieldsSet) {
            final Object defValue = def.get(field.getName());
            if (defValue != null) {
                defaults.add(new DefaultTransfer(field, defValue));
            }
        }
        // ------------------------------------------------

        currentValues.forEachEntry(entry -> {
            try {
                final var key = entry.key().get();
                final V currentVal = entry.value().get();
                final Object newObj = constructor.newInstance();

                // Apply transfers
                for (final FieldTransfer ft : transfers) {
                    final Object fieldVal = ft.srcHandle.get(currentVal);
                    final Object value = ft.defValue != null
                            ? (ft.isDestEnum ? toEnum(ft.destType, ft.defValue) : ft.defValue)
                            : (ft.isDestEnum && fieldVal != null ? toEnum(ft.destType, fieldVal) : fieldVal);
                    ft.destHandle.set(newObj, value);
                }

                // Apply defaults
                for (final DefaultTransfer dt : defaults) {
                    dt.destHandle.set(newObj, dt.value);
                }

                map.put(key, newObj);
            } catch (final IllegalArgumentException | IllegalAccessException
                    | InstantiationException | InvocationTargetException e) {
                Logger.error("Error during migration for [{}]", toObjectClass);
                Logger.error(e);
            }
        });

        return map;
    }

    /**
     * Limits a Map to a specified number of entries.
     *
     * @param <K>        The type of keys.
     * @param <V>        The type of values.
     * @param sourceData The source map.
     * @param limit      The maximum number of entries.
     * @return A new Map containing at most 'limit' entries.
     */
    public <K, V> Map<K, V> limitMapValues(final Map<K, V> sourceData, final int limit) {
        if (sourceData.size() <= limit) {
            return sourceData;
        }

        final var limitedMap = new HashMap<K, V>(limit);
        int count = 0;
        for (final var entry : sourceData.entrySet()) {
            if (count >= limit) {
                break;
            }
            limitedMap.put(entry.getKey(), entry.getValue());
            count++;
        }
        return limitedMap;
    }

    /**
     * Limits a Set to a specified number of entries.
     *
     * @param <K>        The type of elements.
     * @param sourceData The source set.
     * @param limit      The maximum number of entries.
     * @return A new Set containing at most 'limit' entries.
     */
    public <K> Set<K> limitSetValues(final Set<K> sourceData, final int limit) {
        if (sourceData.size() <= limit) {
            return sourceData;
        }

        final var limitedSet = new HashSet<K>(limit);
        int count = 0;
        for (final var key : sourceData) {
            if (count >= limit) {
                break;
            }
            limitedSet.add(key);
            count++;
        }
        return limitedSet;
    }

    /**
     * Limits a List to a specified number of entries.
     *
     * @param <K>        The type of elements.
     * @param sourceData The source list.
     * @param limit      The maximum number of entries.
     * @return A new List containing at most 'limit' entries.
     */
    public <K> List<K> limitListValues(final List<K> sourceData, final int limit) {
        if (sourceData.size() <= limit) {
            return sourceData;
        }

        final var limitedList = new ArrayList<K>(limit);
        int count = 0;
        for (final var key : sourceData) {
            if (count >= limit) {
                break;
            }
            limitedList.add(key);
            count++;
        }
        return limitedList;
    }

    private static final ExecutorService SHARED_EXECUTOR = Executors.newFixedThreadPool(Math.min(processors, 8));

    /**
     * Processes an Iterable in parallel using a shared executor service.
     *
     * @param iterable The iterable to process.
     * @param limit    The maximum number of items to process.
     * @param action   The predicate action to perform on each item.
     * @throws InterruptedException If the thread is interrupted.
     */
    public void parallelIterable(final Iterable<String> iterable, final int limit, final Predicate<String> action)
            throws InterruptedException {
        final AtomicInteger matchCounter = new AtomicInteger(0);
        parallelIterable(iterable, limit, matchCounter, action);
    }

    /**
     * Processes an Iterable in parallel using a shared executor service.
     *
     * @param <T>          The type of elements.
     * @param iterable     The iterable to process.
     * @param limit        The maximum number of items to process.
     * @param matchCounter An atomic counter to track processed items.
     * @param action       The predicate action to perform on each item.
     * @throws InterruptedException If the thread is interrupted.
     */
    public <T> void parallelIterable(final Iterable<T> iterable, final int limit, final AtomicInteger matchCounter,
            final Predicate<T> action) throws InterruptedException {
        if (limit <= 0 || iterable == null) {
            return;
        }

        final var iterator = iterable.iterator();
        final var iteratorLock = new Object();
        final var batchSize = 100;
        // Use shared executor instead of creating new one
        final int consumerThreads = Math.min(processors, 8);
        final var futures = new ArrayList<Future<?>>(consumerThreads);

        for (int i = 0; i < consumerThreads; i++) {
            futures.add(SHARED_EXECUTOR.submit(() -> {
                final var batch = new ArrayList<T>(batchSize);

                while (matchCounter.get() < limit && !Thread.currentThread().isInterrupted()) {
                    // Fetch batch
                    batch.clear();
                    synchronized (iteratorLock) {
                        if (!iterator.hasNext()) {
                            return; // No more data
                        }
                        for (int j = 0; j < batchSize && iterator.hasNext(); j++) {
                            batch.add(iterator.next());
                        }
                    }

                    // Process batch
                    for (final T item : batch) {
                        if (matchCounter.get() >= limit) {
                            return; // Early exit
                        }

                        try {
                            if (action.test(item)) {
                                matchCounter.incrementAndGet();
                                // Note: May slightly exceed limit, acceptable for performance
                            }
                        } catch (final Exception e) {
                            Logger.error("[Parallel Iterable] - Error processing item", e);
                        }
                    }
                }
            }));
        }

        // Wait for all tasks to complete
        for (final Future<?> future : futures) {
            try {
                future.get();
            } catch (final ExecutionException e) {
                Logger.error("[Parallel Iterable] - Consumer thread failed", e.getCause());
            } catch (final CancellationException e) {
                // Expected if cancelled
            }
        }
    }

    /**
     * Concatenates two Iterables into a single Iterable, limited by a maximum
     * count.
     *
     * @param <T>    The type of elements.
     * @param first  The first iterable.
     * @param second The second iterable.
     * @param limit  The maximum number of elements to return.
     * @return A concatenated Iterable.
     */
    public <T> Iterable<T> concatIterable(final Iterable<T> first, final Iterable<T> second, final int limit) {
        return () -> new Iterator<>() {
            private final Iterator<T> firstIterator = first.iterator();
            private final Iterator<T> secondIterator = second.iterator();
            private int remaining = limit != -1 ? limit : Integer.MAX_VALUE;

            @Override
            public boolean hasNext() {
                return remaining > 0 && (firstIterator.hasNext() || secondIterator.hasNext());
            }

            @Override
            public T next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                remaining--;
                return firstIterator.hasNext() ? firstIterator.next() : secondIterator.next();
            }
        };
    }

    /**
     * Executes a list of Runnable tasks in parallel using Virtual Threads.
     *
     * @param tasks The list of tasks to execute.
     */
    public void processInParallel(final List<Runnable> tasks) {
        if (tasks == null || tasks.size() == 0)
            return;

        final List<Thread> threads = new ArrayList<>();
        for (final Runnable task : tasks) {
            if (task != null) {
                threads.add(Thread.ofVirtual().start(task));
            }
        }

        for (final Thread thread : threads) {
            try {
                thread.join();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                Logger.error(e);
            }
        }
    }
}
