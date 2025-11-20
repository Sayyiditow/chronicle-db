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
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
 *   <li><b>Reflection & Field Access:</b> High-performance field access using VarHandles and MethodHandles</li>
 *   <li><b>Search Operations:</b> Manual filtering logic for non-indexed searches</li>
 *   <li><b>Index Management:</b> Building, updating, and removing secondary indexes</li>
 *   <li><b>CSV Operations:</b> Converting entities to CSV rows and headers</li>
 *   <li><b>Data Migration:</b> Moving records between different entity versions</li>
 *   <li><b>File Operations:</b> File management utilities</li>
 *   <li><b>Type Conversion:</b> Converting between types (enums, numbers, strings)</li>
 * </ul>
 * </p>
 * 
 * <p>
 * <b>Performance Optimizations:</b>
 * <ul>
 *   <li>Uses VarHandles for fast field access (faster than reflection)</li>
 *   <li>Caches MethodHandles and field metadata per class</li>
 *   <li>Parallel processing for batch operations</li>
 *   <li>Thread-local StringBuilder for string concatenation</li>
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

    public ClassData getClassData(final Class<?> clazz) {
        return CLASS_DATA_CACHE.computeIfAbsent(clazz, ClassData::new);
    }

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

    public int compare(final Object obj1, final Object obj2) {
        if (obj1 instanceof final Number n1 && obj2 instanceof final Number n2) {
            return Double.compare(n1.doubleValue(), n2.doubleValue());
        }
        return String.valueOf(obj1).compareTo(String.valueOf(obj2));
    }

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

    public boolean startsWithIgnoreCase(final Object value, final Object searchTerm) {
        final String str = String.valueOf(value);
        final String term = String.valueOf(searchTerm);
        return str.regionMatches(true, 0, term, 0, term.length());
    }

    public boolean endsWithIgnoreCase(final Object value, final Object searchTerm) {
        final String str = String.valueOf(value);
        final String term = String.valueOf(searchTerm);
        return str.regionMatches(true, str.length() - term.length(), term, 0, term.length());
    }

    public Enum toEnum(final Class<?> enumClass, final Object value) {
        try {
            return Enum.valueOf((Class<Enum>) enumClass, value.toString());
        } catch (final Exception e) {
            return null;
        }
    }

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

    private Object setSearchTermNonIndexed(final Object searchTerm, final Class<?> fieldClass) {
        if (fieldClass.isEnum() && (searchTerm instanceof String)) {
            return toEnum(fieldClass, searchTerm);
        } else if (fieldClass == long.class && (searchTerm instanceof String || searchTerm instanceof Integer)) {
            return Long.parseLong(searchTerm.toString());
        }

        return searchTerm;
    }

    private boolean containsAny(final Object[] array, final Set<Object> searchSet) {
        for (final Object obj : array) {
            if (searchSet.contains(obj)) {
                return true;
            }
        }
        return false;
    }

    private boolean containsNone(final Object[] array, final Set<Object> searchSet) {
        for (final Object obj : array) {
            if (searchSet.contains(obj)) {
                return false; // Found one, so NOT "none"
            }
        }
        return true;
    }

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

    public <V> boolean search(final Search search, final String key, final V value, final Class<?> valueClass) {
        final String[] fields = search.field().split("\\|");
        final SearchType searchType = search.searchType();

        // Compute once, reuse for all fields
        Set<Object> searchTermSet;
        List<Object> searchTermBetween;
        Object searchTerm;

        for (final String field : fields) {
            final FieldData fieldData = getFieldData(valueClass, field);
            if (fieldData == null)
                continue;

            final var fieldType = fieldData.field.getType();
            if (searchType == SearchType.IN || searchType == SearchType.NOT_IN
                    || searchType == SearchType.CONTAINS || searchType == SearchType.NOT_CONTAINS) {
                searchTermSet = setSearchTermNonIndexed((List<Object>) search.searchTerm(), fieldType);
                searchTerm = null;
                searchTermBetween = null;
            } else if (searchType == SearchType.BETWEEN) {
                searchTermBetween = (List<Object>) search.searchTerm();
                searchTerm = null;
                searchTermSet = null;
            } else {
                searchTerm = setSearchTermNonIndexed(search.searchTerm(), fieldType);
                searchTermSet = null;
                searchTermBetween = null;
            }

            final Object currentValue = fieldData.varHandle.get(value);

            if (matchesSearch(currentValue, searchTerm, searchTermSet, searchTermBetween, searchType)) {
                return true;
            }
        }

        return false;
    }

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

    public String[] getCsvHeaders(final String[] fields) {
        final String[] headers = new String[fields.length + 1];
        headers[0] = "ID";
        System.arraycopy(fields, 0, headers, 1, fields.length);
        return headers;
    }

    public <V> void setNonEnumValue(final V object, final String fieldName, final Object fieldValue)
            throws Throwable {
        final var fieldData = getFieldData(object.getClass(), fieldName);
        if (fieldData != null)
            fieldData.varHandle.set(object, fieldValue);
    }

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

    public <V> void concatenateObjectValue(final V object, final String fieldName, final String fieldValue)
            throws Throwable {
        final var fieldData = getFieldData(object.getClass(), fieldName);
        if (fieldData != null) {
            final var value = (String) fieldData.varHandle.get(object);
            fieldData.varHandle.set(object, value + fieldValue);
        }
    }

    public <V> void replaceObjectValue(final V object, final String fieldName, final String fieldValue,
            final String toReplace) throws Throwable {
        final var fieldData = getFieldData(object.getClass(), fieldName);
        if (fieldData != null) {
            final var value = ((String) fieldData.varHandle.get(object)).replace(toReplace, fieldValue);
            fieldData.varHandle.set(object, value);
        }
    }

    public void deleteFileIfExists(final String filePath) {
        try {
            Files.delete(Path.of(filePath));
        } catch (final IOException e) {
            Logger.info("File for deletion does not exist [{}].", filePath);
        }
    }

    public void move(final Path source, final Path dest) {
        try {
            Files.move(source, dest, REPLACE_EXISTING);
        } catch (final IOException e) {
            Logger.error("Error moving from [{}]  to [{}]. {}", source, dest, e);
        }
    }

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
            final String fromObjectClass,
            final String toObjectClass, final Map<String, String> move, final Map<String, Object> def)
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

        currentValues.forEachEntry(entry -> {
            try {
                final var key = entry.key().get();
                final V currentVal = entry.value().get();
                final Object newObj = constructor.newInstance();

                for (final Field field : fields) {
                    final String fieldName = field.getName();
                    final String destFieldName = move.getOrDefault(fieldName, fieldName); // Faster than null check
                    final Object defValue = def.get(fieldName);

                    Field f2 = null;
                    try {
                        f2 = newObj.getClass().getDeclaredField(destFieldName);
                    } catch (final NoSuchFieldException e) {
                        continue;
                    }
                    final var f2Type = f2.getType();
                    final Object fieldVal = field.get(currentVal);
                    final Object value = defValue != null
                            ? (f2Type.isEnum() ? toEnum(f2Type, defValue) : defValue)
                            : (f2Type.isEnum() && fieldVal != null ? toEnum(f2Type, fieldVal) : fieldVal);
                    f2.set(newObj, value);
                }

                for (final Field field : newFieldsSet) {
                    final Object defValue = def.get(field.getName());
                    if (defValue != null) {
                        final var fieldType = field.getType();
                        final var value = fieldType.isEnum() ? toEnum(fieldType, defValue) : defValue;
                        field.set(newObj, value);
                    }
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

    public <K, V> Map<K, V> limitMapValues(final Map<K, V> sourceData, final int limit) {
        if (sourceData.size() <= limit) {
            return sourceData;
        }

        final var limitedMap = new HashMap<K, V>();
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

    public <K> Set<K> limitSetValues(final Set<K> sourceData, final int limit) {
        if (sourceData.size() <= limit) {
            return sourceData;
        }

        final var limitedSet = new HashSet<K>();
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

    public <K> List<K> limitListValues(final List<K> sourceData, final int limit) {
        if (sourceData.size() <= limit) {
            return sourceData;
        }

        final var limitedList = new ArrayList<K>();
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

    public void parallelIterable(final Iterable<String> iterable, final int limit, final Predicate<String> action)
            throws InterruptedException {
        final AtomicInteger matchCounter = new AtomicInteger(0);
        parallelIterable(iterable, limit, matchCounter, action);
    }

    public <T> void parallelIterable(final Iterable<T> iterable, final int limit, final AtomicInteger matchCounter,
            final Predicate<T> action) throws InterruptedException {
        if (limit <= 0 || iterable == null) {
            return;
        }

        final var iterator = iterable.iterator();
        final var iteratorLock = new Object();
        final var batchSize = 100;
        final int consumerThreads = Math.min(processors, 8);
        final var executor = Executors.newFixedThreadPool(consumerThreads);
        final var futures = new ArrayList<Future<?>>(consumerThreads);

        try {
            for (int i = 0; i < consumerThreads; i++) {
                futures.add(executor.submit(() -> {
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
                    // Expected if shutdownNow() was called
                }
            }

        } finally {
            executor.shutdownNow();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                Logger.warn("[Parallel Iterable] - Executor did not terminate in time");
            }
        }
    }

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
