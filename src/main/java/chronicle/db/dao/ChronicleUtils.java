package chronicle.db.dao;

import static chronicle.db.service.MapDb.MAP_DB;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.tinylog.Logger;

import chronicle.db.entity.CsvObject;
import chronicle.db.entity.Search;
import chronicle.db.entity.Search.SearchType;
import chronicle.db.service.MapDb.SharedIndexMap;
import net.openhft.chronicle.map.ChronicleMap;

@SuppressWarnings({ "unchecked", "rawtypes" })
public final class ChronicleUtils {
    private static final ConcurrentMap<String, Object> indexWriteLocks = new ConcurrentHashMap<>();
    public static final ChronicleUtils CHRONICLE_UTILS = new ChronicleUtils();
    private static final int processors = Runtime.getRuntime().availableProcessors();

    private static class FieldData {
        final Field field;
        final MethodHandle getterHandle;
        final MethodHandle setterHandle;

        FieldData(final Field field, final MethodHandle getterHandle, final MethodHandle setterHandle) {
            this.field = field;
            this.getterHandle = getterHandle;
            this.setterHandle = setterHandle;
        }
    }

    private static class ClassData {
        final Map<String, FieldData> fields = new ConcurrentHashMap<>();
        final MethodHandle headerHandle;
        final MethodHandle rowHandle;

        ClassData(final Class<?> clazz) {
            try {
                final MethodHandles.Lookup lookup = MethodHandles.lookup();
                this.headerHandle = lookup.findVirtual(clazz, "header", MethodType.methodType(String[].class));
                this.rowHandle = lookup.findVirtual(clazz, "row", MethodType.methodType(Object[].class, Object.class));
            } catch (NoSuchMethodException | IllegalAccessException e) {
                throw new RuntimeException("Failed to initialize MethodHandles for " + clazz.getSimpleName(), e);
            }
        }
    }

    private static final Map<Class<?>, ClassData> CLASS_DATA_CACHE = new ConcurrentHashMap<>();

    private ClassData getClassData(final Class<?> clazz) {
        return CLASS_DATA_CACHE.computeIfAbsent(clazz, ClassData::new);
    }

    private FieldData getFieldData(final Class<?> clazz, final String fieldName) {
        final ClassData classData = getClassData(clazz);
        return classData.fields.computeIfAbsent(fieldName, f -> {
            try {
                final Field field = clazz.getField(f);
                final MethodHandle getterHandle = MethodHandles.lookup().unreflectGetter(field);
                final MethodHandle setterHandle = MethodHandles.lookup().unreflectSetter(field);
                return new FieldData(field, getterHandle, setterHandle);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                Logger.warn("No such field [{}] in class [{}].", f, clazz.getSimpleName());
                return null;
            }
        });
    }

    private MethodHandle getCachedFieldGetterHandle(final Class<?> clazz, final String fieldName) {
        final FieldData fieldData = getFieldData(clazz, fieldName);
        return fieldData != null ? fieldData.getterHandle : null;
    }

    private MethodHandle getCachedFieldSetterHandle(final Class<?> clazz, final String fieldName) {
        final FieldData fieldData = getFieldData(clazz, fieldName);
        return fieldData != null ? fieldData.setterHandle : null;
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
        return String.valueOf(str).toLowerCase().contains(String.valueOf(searchTerm).toLowerCase());
    }

    public Enum toEnum(final Class<?> enumClass, final Object value) {
        try {
            return Enum.valueOf((Class<Enum>) enumClass, value.toString());
        } catch (final Exception e) {
            return null;
        }
    }

    public Set<Object> setSearchTermNonIndexed(final List<Object> searchTerms, final Class<?> fieldClass) {
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

    public Object setSearchTermNonIndexed(final Object searchTerm, final Class<?> fieldClass) {
        if (fieldClass.isEnum() && (searchTerm instanceof String)) {
            return toEnum(fieldClass, searchTerm);
        } else if (fieldClass == long.class && (searchTerm instanceof String || searchTerm instanceof Integer)) {
            return Long.parseLong(searchTerm.toString());
        }

        return searchTerm;
    }

    public <V> boolean search(final Search search, final String key, final V value, final Class<?> valueClass)
            throws Throwable {
        final String[] fields = search.field().split("\\|");

        for (final String field : fields) {
            final FieldData fieldData = getFieldData(valueClass, field);
            if (fieldData == null) {
                continue;
            }

            final var fieldType = fieldData.field.getType();
            final Object searchTerm = setSearchTermNonIndexed(search.searchTerm(), fieldType);
            final SearchType searchType = search.searchType();
            final Set<Object> searchTermSet = (searchType == SearchType.IN || searchType == SearchType.NOT_IN
                    || searchType == SearchType.CONTAINS || searchType == SearchType.NOT_CONTAINS)
                            ? setSearchTermNonIndexed((List<Object>) search.searchTerm(), fieldType)
                            : null;
            final var searchTermBetween = searchType == SearchType.BETWEEN ? (List<Object>) search.searchTerm() : null;

            final Object currentValue = fieldData.getterHandle.invoke(value);
            final boolean match = switch (searchType) {
                case EQUAL -> Objects.equals(currentValue, searchTerm);
                case NOT_EQUAL -> !Objects.equals(currentValue, searchTerm);
                case LESS -> compare(currentValue, searchTerm) < 0;
                case GREATER -> compare(currentValue, searchTerm) > 0;
                case LESS_OR_EQUAL -> compare(currentValue, searchTerm) <= 0;
                case GREATER_OR_EQUAL -> compare(currentValue, searchTerm) >= 0;
                case LIKE -> containsIgnoreCase(currentValue, searchTerm);
                case NOT_LIKE -> !containsIgnoreCase(currentValue, searchTerm);
                case CONTAINS -> {
                    for (final var obj : (Object[]) currentValue) {
                        if (searchTermSet.contains(obj)) {
                            yield true;
                        }
                    }
                    yield false;
                }
                case NOT_CONTAINS -> {
                    for (final var obj : (Object[]) currentValue) {
                        if (!searchTermSet.contains(obj)) {
                            yield true;
                        }
                    }
                    yield false;
                }
                case STARTS_WITH ->
                    String.valueOf(currentValue).toLowerCase().startsWith(String.valueOf(searchTerm).toLowerCase());
                case ENDS_WITH ->
                    String.valueOf(currentValue).toLowerCase().endsWith(String.valueOf(searchTerm).toLowerCase());
                case IN -> searchTermSet.contains(currentValue);
                case NOT_IN -> !searchTermSet.contains(currentValue);
                case BETWEEN -> compare(currentValue, searchTermBetween.get(0)) >= 0
                        && compare(currentValue, searchTermBetween.get(1)) <= 0;
                default -> false;
            };

            if (match) {
                return true; // OR logic: return as soon as any field matches
            }
        }

        return false;
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
            final String dataPath, final String indexDirPath, final Class<?> valueClass,
            final Map<String, Set<Object>> exclusions) {
        final int BATCH_SIZE = 100_000;

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
            final String indexPath = indexDirPath + "/" + field;
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
                final String indexPath = indexDirPath + "/" + field;
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

                        final Set<Object> excluded = exclusions.getOrDefault(compoundField, Collections.emptySet());
                        final List<FieldData> fieldDataList = fieldEntry.getValue();
                        sb.setLength(0);
                        boolean shouldSkip = false;

                        for (final FieldData fd : fieldDataList) {
                            final var val = String.valueOf(fd.getterHandle.invoke(value));
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
                                final var sharedIndexMap = openIndexes.get(indexDirPath + "/" + field);
                                sharedIndexMap.index.addAll(batch);
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
                    final var sharedIndexMap = openIndexes.get(indexDirPath + "/" + field);
                    sharedIndexMap.index.addAll(batch);
                }
            });
            Logger.info("Indexed [{}] records for fields: {} at [{}]", recordCount.get(), indexFieldMap.keySet(),
                    dataPath);
        } finally {
            openIndexes.forEach((indexPath, sharedIndexMap) -> {
                sharedIndexMap.commit();
                sharedIndexMap.close();
            });
            Logger.info("Indexing {} at [{}] complete.", fields, dataPath);
        }
    }

    public <V> void removeFromIndex(final String dbName, final String dataPath, final Set<String> indexFileNames,
            final Map<String, V> values, final Class<?> valueClass) {
        if (values.isEmpty() || indexFileNames.isEmpty()) {
            return;
        }

        final Map<String, SharedIndexMap> openIndexes = new ConcurrentHashMap<>();
        final var pathsToSync = ConcurrentHashMap.newKeySet();
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
                final String indexPath = dataPath + "/indexes/" + indexName;
                try {
                    openIndexes.put(indexPath, MAP_DB.openIndex(indexPath));
                } catch (final RuntimeException e) {
                    Logger.warn("Skipping index removal for [{}]: {}", indexPath, e.getMessage());
                }
            }

            // Step 2: Remove from each index
            indexFieldMap.entrySet().parallelStream().forEach(entry -> {
                final String compoundField = entry.getKey();
                final List<FieldData> fieldGetters = entry.getValue();
                final String indexPath = dataPath + "/indexes/" + compoundField;
                final var sharedIndexMap = openIndexes.get(indexPath);

                final Set<byte[]> keysToRemove = new HashSet<>(values.size());
                final StringBuilder sb = new StringBuilder();

                for (final var e : values.entrySet()) {
                    final var key = e.getKey();
                    final V value = e.getValue();

                    try {
                        sb.setLength(0);
                        for (final FieldData fd : fieldGetters) {
                            final var val = String.valueOf(fd.getterHandle.invoke(value));
                            sb.append(val);
                        }
                        keysToRemove.add(MAP_DB.createIndexKey(sb.toString(), key.toString()));
                    } catch (final Throwable t) {
                        Logger.error("Failed to generate key for [{}] in [{}]", key, compoundField);
                        Logger.error(t);
                    }
                }

                if (!keysToRemove.isEmpty()) {
                    pathsToSync.add(indexPath);
                    final Object lock = indexWriteLocks.computeIfAbsent(indexPath, k -> new Object());
                    synchronized (lock) {
                        keysToRemove.parallelStream().forEach(key -> {
                            sharedIndexMap.index.remove(key);
                        });
                    }
                    Logger.info("Removed [{}] records from index: [{}]", keysToRemove.size(), compoundField);
                }
            });
        } finally {
            openIndexes.forEach((path, sharedIndexMap) -> {
                if (pathsToSync.contains(path)) {
                    sharedIndexMap.commit();
                }
                sharedIndexMap.close();
            });
        }
    }

    public <V> void updateIndex(final String dbName, final String dataPath, final Set<String> indexFileNames,
            final Map<String, V> values, final Map<String, V> previousValues, final Class<?> valueClass,
            final Map<String, Set<Object>> exclusions) {
        if (values.isEmpty() || indexFileNames.isEmpty()) {
            return;
        }

        final int BATCH_SIZE = 100_000;
        final Map<String, SharedIndexMap> openIndexes = new ConcurrentHashMap<>();
        final var pathsToSync = ConcurrentHashMap.newKeySet();

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
                final String indexPath = dataPath + "/indexes/" + indexName;
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
                final String indexPath = dataPath + "/indexes/" + indexName;
                final var sharedIndexMap = openIndexes.get(indexPath);
                final Set<byte[]> addBatch = new HashSet<>(BATCH_SIZE);
                final Set<byte[]> removeBatch = new HashSet<>(BATCH_SIZE);
                int recordCount = 0;

                final Set<Object> excluded = exclusions.getOrDefault(indexName, Collections.emptySet());
                final StringBuilder sb = new StringBuilder();

                for (final var valEntry : values.entrySet()) {
                    final var key = valEntry.getKey();
                    final V newVal = valEntry.getValue();
                    final V prevVal = previousValues.get(key);

                    try {
                        sb.setLength(0);
                        boolean skipAdd = false;

                        for (final FieldData fd : fieldGetters) {
                            final var value = String.valueOf(fd.getterHandle.invoke(newVal));
                            if (excluded.contains(value)) {
                                skipAdd = true;
                            }
                            sb.append(value);
                        }
                        final var newValStr = sb.toString();

                        if (prevVal != null) {
                            sb.setLength(0);
                            for (final FieldData fd : fieldGetters) {
                                final var value = String.valueOf(fd.getterHandle.invoke(prevVal));
                                sb.append(value);
                            }
                            final var oldValStr = sb.toString();

                            if (!Objects.equals(oldValStr, newValStr)) {
                                // Always remove if changed (regardless of exclusion)
                                removeBatch.add(MAP_DB.createIndexKey(oldValStr, key.toString()));
                                // Add new value only if not excluded and not empty
                                if (!skipAdd) {
                                    addBatch.add(MAP_DB.createIndexKey(newValStr, key.toString()));
                                }
                                recordCount++;
                            }
                        } else {
                            // Add new value only if not excluded
                            if (!skipAdd) {
                                addBatch.add(MAP_DB.createIndexKey(newValStr, key.toString()));
                                recordCount++;
                            }
                        }

                        if (addBatch.size() >= BATCH_SIZE || removeBatch.size() >= BATCH_SIZE) {
                            pathsToSync.add(indexPath);
                            final Object lock = indexWriteLocks.computeIfAbsent(indexPath, k -> new Object());
                            synchronized (lock) {
                                removeBatch.parallelStream().forEach(remove -> {
                                    sharedIndexMap.index.remove(remove);
                                });
                                sharedIndexMap.index.addAll(addBatch);
                                removeBatch.clear();
                                addBatch.clear();
                            }
                        }
                    } catch (final Throwable t) {
                        Logger.error("Failed to update index [{}] for key [{}]", indexName, key);
                        Logger.error(t);
                    }
                }

                // Final flush
                if (!addBatch.isEmpty() || !removeBatch.isEmpty()) {
                    pathsToSync.add(indexPath);
                    final Object lock = indexWriteLocks.computeIfAbsent(indexPath, k -> new Object());
                    synchronized (lock) {
                        removeBatch.parallelStream().forEach(remove -> {
                            sharedIndexMap.index.remove(remove);
                        });
                        sharedIndexMap.index.addAll(addBatch);
                    }
                }

                if (recordCount != 0)
                    Logger.info("Updated [{}] records for index: [{}]", recordCount, indexName);
            });
        } finally {
            openIndexes.forEach((path, sharedIndexMap) -> {
                if (pathsToSync.contains(path)) {
                    sharedIndexMap.commit();
                }
                sharedIndexMap.close();
            });
        }
    }

    /**
     * Only for chronicle db object types to convert to csv for table display on
     * frontend
     * 
     * @throws Throwable
     * 
     */
    public <V> CsvObject formatChronicleDataToCsv(final Map<String, V> map) throws Throwable {
        if (map.isEmpty())
            return new CsvObject(new String[0], Collections.emptyList());

        final V sampleValue = map.values().iterator().next();
        final var classData = getClassData(sampleValue.getClass());
        final MethodHandle headersMethod = classData.headerHandle;
        final MethodHandle rowMethod = classData.rowHandle;
        final String[] headerList = (String[]) headersMethod.invoke(sampleValue);
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();

        map.entrySet().parallelStream().forEach(entry -> {
            try {
                rowQueue.add((Object[]) rowMethod.invoke(entry.getValue(), entry.getKey()));
            } catch (final Throwable e) {
                // should not happen
                Logger.error("Error when formatting Chronicle object to CSV.");
                Logger.error(e);
            }
        });

        return new CsvObject(headerList, new ArrayList<>(rowQueue));
    }

    public <V> void subsetOfValues(final String[] fields, final String key, final V value,
            final Map<String, LinkedHashMap<String, Object>> map, final String objectName, final Class<?> valueClass) {
        final LinkedHashMap<String, Object> valueMap = new LinkedHashMap<>(fields.length);

        for (final String f : fields) {
            final MethodHandle methodHandle = getCachedFieldGetterHandle(valueClass, f);
            if (methodHandle != null) {
                try {
                    valueMap.put(f, methodHandle.invoke(value));
                } catch (final Throwable e) {
                    // should not happen, all fields must be public
                    Logger.error("Could not get value for field [{}] in [{}].", f, objectName);
                    Logger.error(e);
                }
            }
        }
        map.put(key, valueMap);
    }

    public <V> void subsetOfValues(final String[] fields, final Map.Entry<String, V> entry,
            final Map<String, LinkedHashMap<String, Object>> map, final String objectName, final Class<?> valueClass) {
        final LinkedHashMap<String, Object> valueMap = new LinkedHashMap<>(fields.length);
        final var key = entry.getKey();
        final V value = entry.getValue();

        for (final String f : fields) {
            final MethodHandle methodHandle = getCachedFieldGetterHandle(valueClass, f);
            if (methodHandle != null) {
                try {
                    valueMap.put(f, methodHandle.invoke(value));
                } catch (final Throwable e) {
                    // should not happen, all fields must be public
                    Logger.error("Could not get value for field [{}] in [{}].", f, objectName);
                    Logger.error(e);
                }
            }
        }
        map.put(key, valueMap);
    }

    public <V> Object[] subsetOfValuesToRow(final String[] headers, final String key, final V value,
            final String objectName, final Class<?> valueClass) {
        final Object[] row = new Object[headers.length];
        row[0] = key;
        for (int i = 1; i < headers.length; i++) {
            final String field = headers[i];
            final MethodHandle methodHandle = getCachedFieldGetterHandle(valueClass, field);
            if (methodHandle != null) {
                try {
                    row[i] = methodHandle.invoke(value); // Always start at position 1
                } catch (final Throwable e) {
                    // should not happen, all fields must be public
                    Logger.error("Could not get value for field [{}] in [{}].", field, objectName);
                    Logger.error(e);
                }
            }
        }

        return row;
    }

    public <V> void subsetOfValuesToRow(final String[] fields, final V value,
            final Object[] row, final String objectName, final Class<?> valueClass) {
        for (int i = 0; i < fields.length; i++) {
            final String field = fields[i];
            final MethodHandle methodHandle = getCachedFieldGetterHandle(valueClass, field);
            if (methodHandle != null) {
                try {
                    row[1 + i] = methodHandle.invoke(value); // Always start at position 1
                } catch (final Throwable e) {
                    // should not happen, all fields must be public
                    Logger.error("Could not get value for field [{}] in [{}].", field, objectName);
                    Logger.error(e);
                }
            }
        }
    }

    public String[] getCsvHeaders(final String[] fields) {
        final String[] headers = new String[fields.length + 1];
        headers[0] = "ID";
        System.arraycopy(fields, 0, headers, 1, fields.length);
        return headers;
    }

    /**
     * Only for chronicle db object types to convert to csv for table display on
     * frontend
     * 
     */
    public <V> CsvObject formatSubsetChronicleDataToCsv(final Map<String, V> map,
            final String[] headers, final String objectName, final Class<?> valueClass) {
        final ConcurrentLinkedQueue<Object[]> rowQueue = new ConcurrentLinkedQueue<>();
        final String[] updatedHeaders = new String[headers.length + 1];
        updatedHeaders[0] = "ID";
        System.arraycopy(headers, 0, updatedHeaders, 1, headers.length);

        map.entrySet().parallelStream().forEach(entry -> {
            final var key = entry.getKey();
            final V value = entry.getValue();
            final Object[] row = new Object[updatedHeaders.length];
            row[0] = key;

            subsetOfValuesToRow(headers, value, row, objectName, valueClass);
            rowQueue.add(row);
        });

        return new CsvObject(updatedHeaders, new ArrayList<>(rowQueue));
    }

    public <V> void updateObjectValues(final V oldObject, final Set<String> fields, final V newObject)
            throws Throwable {
        for (final var k : fields) {
            final var fieldData = getFieldData(oldObject.getClass(), k);
            if (fieldData != null)
                fieldData.setterHandle.invoke(oldObject, fieldData.getterHandle.invoke(newObject));
        }
    }

    public <V> void setNonEnumValue(final V object, final String fieldName, final Object fieldValue)
            throws Throwable {
        final var setterHandle = getCachedFieldSetterHandle(object.getClass(), fieldName);
        if (setterHandle != null)
            setterHandle.invoke(object, fieldValue);
    }

    public <V> void setObjectValue(final V object, final String fieldName, final Object fieldValue)
            throws Throwable {
        final var fieldData = getFieldData(object.getClass(), fieldName);

        if (fieldData != null) {
            final var type = fieldData.field.getType();
            if (type.isEnum())
                fieldData.setterHandle.invoke(object, toEnum(type, fieldValue));
            else
                fieldData.setterHandle.invoke(object, fieldValue);

        }
    }

    public <V> void concatenateObjectValue(final V object, final String fieldName, final String fieldValue)
            throws Throwable {
        final var fieldData = getFieldData(object.getClass(), fieldName);
        if (fieldData != null) {
            final var value = (String) fieldData.getterHandle.invoke(object);
            fieldData.setterHandle.invoke(object, value + fieldValue);
        }
    }

    public <V> void replaceObjectValue(final V object, final String fieldName, final String fieldValue,
            final String toReplace) throws Throwable {
        final var fieldData = getFieldData(object.getClass(), fieldName);
        final var value = ((String) fieldData.getterHandle.invoke(object)).replace(toReplace, fieldValue);
        fieldData.setterHandle.invoke(object, value);
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
            final Predicate<T> action)
            throws InterruptedException {
        if (limit <= 0 || iterable == null) {
            return;
        }

        final var executor = Executors.newFixedThreadPool(processors);
        final var done = new AtomicBoolean(false);
        final var iterator = iterable.iterator();
        final var iteratorLock = new Object();
        final var futures = new ArrayList<Future<?>>();
        final var batchSize = 100;

        try {
            // Spawn consumer threads
            final int consumerThreads = Math.min(processors, 8); // Limit to reduce contention
            for (int i = 0; i < consumerThreads; i++) {
                futures.add(executor.submit(() -> {
                    final var batch = new ArrayList<T>(batchSize);
                    while (!done.get() && matchCounter.get() < limit) {
                        batch.clear();
                        synchronized (iteratorLock) {
                            for (int j = 0; j < batchSize && iterator.hasNext(); j++) {
                                batch.add(iterator.next());
                            }
                        }

                        if (batch.isEmpty())
                            return;

                        for (final T item : batch) {
                            if (Thread.currentThread().isInterrupted() || done.get() || matchCounter.get() >= limit)
                                return;
                            try {
                                if (action.test(item)) {
                                    if (matchCounter.incrementAndGet() >= limit) {
                                        done.set(true);
                                        return;
                                    }
                                }
                            } catch (final Exception e) {
                                Logger.error("[Parallel Iterable] - Error processing item [{}]", item);
                                Logger.error(e);
                            }
                        }
                    }
                }));
            }

            // Wait for consumer threads until limit reached
            for (final Iterator<Future<?>> it = futures.iterator(); it.hasNext() && !done.get();) {
                final Future<?> future = it.next();
                try {
                    future.get();
                    it.remove();
                } catch (final Exception e) {
                    Logger.error("[Parallel Iterable] - Consumer thread failed.");
                    Logger.error(e);
                }
            }
        } finally {
            executor.shutdownNow(); // Cancel all tasks
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
}
