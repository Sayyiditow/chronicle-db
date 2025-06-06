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
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.mapdb.HTreeMap;
import org.tinylog.Logger;

import chronicle.db.entity.CsvObject;
import chronicle.db.entity.Search;
import chronicle.db.entity.Search.SearchType;
import net.openhft.chronicle.map.ChronicleMap;

@SuppressWarnings({ "unchecked", "rawtypes" })
public final class ChronicleUtils {
    private static final ConcurrentMap<String, Object> indexWriteLocks = new ConcurrentHashMap<>();
    public static final ChronicleUtils CHRONICLE_UTILS = new ChronicleUtils();

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
        final BigDecimal decimal1 = new BigDecimal(String.valueOf(obj1));
        final BigDecimal decimal2 = new BigDecimal(String.valueOf(obj2));
        return decimal1.compareTo(decimal2);
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

    public Set<Object> setSearchTerm(final List<Object> searchTerms, final Class<?> fieldClass) {
        final int size = searchTerms.size();
        final var searchTermSet = new HashSet<>(size);

        for (int i = 0; i < size; i++) {
            final var searchTerm = searchTerms.get(i);
            if (searchTerm == null) {
                searchTermSet.add("null"); // Explicitly setting "null" as string
                continue;
            }
            // Handle enums first
            if (searchTerm.getClass().isEnum()) {
                searchTermSet.add(searchTerm.toString());
                continue;
            }
            // Optimize for long field type conversion
            if (fieldClass == long.class && (searchTerm instanceof String || searchTerm instanceof Integer)) {
                searchTerms.add(Long.parseLong(searchTerm.toString()));
            }
            // Default: Add the original value
            searchTermSet.add(searchTerm);
        }

        return searchTermSet;
    }

    public Object setSearchTerm(final Object searchTerm, final Class<?> fieldClass) {
        if (searchTerm == null)
            return "null";
        if (searchTerm.getClass().isEnum())
            return searchTerm.toString();
        if (fieldClass == long.class && (searchTerm instanceof String || searchTerm instanceof Integer)) {
            return Long.parseLong(searchTerm.toString());
        }
        return searchTerm;
    }

    public Set<Object> setSearchTermNonIndexed(final List<Object> searchTerms, final Class<?> fieldClass) {
        final int size = searchTerms.size();
        final var searchTermSet = new HashSet<>(size);

        for (int i = 0; i < searchTerms.size(); i++) {
            final var searchTerm = searchTerms.get(i);

            if (fieldClass.isEnum() && (searchTerm instanceof String)) {
                searchTermSet.add(toEnum(fieldClass, searchTerm));
                continue;
            }

            if (fieldClass == long.class
                    && (searchTerm instanceof String || searchTerm instanceof Integer)) {
                searchTermSet.add(Long.parseLong(searchTerm.toString()));
                continue;
            }

            // Default: Add the original value
            searchTermSet.add(searchTerm);
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

    public <K, V> boolean search(final Search search, final K key, final V value) throws Throwable {
        final FieldData fieldData = getFieldData(value.getClass(), search.field());
        if (fieldData == null) {
            return false;
        }

        final var fieldType = fieldData.field.getType();
        final Object searchTerm = setSearchTermNonIndexed(search.searchTerm(), fieldType);
        final SearchType searchType = search.searchType();
        final Set<Object> searchTermSet = (searchType == SearchType.IN || searchType == SearchType.NOT_IN
                || searchType == SearchType.CONTAINS || searchType == SearchType.NOT_CONTAINS)
                        ? setSearchTermNonIndexed((List<Object>) search.searchTerm(), fieldType)
                        : null;

        final Object currentValue = fieldData.getterHandle.invoke(value);
        if (currentValue == null)
            return false;

        return switch (searchType) {
            case EQUAL -> currentValue.equals(searchTerm);
            case NOT_EQUAL -> !currentValue.equals(searchTerm);
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
            default -> false;
        };
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
    public <K, V> void index(final ChronicleMap<K, V> db, final String dbName, final Set<String> fields,
            final String dataPath, final String indexDirPath) {
        Logger.info("Indexing {} at [{}].", fields, dataPath);
        if (db.isEmpty())
            return;

        final Map<String, FieldData> fieldMap = new HashMap<>(fields.size());
        final Map<String, Map<Object, List<K>>> fieldIndexMap = new HashMap<>(fields.size());

        final Class<?>[] valueType = new Class<?>[1];

        // fastest way to get first value class
        try {
            db.forEachEntry(e -> {
                if (valueType[0] == null && e.value() != null) {
                    valueType[0] = e.value().get().getClass();
                    throw new RuntimeException("Breaking forEachEntry.");
                }
            });
        } catch (final RuntimeException e) {// ignored
        }

        for (final String field : fields) {
            final FieldData fieldGetterHandle = getFieldData(valueType[0], field);
            if (fieldGetterHandle != null)
                fieldMap.put(field, fieldGetterHandle);
        }

        db.forEachEntry(entry -> {
            final K key = entry.key().get();
            final V value = entry.value().get();
            for (final String field : fieldMap.keySet()) {
                final FieldData fieldData = fieldMap.get(field);
                final Map<Object, List<K>> indexMap = fieldIndexMap.computeIfAbsent(field, k -> new HashMap<>());
                try {
                    Object currentValue = fieldData.getterHandle.invoke(value);
                    if (fieldData.field.getType().isEnum() || currentValue == null)
                        currentValue = Objects.toString(currentValue, "null");
                    indexMap.computeIfAbsent(currentValue, k -> new ArrayList<>()).add(key);
                } catch (final Throwable e) {
                    // should not happen, all fields are public
                }
            }
        });

        // Write to disk in parallel
        fieldIndexMap.entrySet().parallelStream().forEach(entry -> {
            final String indexPath = indexDirPath + "/" + entry.getKey();
            final var lock = indexWriteLocks.computeIfAbsent(indexPath, k -> new Object());
            synchronized (lock) {
                final HTreeMap<Object, List<K>> indexDb = MAP_DB.open(indexPath);
                if (indexDb != null) {
                    try {
                        indexDb.putAll(entry.getValue());
                    } finally {
                        MAP_DB.close(indexPath);
                    }
                }
            }
        });
    }

    private <K, V> void removeFromIndex(final String dbName, final String dataPath, final Map<K, V> values,
            final String file) {
        final String indexPath = dataPath + "/indexes/" + file;
        if (values.isEmpty())
            return;

        final V sampleValue = values.values().iterator().next();
        final FieldData fieldData = getFieldData(sampleValue.getClass(), file);
        if (fieldData == null) {
            deleteFileIfExists(indexPath);
            return;
        }
        final boolean isEnum = fieldData.field.getType().isEnum();

        final Map<Object, List<K>> updatesToRemove = new HashMap<>(values.size());
        for (final var entry : values.entrySet()) {
            final V value = entry.getValue();
            Object indexKey;
            try {
                indexKey = fieldData.getterHandle.invoke(value);
            } catch (final Throwable e) {
                // should not happen as all fields are public
                continue;
            }
            if (isEnum || indexKey == null)
                indexKey = Objects.toString(indexKey, "null");
            updatesToRemove.computeIfAbsent(indexKey, k -> new ArrayList<>()).add(entry.getKey());
        }

        final var lock = indexWriteLocks.computeIfAbsent(indexPath, k -> new Object());
        synchronized (lock) {
            final HTreeMap<Object, List<K>> indexDb = MAP_DB.open(indexPath);
            if (indexDb != null) {
                try {
                    for (final var entry : updatesToRemove.entrySet()) {
                        final List<K> currentList = indexDb.get(entry.getKey());
                        if (currentList != null) {
                            currentList.removeAll(entry.getValue());
                            if (currentList.isEmpty())
                                indexDb.remove(entry.getKey());
                            else
                                indexDb.put(entry.getKey(), currentList);
                        }
                    }
                } finally {
                    MAP_DB.close(indexPath);
                }
            }
        }
    }

    /**
     * Update the index
     * 
     * @param dbFileName     the file where the data is stored
     * @param dataPath       the folder path
     * @param field          the value object field enum
     * @param indexFileNames
     * @throws IOException
     * @throws InterruptedException
     */
    public <K, V> void removeFromIndex(final String dbName, final String dataPath,
            final Set<String> indexFileNames, final Map<K, V> values) {
        indexFileNames.parallelStream().forEach(file -> {
            removeFromIndex(dbName, dataPath, values, file);
        });
    }

    public <K, V> void updateIndex(final String dbName, final String dataPath, final Map<K, V> values,
            final String file, final Map<K, V> prevValues) {
        if (values.isEmpty())
            return;
        final String indexPath = dataPath + "/indexes/" + file;

        final V sampleValue = values.values().iterator().next();
        final var sampleValueClass = sampleValue.getClass();
        final FieldData fieldData = getFieldData(sampleValueClass, file);
        if (fieldData == null) {
            Logger.warn("No field data for index [{}] and class [{}], skipping", file,
                    sampleValueClass.getSimpleName());
            deleteFileIfExists(indexPath);
            return;
        }
        final boolean isEnum = fieldData.field.getType().isEnum();

        final Map<Object, List<K>> updatesToAdd = new HashMap<>(values.size());
        final Map<Object, List<K>> updatesToRemove = new HashMap<>(prevValues.size());

        for (final K key : values.keySet()) {
            final V newValue = values.get(key);
            final V prevValue = prevValues.get(key);
            Object newIndexKey;
            try {
                newIndexKey = fieldData.getterHandle.invoke(newValue);
            } catch (final Throwable e) {
                // should not happen
                continue;
            }
            if (isEnum || newIndexKey == null)
                newIndexKey = Objects.toString(newIndexKey, "null");

            if (prevValue == null) {
                updatesToAdd.computeIfAbsent(newIndexKey, k -> new ArrayList<>()).add(key);
            } else {
                Object prevIndexKey;
                try {
                    prevIndexKey = fieldData.getterHandle.invoke(prevValue);
                } catch (final Throwable e) {
                    // should not happen
                    continue;
                }
                if (!Objects.equals(newIndexKey, prevIndexKey)) {
                    if (isEnum) {
                        prevIndexKey = String.valueOf(prevIndexKey);
                        newIndexKey = String.valueOf(newIndexKey);
                    }
                    if (prevIndexKey == null)
                        prevIndexKey = "null";
                    if (newIndexKey == null)
                        newIndexKey = "null";
                    updatesToRemove.computeIfAbsent(prevIndexKey, k -> new ArrayList<>()).add(key);
                    updatesToAdd.computeIfAbsent(newIndexKey, k -> new ArrayList<>()).add(key);
                }
            }
        }

        final var lock = indexWriteLocks.computeIfAbsent(indexPath, k -> new Object());
        synchronized (lock) {
            final HTreeMap<Object, List<K>> indexDb = MAP_DB.open(indexPath);
            if (indexDb != null) {
                try {
                    for (final var entry : updatesToRemove.entrySet()) {
                        final List<K> currentList = indexDb.get(entry.getKey());
                        if (currentList != null) {
                            currentList.removeAll(entry.getValue());
                            if (currentList.isEmpty())
                                indexDb.remove(entry.getKey());
                            else
                                indexDb.put(entry.getKey(), currentList);
                        }
                    }
                    for (final var entry : updatesToAdd.entrySet()) {
                        final List<K> currentList = indexDb.computeIfAbsent(entry.getKey(), k -> new ArrayList<>());
                        currentList.addAll(entry.getValue());
                        indexDb.put(entry.getKey(), currentList);
                    }
                } finally {
                    MAP_DB.close(indexPath);
                }
            }
        }
    }

    /**
     * Update indexes by removing first then adding them
     * 
     * @param dataPath       the folder path
     * @param field          the value object field enum
     * @param indexFileNames index files
     * @throws IOException
     * @throws InterruptedException
     */
    public <K, V> void updateIndex(final String dbName, final String dataPath,
            final Set<String> indexFileNames, final Map<K, V> values, final Map<K, V> previousValues) {
        indexFileNames.parallelStream().forEach(file -> {
            updateIndex(dbName, dataPath, values, file, previousValues);
        });
    }

    /**
     * Only for chronicle db object types to convert to csv for table display on
     * frontend
     * 
     * @throws Throwable
     * 
     */
    public <K, V> CsvObject formatChronicleDataToCsv(final Map<K, V> map) throws Throwable {
        if (map.isEmpty())
            return new CsvObject(new String[0], Collections.emptyList());

        final V sampleValue = map.values().iterator().next();
        final var classData = getClassData(sampleValue.getClass());
        final MethodHandle headersMethod = classData.headerHandle;
        final MethodHandle rowMethod = classData.rowHandle;
        final String[] headerList = (String[]) headersMethod.invoke(sampleValue);
        final List<Object[]> rowList = new ArrayList<>(map.size());

        for (final var entry : map.entrySet()) {
            rowList.add((Object[]) rowMethod.invoke(entry.getValue(), entry.getKey()));
        }

        return new CsvObject(headerList, rowList);
    }

    public <K, V> void subsetOfValues(final String[] fields, final Map.Entry<K, V> entry,
            final Map<K, LinkedHashMap<String, Object>> map, final String objectName) {
        final LinkedHashMap<String, Object> valueMap = new LinkedHashMap<>(fields.length);
        final K key = entry.getKey();
        final V value = entry.getValue();
        final var valueClass = value.getClass();

        for (final String f : fields) {
            final MethodHandle methodHandle = getCachedFieldGetterHandle(valueClass, f);
            if (methodHandle != null) {
                try {
                    valueMap.put(f, methodHandle.invoke(value));
                } catch (final Throwable e) {
                    // should not happen, all fields must be public
                    Logger.error("Field [{}] in [{}] could not get value.", f, objectName);
                    Logger.error(e);
                }
            }
        }
        map.put(key, valueMap);
    }

    /**
     * Only for chronicle db object types to convert to csv for table display on
     * frontend
     * 
     */
    public <K> CsvObject formatSubsetChronicleDataToCsv(final Map<K, LinkedHashMap<String, Object>> map,
            final String[] headers) {
        final List<Object[]> rowList = new ArrayList<>(map.size()); // Pre-size list
        final String[] updatedHeaders = new String[headers.length + 1];
        updatedHeaders[0] = "ID";
        System.arraycopy(headers, 0, updatedHeaders, 1, headers.length);

        for (final var entry : map.entrySet()) {
            final K key = entry.getKey();
            final LinkedHashMap<String, Object> valueMap = entry.getValue();
            // Create row array: key + values
            final Object[] obj = new Object[updatedHeaders.length]; // Size based on headers
            obj[0] = key;

            // Copy values directly from valueMap.values()
            final Object[] values = valueMap.values().toArray();
            System.arraycopy(values, 0, obj, 1, Math.min(values.length, obj.length - 1));
            rowList.add(obj);
        }

        return new CsvObject(updatedHeaders, rowList);
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
    public <K, V> Map<K, Object> moveRecords(final Map<K, V> currentValues,
            final String toObjectClass, final Map<String, String> move, final Map<String, Object> def)
            throws SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, ClassNotFoundException {
        if (currentValues.isEmpty())
            return new HashMap<>(); // Early exit

        final Map<K, Object> map = new HashMap<>(currentValues.size()); // Pre-size map
        final Class<?> cls = Class.forName(toObjectClass);
        final Constructor<?> constructor = cls.getConstructor();
        final V sampleValue = currentValues.values().iterator().next();
        final Field[] fields = sampleValue.getClass().getDeclaredFields();
        final Object newInstance = constructor.newInstance(); // Pre-instantiate once
        final Field[] newFields = newInstance.getClass().getDeclaredFields();
        final Set<Field> newFieldsSet = new HashSet<>(Arrays.asList(newFields)); // Faster lookup
        newFieldsSet.removeAll(Arrays.asList(fields));

        for (final var entry : currentValues.entrySet()) {
            final K key = entry.getKey();
            final V currentVal = entry.getValue();
            final Object newObj = constructor.newInstance();

            for (final Field field : fields) {
                final String fieldName = field.getName();
                final String destFieldName = move.getOrDefault(fieldName, fieldName); // Faster than null check
                final Object defValue = def.get(fieldName);

                try {
                    final Field f2 = newObj.getClass().getField(destFieldName);
                    final var f2Type = f2.getType();
                    final Object fieldVal = field.get(currentVal);
                    final Object value = defValue != null
                            ? (f2Type.isEnum() ? toEnum(f2Type, defValue) : defValue)
                            : (f2Type.isEnum() && fieldVal != null ? toEnum(f2Type, fieldVal) : fieldVal);
                    f2.set(newObj, value);
                } catch (final NoSuchFieldException e) {
                }
            }

            for (final Field field : newFieldsSet) { // Use Set for iteration
                final Object defValue = def.get(field.getName());
                if (defValue != null) {
                    final var fieldType = field.getType();
                    final var value = fieldType.isEnum() ? toEnum(fieldType, defValue) : defValue;
                    field.set(newObj, value);
                }
            }
            map.put(key, newObj);
        }

        return map;
    }

    public <K> Class<?> getFieldClass(final Map<Object, List<K>> index) {
        Class<?> fieldClass = null;
        for (final Object key : index.keySet()) {
            if (key != null) {
                fieldClass = key.getClass();
                break;
            }
        }

        return fieldClass;
    }

    public <K, V> Map<K, V> limitMapValues(final Map<K, V> sourceData, final int limit) {
        final int maxEntries = (int) limit;
        final var limitedMap = new HashMap<K, V>();
        int count = 0;
        for (final var entry : sourceData.entrySet()) {
            if (count >= maxEntries) {
                break;
            }
            limitedMap.put(entry.getKey(), entry.getValue());
            count++;
        }
        return limitedMap;
    }

    /**
     * Paginate a map using a time field field such as createdAt (must use long)
     * 
     * @param data
     * @param limit
     * @param page
     * @param orderByField
     */
    public Map<String, Object> setPages(final Map<String, Map<String, Object>> data, final int limit, final int page,
            final String orderByField) {
        if (data == null || data.isEmpty() || limit <= 0 || page < 0) {
            return Map.of("totalPages", 0, "page", page, "data", Collections.emptyMap());
        }

        // Calculate total pages
        final int totalPages = (int) Math.ceil((double) data.size() / limit);

        // Create a PriorityQueue to get the required range of entries sorted by
        // orderByField
        final PriorityQueue<Map.Entry<String, Map<String, Object>>> queue = new PriorityQueue<>(
                (e1, e2) -> {
                    final var e1Value = e1.getValue();
                    final var valueClass = e1Value.getClass();
                    final var fieldData = getFieldData(valueClass, orderByField);
                    try {
                        final Object value1 = fieldData.getterHandle.invoke(e1Value);
                        final Object value2 = fieldData.getterHandle.invoke(e2.getValue());
                        return Long.compare((long) value1, (long) value2);
                    } catch (final Throwable t) {
                        return 0;
                    }
                });
        queue.addAll(data.entrySet());

        // Extract entries for the requested page
        final Map<String, Map<String, Object>> resultData = new HashMap<>();
        final int start = page * limit;
        final int end = Math.min(start + limit, data.size());
        int index = 0;

        while (!queue.isEmpty() && index < end) {
            final Map.Entry<String, Map<String, Object>> entry = queue.poll();
            if (index >= start) {
                resultData.put(entry.getKey(), entry.getValue());
            }
            index++;
        }

        return Map.of("totalPages", totalPages, "page", page, "data", resultData);
    }
}
