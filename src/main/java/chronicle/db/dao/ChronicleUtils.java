package chronicle.db.dao;

import static chronicle.db.service.MapDb.MAP_DB;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.mapdb.HTreeMap;
import org.tinylog.Logger;

import com.jsoniter.JsonIterator;
import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.TypeLiteral;

import chronicle.db.entity.CsvObject;
import chronicle.db.entity.Search;
import chronicle.db.entity.Search.SearchType;

@SuppressWarnings({ "unchecked", "rawtypes" })
public final class ChronicleUtils {
    private static final ConcurrentMap<String, Object> indexWriteLocks = new ConcurrentHashMap<>();
    public static final ChronicleUtils CHRONICLE_UTILS = new ChronicleUtils();
    private static final ConcurrentMap<Class<?>, Map<String, Field>> FIELD_CACHE = new ConcurrentHashMap<>();

    private Field getCachedField(final Class<?> clazz, final String fieldName) {
        return FIELD_CACHE.computeIfAbsent(clazz, c -> new ConcurrentHashMap<>())
                .computeIfAbsent(fieldName, f -> {
                    try {
                        return clazz.getField(f);
                    } catch (final NoSuchFieldException e) {
                        Logger.error("No such field [{}] in class [{}].", f, clazz.getSimpleName(), e);
                        return null;
                    }
                });
    }

    public <T> String toJsonFromObj(final T prop) {
        return JsonStream.serialize(prop);
    }

    public <T> void toJsonFileFromObj(final String path, final T prop) throws IOException {
        Files.writeString(Path.of(path), toJsonFromObj(prop), StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
    }

    public <T> T fromJsonToObj(final String json, final TypeLiteral<T> typeLiteral) {
        return JsonIterator.deserialize(json, typeLiteral);
    }

    public <T> T fromJsonFileToObj(final String path, final TypeLiteral<T> typeLiteral) throws IOException {
        return JsonIterator.deserialize(Files.readAllBytes(Path.of(path)), typeLiteral);
    }

    /**
     * Retrieve a list of files in a dirPath and throw an exception is dirPath is
     * null
     *
     * @param dirPath dirPath to retrieve files from
     * @return a list of files
     */
    public List<String> getFileList(final String dirPath) throws IOException {
        try (Stream<Path> stream = Files.list(Path.of(dirPath))) {
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

    public List<Object> setSearchTerm(final List<Object> searchTerms, final Class<?> fieldClass) {
        for (int i = 0; i < searchTerms.size(); i++) {
            final var searchTerm = searchTerms.get(i);
            if (searchTerm == null) {
                searchTerms.set(i, "null"); // Explicitly setting "null" as string
                continue;
            }
            // Handle enums first
            if (searchTerm.getClass().isEnum()) {
                searchTerms.set(i, searchTerm.toString());
                continue;
            }
            // Optimize for long field type conversion
            if (fieldClass == long.class && (searchTerm instanceof String || searchTerm instanceof Integer)) {
                searchTerms.set(i, Long.parseLong(searchTerm.toString()));
            }
        }

        return searchTerms;
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

    public List<Object> setSearchTermNonIndexed(final List<Object> searchTerms, final Class<?> fieldClass) {
        for (int i = 0; i < searchTerms.size(); i++) {
            final var searchTerm = searchTerms.get(i);

            if (searchTerm != null) {
                if (fieldClass.isEnum() && (searchTerm instanceof String)) {
                    searchTerms.set(i, toEnum(fieldClass, searchTerm));
                } else if (fieldClass == long.class
                        && (searchTerm instanceof String || searchTerm instanceof Integer)) {
                    searchTerms.set(i, Long.parseLong(searchTerm.toString()));
                }
            }
        }

        return searchTerms;
    }

    public Object setSearchTermNonIndexed(final Object searchTerm, final Class<?> fieldClass) {
        if (fieldClass.isEnum() && (searchTerm instanceof String)) {
            return toEnum(fieldClass, searchTerm);
        } else if (fieldClass == long.class && (searchTerm instanceof String || searchTerm instanceof Integer)) {
            return Long.parseLong(searchTerm.toString());
        }

        return searchTerm;
    }

    public <K, V> void search(final Search search, final K key, final V value, final Map<K, V> map)
            throws IllegalArgumentException, IllegalAccessException {
        final Field field = getCachedField(value.getClass(), search.field());
        if (field == null) {
            Logger.error("No such field [{}] exists on searching {}", search.field(), value.getClass().getSimpleName());
            return;
        }

        final Object searchTerm = setSearchTermNonIndexed(search.searchTerm(), field.getType());
        final SearchType searchType = search.searchType();
        final List<Object> searchTermList = (searchType == SearchType.IN || searchType == SearchType.NOT_IN)
                ? setSearchTermNonIndexed((List<Object>) search.searchTerm(), field.getType())
                : null;

        final Object currentValue = field.get(value);
        if (currentValue == null)
            return;

        switch (searchType) {
            case EQUAL -> {
                if (currentValue.equals(searchTerm))
                    map.put(key, value);
            }
            case NOT_EQUAL -> {
                if (!currentValue.equals(searchTerm))
                    map.put(key, value);
            }
            case LESS -> {
                if (compare(currentValue, searchTerm) < 0)
                    map.put(key, value);
            }
            case GREATER -> {
                if (compare(currentValue, searchTerm) > 0)
                    map.put(key, value);
            }
            case LESS_OR_EQUAL -> {
                if (compare(currentValue, searchTerm) <= 0)
                    map.put(key, value);
            }
            case GREATER_OR_EQUAL -> {
                if (compare(currentValue, searchTerm) >= 0)
                    map.put(key, value);
            }
            case LIKE -> {
                if (containsIgnoreCase(currentValue, searchTerm))
                    map.put(key, value);
            }
            case NOT_LIKE -> {
                if (!containsIgnoreCase(currentValue, searchTerm))
                    map.put(key, value);
            }
            // for arrays
            case CONTAINS -> {
                for (final var obj : (Object[]) currentValue) {
                    if (obj.equals(searchTerm))
                        map.put(key, value);
                }
            }
            case NOT_CONTAINS -> {
                for (final var obj : (Object[]) currentValue) {
                    if (!obj.equals(searchTerm))
                        map.put(key, value);
                }
            }
            case STARTS_WITH -> {
                if (String.valueOf(currentValue).toLowerCase().startsWith(String.valueOf(searchTerm).toLowerCase()))
                    map.put(key, value);
            }
            case ENDS_WITH -> {
                if (String.valueOf(currentValue).toLowerCase().endsWith(String.valueOf(searchTerm).toLowerCase()))
                    map.put(key, value);
            }
            case IN -> {
                if (searchTermList.contains(currentValue))
                    map.put(key, value);
            }
            case NOT_IN -> {
                if (!searchTermList.contains(currentValue))
                    map.put(key, value);
            }
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
    public <K, V> void index(final Map<K, V> db, final String dbName, final List<String> fields,
            final String dataPath, final String indexDirPath) {
        Logger.info("Indexing {} db at {} for : {}.", dbName, dataPath, fields);
        final V sampleValue = db.isEmpty() ? null : db.values().iterator().next();
        if (sampleValue == null)
            return;

        final Map<String, Field> fieldMap = new HashMap<>(fields.size());
        final Map<String, Map<Object, List<K>>> fieldIndexMap = new HashMap<>(fields.size());
        for (final String field : fields) {
            final Field f = getCachedField(sampleValue.getClass(), field);
            if (f != null)
                fieldMap.put(field, f);
        }

        for (final var entry : db.entrySet()) {
            final K key = entry.getKey();
            final V value = entry.getValue();
            for (final String field : fieldMap.keySet()) {
                final Field f = fieldMap.get(field);
                final Map<Object, List<K>> indexMap = fieldIndexMap.computeIfAbsent(field, k -> new HashMap<>());
                try {
                    Object currentValue = f.get(value);
                    if (f.getType().isEnum() || currentValue == null)
                        currentValue = Objects.toString(currentValue, "null");
                    indexMap.computeIfAbsent(currentValue, k -> new ArrayList<>()).add(key);
                } catch (final IllegalAccessException e) {
                    // should not happen, all fields are public
                }
            }
        }

        // Write to disk in parallel
        fieldIndexMap.entrySet().parallelStream().forEach(entry -> {
            final String indexPath = indexDirPath + "/" + entry.getKey();
            final var lock = indexWriteLocks.computeIfAbsent(indexPath, k -> new Object());
            synchronized (lock) {
                final HTreeMap<Object, List<K>> indexDb = MAP_DB.getDb(indexPath);
                indexDb.putAll(entry.getValue());
                MAP_DB.close(indexPath);
            }
        });
    }

    private <K, V> void removeFromIndex(final String dbName, final String dataPath, final Map<K, V> values,
            final String file) {
        final String indexPath = dataPath + "/indexes/" + file;
        if (values.isEmpty())
            return;

        final V sampleValue = values.values().iterator().next();
        final Field field = getCachedField(sampleValue.getClass(), file);
        if (field == null) {
            deleteFileIfExists(indexPath);
            return;
        }
        final boolean isEnum = field.getType().isEnum();

        final Map<Object, List<K>> updatesToRemove = new HashMap<>(values.size());
        for (final var entry : values.entrySet()) {
            final V value = entry.getValue();
            Object indexKey;
            try {
                indexKey = field.get(value);
            } catch (final IllegalAccessException e) {
                // should not happen as all fields are public
                continue;
            }
            if (isEnum || indexKey == null)
                indexKey = Objects.toString(indexKey, "null");
            updatesToRemove.computeIfAbsent(indexKey, k -> new ArrayList<>()).add(entry.getKey());
        }

        final var lock = indexWriteLocks.computeIfAbsent(indexPath, k -> new Object());
        synchronized (lock) {
            final HTreeMap<Object, List<K>> indexDb = MAP_DB.getDb(indexPath);
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
            final List<String> indexFileNames, final Map<K, V> values) {
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
        final Field field = getCachedField(sampleValue.getClass(), file);
        if (field == null) {
            deleteFileIfExists(indexPath);
            return;
        }
        final boolean isEnum = field.getType().isEnum();

        final Map<Object, List<K>> updatesToAdd = new HashMap<>(values.size());
        final Map<Object, List<K>> updatesToRemove = new HashMap<>(prevValues.size());

        for (final K key : values.keySet()) {
            final V newValue = values.get(key);
            final V prevValue = prevValues.get(key);
            Object newIndexKey;
            try {
                newIndexKey = field.get(newValue);
            } catch (final IllegalAccessException e) {
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
                    prevIndexKey = field.get(prevValue);
                } catch (final IllegalAccessException e) {
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
            final HTreeMap<Object, List<K>> indexDb = MAP_DB.getDb(indexPath);
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
            final List<String> indexFileNames, final Map<K, V> values, final Map<K, V> previousValues) {
        indexFileNames.parallelStream().forEach(file -> {
            updateIndex(dbName, dataPath, values, file, previousValues);
        });
    }

    /**
     * Only for chronicle db object types to convert to csv for table display on
     * frontend
     * 
     * @throws SecurityException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     */
    public <K, V> CsvObject formatChronicleDataToCsv(final Map<K, V> map)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        if (map.isEmpty())
            return new CsvObject(new String[0], Collections.emptyList());

        final V sampleValue = map.values().iterator().next();
        final Method headersMethod = sampleValue.getClass().getDeclaredMethod("header");
        final Method rowMethod = sampleValue.getClass().getDeclaredMethod("row", Object.class);
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

        for (final String f : fields) {
            if ("id".equals(f)) {
                valueMap.put(objectName + ".id", key);
            } else {
                try {
                    final Field field = getCachedField(value.getClass(), f);
                    valueMap.put(f, field.get(value));
                } catch (final IllegalAccessException e) {
                    Logger.error("No such field: [{}] when making a subset of {}. {}", f, objectName, e);
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
        final String[] updatedHeaders = copyArray(new String[] { "ID" }, headers);

        for (final var entry : map.entrySet()) {
            final K key = entry.getKey();
            final LinkedHashMap<String, Object> valueMap = entry.getValue();
            final Object[] obj = new Object[valueMap.size() + 1]; // Size based on value map
            obj[0] = key;

            int i = 1;
            for (final Object value : valueMap.values()) { // Iterate values directly
                obj[i++] = value;
            }
            rowList.add(obj);
        }

        return new CsvObject(updatedHeaders, rowList);
    }

    public <V> void partialUpdateSetter(final V object, final String fieldName, final Object fieldValue)
            throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        final var field = getCachedField(object.getClass(), fieldName);
        final var type = field.getType();
        if (type.isEnum())
            field.set(object, toEnum(type, fieldValue));
        else
            field.set(object, fieldValue);
    }

    public <V> void partialUpdateConcatenator(final V object, final String fieldName, final String fieldValue)
            throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        final var field = getCachedField(object.getClass(), fieldName);
        final var value = (String) field.get(object);
        field.set(object, value + fieldValue);
    }

    public void deleteFileIfExists(final String filePath) {
        try {
            Files.delete(Path.of(filePath));
        } catch (final IOException e) {
            Logger.info("No such file {}.", filePath);
        }
    }

    public void move(final Path source, final Path dest) {
        try {
            Files.move(source, dest, REPLACE_EXISTING);
        } catch (final IOException e) {
            Logger.error("Error moving from {}  to {}. {}", source, dest, e);
        }
    }

    public void moveDirContentsStartsWith(final Path src, final Path dest, final String filePrefix)
            throws IOException {
        Files.walk(src).filter(path -> !path.equals(src))
                .filter(path -> path.getFileName().toString().startsWith(filePrefix))
                .forEach(source -> move(source, dest.resolve(src.relativize(source))));
    }

    public <T> T[] copyArray(final T[] prefix, final T[] toCopy) {
        final int aLen = prefix.length;
        final int bLen = toCopy.length;

        final T[] copied = (T[]) Array.newInstance(prefix.getClass().getComponentType(), aLen + bLen);
        System.arraycopy(prefix, 0, copied, 0, aLen);
        System.arraycopy(toCopy, 0, copied, aLen, bLen);

        return copied;
    }

    public Map<String, Object> objectToMap(final Object object, final String objectName, final Object key)
            throws IllegalAccessException {
        final Field[] fields = object.getClass().getDeclaredFields();
        final Map<String, Object> map = new HashMap<>(fields.length + 1); // Pre-size with key
        final String prefix = objectName + "."; // Precompute prefix

        map.put(prefix + "key", key);
        for (final Field field : fields) {
            map.put(prefix + field.getName(), field.get(object));
        }

        return map;
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
                    final Object fieldVal = field.get(currentVal);
                    final Object value = defValue != null
                            ? (f2.getType().isEnum() ? toEnum(f2.getType(), defValue) : defValue)
                            : (f2.getType().isEnum() && fieldVal != null ? toEnum(f2.getType(), fieldVal) : fieldVal);
                    f2.set(newObj, value);
                } catch (final NoSuchFieldException e) {
                }
            }

            for (final Field field : newFieldsSet) { // Use Set for iteration
                final Object defValue = def.get(field.getName());
                if (defValue != null) {
                    final var value = field.getType().isEnum() ? toEnum(field.getType(), defValue) : defValue;
                    field.set(newObj, value);
                }
            }
            map.put(key, newObj);
        }

        return map;
    }
}
