package chronicle.db.dao;

import static chronicle.db.service.MapDb.MAP_DB;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.lang.reflect.Array;
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
    public static final ChronicleUtils CHRONICLE_UTILS = new ChronicleUtils();
    private static final ConcurrentMap<String, Object> LOCKS = new ConcurrentHashMap<>();

    public <K> void getLog(final String name, final K key, final String path) {
        Logger.info("Querying {} using key {} at {}.", name, key, path);
    }

    public <K> void deleteLog(final String name, final K key, final String path) {
        Logger.info("Deleting from {} using key {} at {}.", name, key, path);
    }

    public <K> void deleteAllLog(final String name, final Set<K> keys) {
        Logger.info("Deleting from db: {} using multiple keys {}.", name, keys);
    }

    public <K> void successDeleteLog(final String name, final K key, final String path) {
        Logger.info("Object with key {} deleted from {} at {}.", key, name, path);
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
            if (searchTerm != null) {
                if (searchTerm.getClass().isEnum()) {
                    searchTerms.set(i, String.valueOf(searchTerm));
                } else if (fieldClass.isAssignableFrom(long.class)
                        && (searchTerm instanceof String || searchTerm instanceof Integer
                                || searchTerm.getClass().isAssignableFrom(int.class))) {
                    searchTerms.set(i, toEnum(fieldClass, Long.parseLong(searchTerm.toString())));
                }
            } else {
                searchTerms.set(i, String.valueOf(searchTerm));
            }
        }

        return searchTerms;
    }

    public Object setSearchTerm(final Object searchTerm, final Class<?> fieldClass) {
        if (searchTerm != null) {
            if (searchTerm.getClass().isEnum()) {
                return String.valueOf(searchTerm);
            } else if (fieldClass.isAssignableFrom(long.class)
                    && (searchTerm instanceof String || searchTerm instanceof Integer
                            || searchTerm.getClass().isAssignableFrom(int.class))) {
                return Long.parseLong(searchTerm.toString());
            }
            return searchTerm;
        }

        return "null";
    }

    public List<Object> setSearchTermNonIndexed(final List<Object> searchTerms, final Class<?> fieldClass) {
        for (int i = 0; i < searchTerms.size(); i++) {
            final var searchTerm = searchTerms.get(i);
            if (searchTerm != null) {
                if (fieldClass.isEnum() && (searchTerm instanceof String)) {
                    searchTerms.set(i, toEnum(fieldClass, searchTerm));
                } else if (fieldClass.isAssignableFrom(long.class)
                        && (searchTerm instanceof String || searchTerm instanceof Integer
                                || searchTerm.getClass().isAssignableFrom(int.class))) {
                    searchTerms.set(i, toEnum(fieldClass, Long.parseLong(searchTerm.toString())));
                }
            }
        }

        return searchTerms;
    }

    public Object setSearchTermNonIndexed(final Object searchTerm, final Class<?> fieldClass) {
        if (fieldClass.isEnum() && (searchTerm instanceof String)) {
            return toEnum(fieldClass, searchTerm);
        } else if (fieldClass.isAssignableFrom(long.class)
                && (searchTerm instanceof String || searchTerm instanceof Integer
                        || searchTerm.getClass().isAssignableFrom(int.class))) {
            return Long.parseLong(searchTerm.toString());
        }

        return searchTerm;

    }

    public <K, V> void search(final Search search, final K key, final V value, final Map<K, V> map)
            throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        final Field field = value.getClass().getField(search.field());
        List<Object> searchTermList = new ArrayList<>();

        if (Objects.nonNull(field)) {
            final Object searchTerm = setSearchTermNonIndexed(search.searchTerm(), field.getType());
            if (search.searchType() == SearchType.IN || search.searchType() == SearchType.NOT_IN) {
                searchTermList = setSearchTermNonIndexed((List<Object>) search.searchTerm(), field.getType());
            }
            final Object currentValue = field.get(value);

            if (Objects.nonNull(currentValue)) {
                switch (search.searchType()) {
                    case EQUAL:
                        if (currentValue.equals(searchTerm))
                            map.put(key, value);
                        break;
                    case NOT_EQUAL:
                        if (!currentValue.equals(searchTerm))
                            map.put(key, value);
                        break;
                    case LESS:
                        if (compare(currentValue, searchTerm) < 0)
                            map.put(key, value);
                        break;
                    case GREATER:
                        if (compare(currentValue, searchTerm) > 0)
                            map.put(key, value);
                        break;
                    case LESS_OR_EQUAL:
                        if (compare(currentValue, searchTerm) <= 0)
                            map.put(key, value);
                        break;
                    case GREATER_OR_EQUAL:
                        if (compare(currentValue, searchTerm) >= 0)
                            map.put(key, value);
                        break;
                    case LIKE:
                        if (containsIgnoreCase(currentValue, searchTerm))
                            map.put(key, value);
                        break;
                    case NOT_LIKE:
                        if (!containsIgnoreCase(currentValue, searchTerm))
                            map.put(key, value);
                        break;
                    case CONTAINS:
                        if (Collections.singleton(currentValue).contains(searchTerm))
                            map.put(key, value);
                        break;
                    case NOT_CONTAINS:
                        if (!Collections.singleton(currentValue).contains(searchTerm))
                            map.put(key, value);
                        break;
                    case STARTS_WITH:
                        if (String.valueOf(currentValue).toLowerCase()
                                .startsWith(String.valueOf(searchTerm).toLowerCase()))
                            map.put(key, value);
                        break;
                    case ENDS_WITH:
                        if (String.valueOf(currentValue).toLowerCase()
                                .endsWith(String.valueOf(searchTerm).toLowerCase()))
                            map.put(key, value);
                        break;
                    case IN:
                        if (searchTermList.contains(currentValue))
                            map.put(key, value);
                        break;
                    case NOT_IN:
                        if (!searchTermList.contains(currentValue))
                            map.put(key, value);
                        break;
                }
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
    public <K, V> void index(final Map<K, V> db, final String dbName, final String[] fields,
            final String dataPath, final String indexDirPath) {
        final Map<String, Map<Object, List<K>>> fieldIndexMap = new HashMap<>();
        final Map<String, Field> fieldMap = new HashMap<>();
        final var nonExistentFields = new HashMap<>();
        Logger.info("Indexing {} db at {} for : {}.", dbName, dataPath, Arrays.toString(fields));

        for (final var entry : db.entrySet()) {
            for (final var field : fields) {
                if (!nonExistentFields.containsKey(field)) {
                    final Field f = fieldMap.computeIfAbsent(field, k -> {
                        try {
                            return entry.getValue().getClass().getField(field);
                        } catch (final NoSuchFieldException e) {
                            Logger.error("No such field exists {} when indexing {} at {}. {}", field, dbName, dataPath,
                                    e.getMessage());
                            return null;
                        }
                    });
                    if (f == null) {
                        deleteFileIfExists(indexDirPath + "/" + field);
                        nonExistentFields.put(field, true);
                        continue;
                    }
                    final var indexMap = fieldIndexMap.computeIfAbsent(field, k -> new HashMap<>());
                    try {
                        var currentValue = f.get(entry.getValue());
                        if (f.getType().isEnum() || currentValue == null)
                            currentValue = Objects.toString(currentValue, "null");
                        indexMap.computeIfAbsent(currentValue, k -> new ArrayList<>()).add(entry.getKey());
                    } catch (IllegalArgumentException | IllegalAccessException e) {
                        Logger.error("Error getting field value for {} at {}. {}", field, dbName, e.getMessage());
                    }
                }
            }
        }

        for (final var entry : fieldIndexMap.entrySet()) {
            final var indexPath = indexDirPath + "/" + entry.getKey();
            final Object lock = LOCKS.computeIfAbsent(indexPath, k -> new Object());

            synchronized (lock) {
                deleteFileIfExists(indexPath);
                final HTreeMap<Object, List<K>> indexDb = MAP_DB.getDb(indexPath);
                indexDb.putAll(entry.getValue());
                indexDb.close();
            }

        }
    }

    private <K> void removeKeyFromIndex(final HTreeMap<Object, List<K>> indexDb, final Object indexKey, final K key) {
        final List<K> prevKeys = indexDb.get(indexKey);
        if (prevKeys != null && prevKeys.remove(key)) {
            if (prevKeys.isEmpty()) {
                indexDb.remove(indexKey);
            } else {
                indexDb.put(indexKey, prevKeys);
            }
        }
    }

    private <K, V> void removeFromIndex(final String dbName, final String dataPath, final Map<K, V> values,
            final String file) {
        final var indexPath = dataPath + "/indexes/" + file;
        final Object lock = LOCKS.computeIfAbsent(indexPath, k -> new Object());

        synchronized (lock) {
            final HTreeMap<Object, List<K>> indexDb = MAP_DB.getDb(indexPath);
            try {
                Logger.info("Removing from index {} at {}.", file, dataPath);
                for (final var entry : values.entrySet()) {
                    final V value = entry.getValue();
                    final var field = value.getClass().getField(file);
                    var indexKey = field.get(value);
                    if (field.getType().isEnum() || indexKey == null)
                        indexKey = Objects.toString(indexKey, "null");
                    removeKeyFromIndex(indexDb, indexKey, entry.getKey());
                }
            } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException e) {
                Logger.error("No such field exists {} when removing from index {} at {}. {}", file, dbName, dataPath,
                        e);
                deleteFileIfExists(indexPath);
            } finally {
                indexDb.close();
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
        for (final var file : indexFileNames) {
            removeFromIndex(dbName, dataPath, values, file);
        }
    }

    private <K> void addKeyToIndex(final HTreeMap<Object, List<K>> indexDb, final Object indexKey, final K key) {
        final List<K> keys = indexDb.computeIfAbsent(indexKey, k -> new ArrayList<>());
        if (!keys.contains(key) && keys.add(key)) {
            indexDb.put(indexKey, keys);
        }
    }

    private <K, V> void updateIndex(final String dbName, final String dataPath,
            final Map<K, V> values, final String file, final Map<K, V> prevValues) {
        final var indexPath = dataPath + "/indexes/" + file;
        final Object lock = LOCKS.computeIfAbsent(indexPath, k -> new Object());

        synchronized (lock) {
            final HTreeMap<Object, List<K>> indexDb = MAP_DB.getDb(indexPath);
            try {
                for (final var key : values.keySet()) {
                    final V newValue = values.get(key);
                    final V prevValue = prevValues.get(key);
                    final var field = newValue.getClass().getField(file);
                    var newIndexKey = field.get(newValue);

                    if (prevValue == null) {
                        if (field.getType().isEnum() || newIndexKey == null)
                            newIndexKey = Objects.toString(newIndexKey, "null");

                        Logger.info("Adding new index {} on {} at {}.", newIndexKey, file, dataPath);
                        addKeyToIndex(indexDb, newIndexKey, key);
                    } else {
                        var prevIndexKey = field.get(prevValue);
                        if (!Objects.equals(newIndexKey, prevIndexKey)) {
                            if (field.getType().isEnum()) {
                                prevIndexKey = String.valueOf(prevIndexKey);
                                newIndexKey = String.valueOf(newIndexKey);
                            }
                            if (prevIndexKey == null) {
                                prevIndexKey = "null";
                            }
                            if (newIndexKey == null) {
                                newIndexKey = "null";
                            }

                            Logger.info("Updating index {} to {} on {} at {}.", prevIndexKey, newIndexKey, file,
                                    dataPath);
                            removeKeyFromIndex(indexDb, prevIndexKey, key);
                            addKeyToIndex(indexDb, newIndexKey, key);
                        }
                    }
                }
            } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException e) {
                Logger.error("No such field exists {} when adding to index {} at. {}", file, dbName, dataPath, e);
                deleteFileIfExists(indexPath);
            } finally {
                indexDb.close();
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
        for (final var file : indexFileNames) {
            updateIndex(dbName, dataPath, values, file, previousValues);
        }
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
            throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException {
        if (map.size() != 0) {
            final V value = map.values().iterator().next();
            final Method headersMethod = value.getClass().getDeclaredMethod("header");
            final Method rowMethod = value.getClass().getDeclaredMethod("row", Object.class);
            final String[] headerList = (String[]) headersMethod.invoke(value);
            final List<Object[]> rowList = new ArrayList<>();

            for (final var entry : map.entrySet()) {
                rowList.add((Object[]) rowMethod.invoke(entry.getValue(), entry.getKey()));
            }

            return new CsvObject(headerList, rowList);
        }
        return new CsvObject(new String[] {}, List.of());
    }

    public <K, V> void subsetOfValues(final String[] fields, final Map.Entry<K, V> entry,
            final Map<K, LinkedHashMap<String, Object>> map, final String objectName) {
        Field field = null;
        final var valueMap = new LinkedHashMap<String, Object>();
        for (final var f : fields) {
            if (f.equals("id")) {
                valueMap.put(objectName + ".id", entry.getKey());
            } else {
                try {
                    field = entry.getValue().getClass().getField(f);
                    if (Objects.nonNull(field)) {
                        valueMap.put(f, field.get(entry.getValue()));
                    }
                } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException e) {
                    Logger.error("No such field: {} when making a subset of {}. {}", f, objectName, e);
                }
            }
        }
        map.put(entry.getKey(), valueMap);
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
    public <K> CsvObject formatSubsetChronicleDataToCsv(final Map<K, LinkedHashMap<String, Object>> map,
            final String[] headers)
            throws NoSuchMethodException, SecurityException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException {
        final List<Object[]> rowList = new ArrayList<>();
        final String[] updatedHeaders = copyArray(new String[] { "ID" }, headers);

        for (final var entry : map.entrySet()) {
            int i = 1;
            final var obj = new Object[entry.getValue().size() + 1];
            obj[0] = entry.getKey();
            for (final var ent : entry.getValue().entrySet()) {
                obj[i] = ent.getValue();
                i++;
            }
            rowList.add(obj);
        }

        return new CsvObject(updatedHeaders, rowList);
    }

    public <V> void partialUpdateSetter(final V object, final String fieldName, final Object fieldValue)
            throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        final var field = object.getClass().getField(fieldName);
        final var type = field.getType();
        if (type.isEnum())
            field.set(object, toEnum(type, fieldValue));
        else
            field.set(object, fieldValue);
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
        final Map<String, Object> map = new HashMap<>();
        final Field[] fields = object.getClass().getDeclaredFields();
        map.put(objectName + ".key", key);

        for (final Field field : fields) {
            map.put(objectName + "." + field.getName(), field.get(object));
        }

        return map;
    }

    public <K, V> Map<K, Object> moveRecords(final Map<K, V> currentValues,
            final String toObjectClass, final Map<String, String> move, final Map<String, Object> def)
            throws ClassNotFoundException, NoSuchMethodException, SecurityException,
            InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        final Map<K, Object> map = new HashMap<>();
        if (currentValues.size() != 0) {
            final var cls = Class.forName(toObjectClass);
            final var constuctor = cls.getConstructor();
            final Field[] fields = currentValues.values().stream().findFirst().get().getClass().getDeclaredFields();
            final var newFields = new ArrayList<>(
                    Arrays.asList(constuctor.newInstance().getClass().getDeclaredFields()));
            newFields.removeAll(new ArrayList<>(Arrays.asList(fields)));

            for (final var entry : currentValues.entrySet()) {
                final var newObj = constuctor.newInstance();
                final var currentVal = entry.getValue();
                for (final var field : fields) {
                    final var fieldName = field.getName();
                    final var destMoveFieldName = move.get(fieldName);
                    final var destFieldName = destMoveFieldName != null ? destMoveFieldName : fieldName;
                    final var defValue = def.get(fieldName);

                    try {
                        final var f2 = newObj.getClass().getField(destFieldName);
                        final var fieldVal = field.get(currentVal);
                        if (defValue != null) {
                            final Object value = f2.getType().isEnum()
                                    ? toEnum(f2.getType(), defValue)
                                    : defValue;
                            f2.set(newObj, value);
                            continue;
                        }
                        final Object value = f2.getType().isEnum() && fieldVal != null
                                ? toEnum(f2.getType(), fieldVal)
                                : fieldVal;
                        f2.set(newObj, value);
                    } catch (final NoSuchFieldException e) {
                        Logger.info("Field from source object does not exist in destination object: {}.", fieldName);
                    }
                }

                for (final var en : def.entrySet()) {
                    for (final var field : newFields) {
                        final var fieldName = field.getName();
                        if (fieldName.equals(en.getKey())) {
                            field.set(newObj, en.getValue());
                        }
                    }
                }
                map.put(entry.getKey(), newObj);
            }
        }
        return map;
    }
}
