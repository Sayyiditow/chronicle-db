package chronicle.db.dao;

import static chronicle.db.service.MapDb.MAP_DB;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.mapdb.HTreeMap;
import org.tinylog.Logger;

import chronicle.db.entity.CsvObject;
import chronicle.db.entity.Search;
import chronicle.db.service.HandleConsumer;

@SuppressWarnings({ "unchecked", "rawtypes" })
public final class ChronicleUtils {
    public static final ChronicleUtils CHRONICLE_UTILS = new ChronicleUtils();

    public <K> void getLog(final String name, final K key) {
        Logger.info("Querying {} using key {}.", name, key);
    }

    public <K> void deleteLog(final String name, final K key) {
        Logger.info("Deleting from {} using key {}.", name, key);
    }

    public void deleteAllLog(final String name) {
        Logger.info("Deleting from db: {} using multiple keys", name);
    }

    public <K> void successDeleteLog(final String name, final K key) {
        Logger.info("Object with key {} deleted from {}.", key, name);
    }

    public void dbFetchError(final String name, final String file) {
        Logger.error("Error while fetching {} for file {}", name, file);
    }

    /**
     * Retrieve a list of files in a dirPath and throw an exception is dirPath is
     * null
     *
     * @param dirPath dirPath to retrieve files from
     * @return a list of files
     */
    public static List<String> getFileList(final String dirPath) throws IOException {
        try (Stream<Path> stream = Files.list(Paths.get(dirPath))) {
            return stream
                    .filter(file -> !Files.isDirectory(file))
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .collect(Collectors.toList());
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
        return Enum.valueOf((Class<Enum>) enumClass, value.toString());
    }

    public <K, V> void search(final Search search, final K key, final V value, final ConcurrentMap<K, V> map)
            throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        final Field field = value.getClass().getField(search.field());

        if (Objects.nonNull(field)) {
            final Object searchTerm = field.getType().isEnum() && (search.searchTerm() instanceof String)
                    ? toEnum(field.getType(), search.searchTerm())
                    : search.searchTerm();
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
                        final Set<Object> set = (Set<Object>) searchTerm;
                        if (set.contains(currentValue))
                            map.put(key, value);
                        break;
                    case NOT_IN:
                        final Set<Object> set2 = (Set<Object>) searchTerm;
                        if (!set2.contains(currentValue))
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
    public <K, V> void index(final ConcurrentMap<K, V> db, final String dbName, final String field,
            final HTreeMap<String, Map<Object, List<K>>> index, final String fileName)
            throws IOException {
        Logger.info("Indexing {} db using {}.", dbName, field);
        final var copy = new HashMap<Object, List<K>>();

        for (final var entry : db.entrySet()) {
            Field f = null;
            try {
                f = entry.getValue().getClass().getField(field);
                if (Objects.nonNull(field)) {
                    final Object currentValue = !f.getType().isEnum() ? f.get(entry.getValue())
                            : f.get(entry.getValue()).toString();
                    List<K> keys = copy.get(currentValue);
                    if (Objects.isNull(keys)) {
                        keys = new ArrayList<>();
                    }
                    keys.add(entry.getKey());
                    copy.put(currentValue, keys);
                }
            } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException e) {
                Logger.error("No such field exists {} when indexing {}. {}", field, dbName, e);
                break;
            }
        }

        index.put(fileName, copy);
    }

    private <K, V> void removeFromIndex(final String dbFileName, final String dbName, final String dataPath,
            final Map<K, V> values, final String file) {
        Field field = null;
        Object indexKey = null;
        try {
            final HTreeMap<String, Map<Object, List<K>>> indexDb = MAP_DB.getDb(dataPath + "/indexes/" + file);
            final var index = indexDb.get(dbFileName);

            for (final var entry : values.entrySet()) {
                final var value = entry.getValue();
                field = value.getClass().getField(file);
                if (Objects.nonNull(field)) {
                    indexKey = !field.getType().isEnum() ? field.get(value)
                            : field.get(value).toString();
                }
                final List<K> keys = index.get(indexKey);
                if (keys.remove(entry.getKey())) {
                    index.put(indexKey, keys);
                    indexDb.put(dbFileName, index);
                }
            }
            indexDb.close();
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException e) {
            Logger.error("No such field exists {} when removing from index {}. {}", file, dbName, e);
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
     */
    public <K, V> void removeFromIndex(final String dbFileName, final String dbName, final String dataPath,
            final List<String> indexFileNames, final Map<K, V> values) throws IOException {
        if (indexFileNames.size() > 1)
            indexFileNames.parallelStream().forEach(file -> {
                removeFromIndex(dbFileName, dbName, dataPath, values, file);
            });
        else
            for (final String file : indexFileNames) {
                removeFromIndex(dbFileName, dbName, dataPath, values, file);
            }
    }

    private <K, V> void updateIndex(final String dbFileName, final String dbName, final String dataPath,
            final Map<K, V> values, final String file, final Map<K, V> prevValues) {
        Field field = null;
        Object indexKey = null;
        try {
            final HTreeMap<String, Map<Object, List<K>>> indexDb = MAP_DB.getDb(dataPath + "/indexes/" + file);
            final var index = indexDb.get(dbFileName);

            // remove from the index first
            for (final var entry : prevValues.entrySet()) {
                final var value = entry.getValue();
                field = value.getClass().getField(file);
                if (Objects.nonNull(field)) {
                    indexKey = !field.getType().isEnum() ? field.get(value)
                            : field.get(value).toString();
                }
                final List<K> keys = index.get(indexKey);
                if (keys.remove(entry.getKey())) {
                    index.put(indexKey, keys);
                    indexDb.put(dbFileName, index);
                }
            }

            for (final var entry : values.entrySet()) {
                final var value = entry.getValue();
                field = value.getClass().getField(file);
                if (Objects.nonNull(field)) {
                    indexKey = !field.getType().isEnum() ? field.get(value)
                            : String.valueOf(field.get(value));
                }

                List<K> keys = index.get(indexKey);
                if (Objects.isNull(keys)) {
                    keys = new ArrayList<>();
                }

                if (!keys.contains(entry.getKey())) {
                    if (keys.add(entry.getKey())) {
                        index.put(indexKey, keys);
                        indexDb.put(dbFileName, index);
                    }
                }
            }
            indexDb.close();
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException e) {
            Logger.error("No such field exists {} when adding to index {}. {}", file, dbName, e);
        }
    }

    /**
     * Update indexes by removing first then adding them
     * 
     * @param dbFileName     the file where the data is stored
     * @param dataPath       the folder path
     * @param field          the value object field enum
     * @param indexFileNames index files
     * @throws IOException
     */
    public <K, V> void updateIndex(final String dbFileName, final String dbName, final String dataPath,
            final List<String> indexFileNames, final Map<K, V> values, final Map<K, V> previousValues)
            throws IOException {
        if (indexFileNames.size() > 1)
            indexFileNames.parallelStream().forEach(file -> {
                updateIndex(dbFileName, dbName, dataPath, values, file, previousValues);
            });
        else
            for (final String file : indexFileNames) {
                updateIndex(dbFileName, dbName, dataPath, values, file, previousValues);
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
    public <K, V> CsvObject formatSingleChronicleDataToCsv(final ConcurrentMap<K, V> map)
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
        return new CsvObject(new String[] {}, new ArrayList<>());
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
    public static <K, V> CsvObject formatMultiChronicleDataToCsv(final ConcurrentMap<String, ConcurrentMap<K, V>> map)
            throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException {
        if (map.size() != 0) {
            final V value = map.values().iterator().next().values().iterator().next();
            final Method headersMethod = value.getClass().getDeclaredMethod("header");
            final Method rowMethod = value.getClass().getDeclaredMethod("row", Object.class);
            final String[] headerList = (String[]) headersMethod.invoke(value);
            final List<Object[]> rowList = new ArrayList<>();

            if (map.size() > 2) {
                map.entrySet().parallelStream().forEach(HandleConsumer.handleConsumerBuilder(entry -> {
                    for (final var e : entry.getValue().entrySet()) {
                        rowList.add((Object[]) rowMethod.invoke(e.getValue(), entry.getKey(), e.getKey()));
                    }
                }));
                return new CsvObject(headerList, rowList);
            }

            for (final var entry : map.entrySet()) {
                for (final var e : entry.getValue().entrySet()) {
                    rowList.add((Object[]) rowMethod.invoke(e.getValue(), entry.getKey(), e.getKey()));
                }
            }

            return new CsvObject(headerList, rowList);
        }
        return new CsvObject(new String[] {}, new ArrayList<>());
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
    public <K> CsvObject formatSubsetChronicleDataToCsv(final ConcurrentMap<K, LinkedHashMap<String, Object>> map,
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
            Files.delete(Paths.get(filePath));
        } catch (final IOException e) {
            Logger.info("No such index file {}.", filePath);
        }
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

    public void runInThreadPool(final List<Runnable> runnableList, final String threadPoolIdentifier)
            throws InterruptedException {
        final ThreadFactory factory = Thread.ofVirtual().name(threadPoolIdentifier).factory();
        try (final ExecutorService executorService = Executors.newThreadPerTaskExecutor(factory)) {
            runnableList.forEach(r -> executorService.execute(r));
            executorService.shutdown();
            if (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                executorService.shutdownNow();
                Logger.info("Threadpool: {} successfully shutdown. {} tasks completed.", threadPoolIdentifier,
                        runnableList.size());
            }
        }
    }
}
