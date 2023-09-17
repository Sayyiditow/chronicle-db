package chronicle.db.dao;

import static chronicle.db.service.MapDb.MAP_DB;

import java.io.IOException;
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
import java.util.concurrent.ConcurrentMap;

import org.mapdb.DB;
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
    public static List<String> getFileList(final String dirPath) {
        final Path path = Path.of(dirPath);
        final List<String> files = new ArrayList<>();

        if (Files.isDirectory(path)) {
            try {
                Files.list(path).map(Path::getFileName).map(Path::toString).forEach(files::add);
            } catch (final IOException e) {
                Logger.error(e.getMessage());
            }
        } else {
            Logger.info("Path: {} is not a directory", dirPath);
        }

        return files;
    }

    public <K, V> void search(final Search search, final K key, final V value, final ConcurrentMap<K, V> map)
            throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        final Field field = value.getClass().getField(search.field());
        if (Objects.nonNull(field)) {
            final Object searchTerm = search.searchTerm();
            final Object currentValue = field.get(value);

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
                    if (new BigDecimal(currentValue.toString())
                            .compareTo(new BigDecimal(searchTerm.toString())) < 0)
                        map.put(key, value);
                    break;
                case GREATER:
                    if (new BigDecimal(currentValue.toString())
                            .compareTo(new BigDecimal(searchTerm.toString())) > 0)
                        map.put(key, value);
                    break;
                case LESS_OR_EQUAL:
                    if (new BigDecimal(currentValue.toString())
                            .compareTo(new BigDecimal(searchTerm.toString())) <= 0)
                        map.put(key, value);
                    break;
                case GREATER_OR_EQUAL:
                    if (new BigDecimal(currentValue.toString())
                            .compareTo(new BigDecimal(searchTerm.toString())) >= 0)
                        map.put(key, value);
                    break;
                case LIKE:
                    if (String.valueOf(currentValue).contains(String.valueOf(searchTerm)))
                        map.put(key, value);
                    break;
                case NOT_LIKE:
                    if (!String.valueOf(currentValue).contains(String.valueOf(searchTerm)))
                        map.put(key, value);
                    break;
                case CONTAINS:
                    if (Collections.singleton(currentValue).contains(searchTerm))
                        map.put(key, value);
                    break;
                case STARTS_WITH:
                    if (String.valueOf(currentValue).startsWith(String.valueOf(searchTerm)))
                        map.put(key, value);
                    break;
                case ENDS_WITH:
                    if (String.valueOf(currentValue).endsWith(String.valueOf(searchTerm)))
                        map.put(key, value);
                    break;
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
                    final Object currentValue = f.get(entry.getValue());
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

    /**
     * Update the index
     * 
     * @param dataPath the folder path
     * @param field    the value object field enum
     * @param indexKey the key of the index
     * @return boolean value true or false if updated
     * @throws IOException
     */
    private <K> boolean removeFromIndex(final String dataPath, final String field, final Object indexKey,
            final K value) throws IOException {
        final String filePath = dataPath + "/indexes/" + field;
        final DB indexDb = MAP_DB.db(filePath);
        final ConcurrentMap<Object, List<K>> index = MAP_DB.getMapDb(indexDb);
        final List<K> keys = index.get(indexKey);

        if (keys.remove(value)) {
            index.put(indexKey, keys);
            indexDb.close();
            return true;
        }
        indexDb.close();

        return false;
    }

    /**
     * Update the index
     * 
     * @param dataPath the folder path
     * @param field    the value object field enum
     * @param indexKey the key of the index
     * @return boolean value true or false if updated
     * @throws IOException
     */
    private <K> boolean addToIndex(final String dataPath, final String field, final Object indexKey,
            final K value) throws IOException {
        final String filePath = dataPath + "/indexes/" + field;
        final DB indexDb = MAP_DB.db(filePath);
        final ConcurrentMap<Object, List<K>> index = MAP_DB.getMapDb(indexDb);
        List<K> keys = index.get(indexKey);
        if (Objects.isNull(keys)) {
            keys = new ArrayList<>();
        }

        if (keys.add(value)) {
            index.put(indexKey, keys);
            indexDb.close();
            return true;
        }
        indexDb.close();

        return false;
    }

    public <K, V> void removeFromIndex(final String dbName, final String dataPath, final List<String> indexFileNames,
            final K key, final V value) throws IOException {
        for (final String file : indexFileNames) {
            Field field = null;
            Object indexKey = null;
            try {
                field = value.getClass().getField(file);
                if (Objects.nonNull(field)) {
                    indexKey = field.get(value);
                }
            } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException e) {
                Logger.error("No such field exists {} when removing from index {}. {}", file, dbName, e);
            }
            removeFromIndex(dataPath, file, indexKey, key);
        }
    }

    public <K, V> void addToIndex(final String dbName, final String dataPath, final List<String> indexFileNames,
            final K key, final V value) throws IOException {
        for (final String file : indexFileNames) {
            Field field = null;
            Object indexKey = null;
            try {
                field = value.getClass().getField(file);
                if (Objects.nonNull(field)) {
                    indexKey = field.get(value);
                }
            } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException e) {
                Logger.error("No such field exists {} when adding to index {}. {}", file, dbName, e);
            }
            addToIndex(dataPath, file, indexKey, key);
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
        final V value = map.values().iterator().next();
        final Method headersMethod = value.getClass().getDeclaredMethod("headers");
        final Method rowMethod = value.getClass().getDeclaredMethod("row", Object.class);
        final String[] headerList = (String[]) headersMethod.invoke(value);
        final List<Object[]> rowList = new ArrayList<>();

        for (final var entry : map.entrySet()) {
            rowList.add((Object[]) rowMethod.invoke(entry.getValue(), entry.getKey()));
        }

        return new CsvObject(headerList, rowList);
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
        final V value = map.values().iterator().next().values().iterator().next();
        final Method headersMethod = value.getClass().getDeclaredMethod("headers");
        final Method rowMethod = value.getClass().getDeclaredMethod("row", Object.class);
        final String[] headerList = (String[]) headersMethod.invoke(value);
        final List<Object[]> rowList = new ArrayList<>();

        if (map.size() > 2) {
            map.entrySet().parallelStream().forEach(HandleConsumer.handleConsumerBuilder(entry -> {
                for (final var e : entry.getValue().entrySet()) {
                    rowList.add((Object[]) rowMethod.invoke(e.getValue(), e.getKey()));
                }
            }));
            return new CsvObject(headerList, rowList);
        }

        for (final var entry : map.entrySet()) {
            for (final var e : entry.getValue().entrySet()) {
                rowList.add((Object[]) rowMethod.invoke(e.getValue(), e.getKey()));
            }
        }

        return new CsvObject(headerList, rowList);
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
    public <K> CsvObject formatSubsetChronicleDataToCsv(
            final ConcurrentMap<K, LinkedHashMap<String, Object>> map, final String[] headers)
            throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException {
        final List<Object[]> rowList = new ArrayList<>();

        for (final var entry : map.entrySet()) {
            int i = 0;
            final var obj = new Object[entry.getValue().size()];
            for (final var ent : entry.getValue().entrySet()) {
                obj[i] = ent.getValue();
                i++;
            }
            rowList.add(obj);
        }

        return new CsvObject(headers, rowList);
    }

    public <V> void partialUpdateSetter(final V object, final String fieldName, final Object fieldValue,
            final Class<?> enumClass)
            throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        final var field = object.getClass().getField(fieldName);
        if (enumClass != null) {
            field.set(object,
                    Enum.valueOf((Class<? extends Enum>) enumClass, fieldValue.toString()));
            return;
        }

        field.set(object, fieldValue);
    }

    public void deleteFileIfExists(final String filePath) {
        try {
            Files.delete(Paths.get(filePath));
        } catch (final IOException e) {
            Logger.info("No such index file {}.", filePath);
        }
    }
}
