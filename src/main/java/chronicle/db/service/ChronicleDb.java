package chronicle.db.service;

import static chronicle.db.dao.ChronicleUtils.CHRONICLE_UTILS;
import static chronicle.db.service.MapDb.MAP_DB;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.mapdb.HTreeMap;
import org.tinylog.Logger;

import chronicle.db.dao.MultiChronicleDao;
import chronicle.db.dao.SingleChronicleDao;
import chronicle.db.entity.CsvObject;
import net.openhft.chronicle.map.ChronicleMap;

/**
 * Using this DB requires to use Value interfaces from Chronical Map:
 * https://github.com/OpenHFT/Chronicle-Values
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public final class ChronicleDb {
    private ChronicleDb() {
    }

    public static final ChronicleDb CHRONICLE_DB = new ChronicleDb();

    /**
     * Create or fetch a db
     * 
     * @param entries    the number of entries of the db as a starter
     * @param averageKey the average key
     * @param filePath   the path to the file to create
     * @param keyClass   the class of the key
     * @param valueClass the class of the value (best to implement Value interface
     *                   for complex structures)
     * @throws IOException
     */
    public <K, V> ChronicleMap<K, V> createOrGet(final String name, final long entries,
            final K averageKey, final V averageValue, final String filePath) throws IOException {
        final File file = new File(filePath);
        final Class<K> keyClass = (Class<K>) averageKey.getClass();
        final Class<V> valueClass = (Class<V>) averageValue.getClass();

        if (file.exists()) {
            Logger.info("Fetching Chronicle DB at: {}", filePath);
            return ChronicleMap.of(keyClass, valueClass).createPersistedTo(file);
        }

        Logger.info("Creating Chronicle DB at: {}", filePath);
        return ChronicleMap.of(keyClass, valueClass).name(name).entries(entries).averageKey(averageKey)
                .averageValue(averageValue).createPersistedTo(file);
    }

    /**
     * Run this on app startup to check and fix if there were any abnormal
     * terminations
     * 
     * @param filePath   the path to the file to with the data
     * @param keyClass   the class of the key
     * @param valueClass the class of the value (best to implement Value interface
     *                   for complex structures)
     */
    public <K, V> ChronicleMap<K, V> recoverDb(final String filePath, final Class<K> keyClass,
            final Class<V> valueClass) throws IOException {
        final File file = new File(filePath);
        Logger.info("Restoring Chronicle DB at: {}", filePath);
        return ChronicleMap.of(keyClass, valueClass).recoverPersistedTo(file, false);
    }

    /**
     * Get the object constructor reflectively to be used when inserting/updating
     * records
     * 
     * @throws ClassNotFoundException
     * @return Constructor<?>
     */

    public Constructor<?> getObjectConstructor(final String objectClassName) throws ClassNotFoundException {
        final var objClass = Class.forName(objectClassName);

        final Constructor<?>[] constructors = objClass.getDeclaredConstructors();
        Constructor<?> c = null;
        for (final Constructor<?> con : constructors) {
            if (con.getParameterCount() > 0) {
                c = con;
            }
        }

        return c;
    }

    /**
     * Constructs the class using reflection
     */
    public Object constructObject(final String objectClassName, final Object[] values) throws ClassNotFoundException,
            InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        final var con = getObjectConstructor(objectClassName);
        final var params = con.getParameterTypes();
        final Object[] preparedValues = new Object[params.length];

        if (params.length != values.length) {
            Logger.error("Length of parameters supplied does not match.");
            return null;
        }

        for (int i = 0; i < params.length; i++) {
            if (params[i].isEnum())
                preparedValues[i] = Enum.valueOf((Class<Enum>) params[i], values[i].toString());
            else
                preparedValues[i] = values[i];
        }

        return con.newInstance(preparedValues);
    }

    /**
     * Gets the multichronicle dao object to run different methods such as CRUD
     * reflectively
     * 
     * @param daoClassName       the full package class name for the dao
     * @param daoClassObjectName the static object name
     * 
     * @return MultiChronicleDao
     * @throws InvocationTargetException
     * @throws InstantiationException
     */
    public MultiChronicleDao getMultiChronicleDao(final String daoClassName, final String dataPath)
            throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException, NoSuchFieldException,
            SecurityException, InstantiationException, InvocationTargetException {
        final var c = getObjectConstructor(daoClassName);
        return (MultiChronicleDao) c.newInstance(dataPath);
    }

    /**
     * Gets the singlechronicle dao object to run different methods such as CRUD
     * reflectively
     * 
     * @param daoClassName       the full package class name for the dao
     * @param daoClassObjectName the static object name
     * 
     * @return SingleChronicleDao
     * @throws InvocationTargetException
     * @throws InstantiationException
     */
    public SingleChronicleDao getSingleChronicleDao(final String daoClassName, final String dataPath)
            throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException, NoSuchFieldException,
            SecurityException, InstantiationException, InvocationTargetException {
        final var c = getObjectConstructor(daoClassName);
        return (SingleChronicleDao) c.newInstance(dataPath);
    }

    private void loopJoinToMap(final Entry<String, Map<Object, List<Object>>> e,
            final ChronicleMap<Object, Object> objectA, final ChronicleMap<Object, Object> objectB,
            final Object objectAUsing, final Object objectBUsing,
            final ConcurrentMap<String, Map<String, Object>> joinedMap,
            final String objectAName, final String objectBName) throws IllegalAccessException {
        for (final var keyEntry : e.getValue().entrySet()) {
            final Object b = objectB.getUsing(keyEntry.getKey(), objectBUsing);
            for (final var key : keyEntry.getValue()) {
                final Object a = objectA.getUsing(key, objectAUsing);
                final var valueMap = CHRONICLE_UTILS.objectToMap(a, objectAName);
                valueMap.putAll(CHRONICLE_UTILS.objectToMap(b, objectBName));
                joinedMap.put(keyEntry.getKey().toString() + key.toString(), valueMap);
            }
        }
    }

    /**
     * Join two objects together using a foreign key field that is indexed on
     * objectB and returns a new map containing fields of both objects
     * 
     * @param objectA         the first object
     * @param objectB         the second object
     * @param foreignKeyField the indexed foreign key
     * @throws SecurityException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     */
    public ConcurrentMap<String, Map<String, Object>> joinToMap(final ChronicleMap<Object, Object> objectA,
            final ChronicleMap<Object, Object> objectB, final Object objectAUsing, final Object objectBUsing,
            final String foreignKeyField, final String objectADataPath, final String objectAName,
            final String objectBName)
            throws NoSuchMethodException, SecurityException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException {
        final String indexPath = objectADataPath + "/indexes/" + foreignKeyField;

        if (!Files.exists(Paths.get(indexPath))) {
            Logger.error("Index is missing for the foreign key: {}.", foreignKeyField);
            return null;
        }

        final HTreeMap<String, Map<Object, List<Object>>> indexDb = MAP_DB.getDb(indexPath);
        final ConcurrentMap<String, Map<String, Object>> joinedMap = new ConcurrentHashMap<>();

        if (indexDb.keySet().size() > 3) {
            indexDb.entrySet().parallelStream().forEach(HandleConsumer.handleConsumerBuilder(e -> {
                loopJoinToMap(e, objectA, objectB, objectAUsing, objectBUsing, joinedMap, objectAName, objectBName);
            }));
            indexDb.close();
            return joinedMap;
        }

        for (final var e : indexDb.entrySet()) {
            loopJoinToMap(e, objectA, objectB, objectAUsing, objectBUsing, joinedMap, objectAName, objectBName);
        }
        indexDb.close();
        return joinedMap;
    }

    private void loopJoinToCsv(final Entry<String, Map<Object, List<Object>>> e,
            final ChronicleMap<Object, Object> objectA, final ChronicleMap<Object, Object> objectB,
            final Object objectAUsing, final Object objectBUsing, final List<Object[]> rowList)
            throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException {
        for (final var keyEntry : e.getValue().entrySet()) {
            final Object b = objectB.getUsing(keyEntry.getKey(), objectBUsing);
            final Method rowMethodB = b.getClass().getDeclaredMethod("row", Object.class);
            for (final var key : keyEntry.getValue()) {
                final Object a = objectA.getUsing(key, objectAUsing);
                final Method rowMethodA = a.getClass().getDeclaredMethod("row", Object.class);
                rowList.add((Object[]) rowMethodA.invoke(a, key));
            }
            rowList.add((Object[]) rowMethodB.invoke(b, keyEntry.getKey()));
        }
    }

    /**
     * Join two objects together using a foreign key field that is indexed on
     * objectB and returns a csvObject for table view
     * 
     * @param objectA         the first object
     * @param objectB         the second object
     * @param foreignKeyField the indexed foreign key
     * @throws SecurityException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     */
    public CsvObject joinToCsv(final ChronicleMap<Object, Object> objectA, final ChronicleMap<Object, Object> objectB,
            final Object objectAUsing, final Object objectBUsing, final String foreignKeyField,
            final String objectADataPath)
            throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException {
        final String indexPath = objectADataPath + "/indexes/" + foreignKeyField;

        if (!Files.exists(Paths.get(indexPath))) {
            Logger.error("Index is missing for the foreign key: {}.", foreignKeyField);
            return null;
        }

        final HTreeMap<String, Map<Object, List<Object>>> indexDb = MAP_DB.getDb(indexPath);
        final var valueA = objectA.values().toArray()[0];
        final var valueB = objectB.values().toArray()[0];
        final Method headersMethodA = valueA.getClass().getDeclaredMethod("headers");
        final Method headersMethodB = valueB.getClass().getDeclaredMethod("headers");
        final String[] headerListA = (String[]) headersMethodA.invoke(valueA);
        final String[] headerListB = (String[]) headersMethodB.invoke(valueB);
        final String[] headers = CHRONICLE_UTILS.copyArray(headerListA, headerListB);
        final List<Object[]> rowList = new ArrayList<>();

        if (indexDb.keySet().size() > 3) {
            indexDb.entrySet().parallelStream().forEach(HandleConsumer.handleConsumerBuilder(e -> {
                loopJoinToCsv(e, objectA, objectB, objectAUsing, objectBUsing, rowList);
            }));
            indexDb.close();
            return new CsvObject(headers, rowList);
        }

        for (final var e : indexDb.entrySet()) {
            loopJoinToCsv(e, objectA, objectB, objectAUsing, objectBUsing, rowList);
        }
        indexDb.close();
        return new CsvObject(headers, rowList);
    }
}
