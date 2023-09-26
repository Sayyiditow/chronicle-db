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
import chronicle.db.entity.Join;
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
            final ChronicleMap<Object, Object> primaryObject, final ChronicleMap<Object, Object> foreignObject,
            final Object primaryUsing, final Object foreignUsing,
            final String primaryObjectName, final String foreignObjectName,
            final ConcurrentMap<String, Map<String, Object>> joinedMap) throws IllegalAccessException {
        for (final var keyEntry : e.getValue().entrySet()) {
            final Object primary = primaryObject.getUsing(keyEntry.getKey(), primaryUsing);
            for (final var key : keyEntry.getValue()) {
                final Object foreign = foreignObject.getUsing(key, foreignUsing);
                final var valueMap = CHRONICLE_UTILS.objectToMap(primary, primaryObjectName);
                valueMap.putAll(CHRONICLE_UTILS.objectToMap(foreign, foreignObjectName));
                joinedMap.putIfAbsent(keyEntry.getKey().toString() + key.toString(), valueMap);
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
    public ConcurrentMap<String, Map<String, Object>> joinToMap(final List<Join> joins)
            throws NoSuchMethodException, SecurityException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException {
        final ConcurrentMap<String, Map<String, Object>> joinedMap = new ConcurrentHashMap<>();

        for (final var join : joins) {
            if (!Files.exists(Paths.get(join.foreignKeyIndexPath))) {
                Logger.error("Index is missing for the foreign key: {}.", join.foreignKeyIndexPath);
                return null;
            }

            final HTreeMap<String, Map<Object, List<Object>>> indexDb = MAP_DB.getDb(join.foreignKeyIndexPath);

            if (indexDb.keySet().size() > 3)
                indexDb.entrySet().parallelStream().forEach(HandleConsumer.handleConsumerBuilder(e -> {
                    loopJoinToMap(e, join.primaryObject, join.foreignObject, join.primaryUsing, join.foreignUsing,
                            join.foreignObjectName, join.primaryObjectName, joinedMap);
                }));
            else
                for (final var e : indexDb.entrySet()) {
                    loopJoinToMap(e, join.primaryObject, join.foreignObject, join.primaryUsing, join.foreignUsing,
                            join.foreignObjectName, join.primaryObjectName, joinedMap);
                }
            indexDb.close();
        }

        return joinedMap;
    }

    private void loopJoinToCsv(final Entry<String, Map<Object, List<Object>>> e,
            final ChronicleMap<Object, Object> primaryObject, final ChronicleMap<Object, Object> foreignObject,
            final Object primaryUsing, final Object foreignUsing, final List<Object[]> rowList)
            throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException {
        for (final var keyEntry : e.getValue().entrySet()) {
            System.out.println(primaryObject.values().toArray()[0].toString());
            final Object primary = primaryObject.getUsing(keyEntry.getKey(), primaryUsing);
            final Method primaryRowMethod = primary.getClass().getDeclaredMethod("row", Object.class);
            final var primaryRow = (Object[]) primaryRowMethod.invoke(primary, keyEntry.getKey());
            for (final var key : keyEntry.getValue()) {
                final Object foreign = foreignObject.getUsing(key, foreignUsing);
                final Method foreignRowMethod = foreign.getClass().getDeclaredMethod("row", Object.class);
                final var foreignRow = (Object[]) foreignRowMethod.invoke(foreign, key);
                rowList.add(CHRONICLE_UTILS.copyArray(primaryRow, foreignRow));
            }
            if (rowList.size() > 1)
                return;
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
    public CsvObject joinToCsv(final List<Join> joins)
            throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException {
        String[] headers = new String[0];
        final List<Object[]> rowList = new ArrayList<>();

        for (final var join : joins) {
            if (!Files.exists(Paths.get(join.foreignKeyIndexPath))) {
                Logger.error("Index is missing for the foreign key: {}.", join.foreignKeyIndexPath);
                return null;
            }

            final HTreeMap<String, Map<Object, List<Object>>> indexDb = MAP_DB.getDb(join.foreignKeyIndexPath);
            final var primaryValue = join.primaryObject.values().toArray()[0];
            final var foreignValue = join.foreignObject.values().toArray()[0];
            final Method primaryHeaderMethod = primaryValue.getClass().getDeclaredMethod("header");
            final Method foreignHeaderMethod = foreignValue.getClass().getDeclaredMethod("header");
            final String[] headerListA = (String[]) primaryHeaderMethod.invoke(primaryValue);
            final String[] headerListB = (String[]) foreignHeaderMethod.invoke(foreignValue);
            headers = CHRONICLE_UTILS.copyArray(headerListA, headerListB);

            if (indexDb.keySet().size() > 3)
                indexDb.entrySet().parallelStream().forEach(HandleConsumer.handleConsumerBuilder(e -> {
                    loopJoinToCsv(e, join.primaryObject, join.foreignObject, join.primaryUsing, join.foreignUsing,
                            rowList);
                }));
            else
                for (final var e : indexDb.entrySet()) {
                    loopJoinToCsv(e, join.primaryObject, join.foreignObject, join.primaryUsing, join.foreignUsing,
                            rowList);
                }

            indexDb.close();
        }
        return new CsvObject(headers, rowList);
    }
}
