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
import java.util.HashSet;
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

    private enum JoinMode {
        FIRST,
        PRIMARY,
        FOREIGN
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
            final Object primaryUsing, final Object foreignUsing, final String primaryObjectName,
            final String foreignObjectName, final ConcurrentMap<String, Map<String, Object>> joinedMap)
            throws IllegalAccessException {
        for (final var keyEntry : e.getValue().entrySet()) {
            if (keyEntry.getKey() != null) {
                final var primaryPrev = joinedMap.get(keyEntry.getKey().toString());

                for (final var key : keyEntry.getValue()) {
                    if (primaryPrev != null) {
                        final Object foreign = foreignObject.getUsing(key, foreignUsing);
                        final var foreignValue = CHRONICLE_UTILS.objectToMap(foreign, foreignObjectName);
                        primaryPrev.putAll(foreignValue);
                        joinedMap.put(key.toString(), primaryPrev);
                    } else {
                        final Object primary = primaryObject.getUsing(keyEntry.getKey(), primaryUsing);
                        final var primaryValue = CHRONICLE_UTILS.objectToMap(primary, primaryObjectName);
                        final var foreignPrev = joinedMap.get(key.toString());
                        if (foreignPrev != null) {
                            foreignPrev.putAll(primaryValue);
                            joinedMap.put(key.toString(), foreignPrev);
                        } else {
                            final Object foreign = foreignObject.getUsing(key, foreignUsing);
                            final var foreignValue = CHRONICLE_UTILS.objectToMap(foreign, foreignObjectName);
                            primaryValue.putAll(foreignValue);
                            joinedMap.put(key.toString(), primaryValue);
                        }
                    }
                }
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
        final var toRemove = new HashSet<>();

        for (final var join : joins) {
            if (!Files.exists(Paths.get(join.foreignKeyIndexPath))) {
                Logger.error("Index is missing for the foreign key: {}.", join.foreignKeyIndexPath);
                return null;
            }

            final HTreeMap<String, Map<Object, List<Object>>> indexDb = MAP_DB.getDb(join.foreignKeyIndexPath);

            if (indexDb.keySet().size() > 3)
                indexDb.entrySet().parallelStream().forEach(HandleConsumer.handleConsumerBuilder(e -> {
                    for (final var entry : join.primaryObject.entrySet()) {
                        loopJoinToMap(e, join.primaryObject.get(entry.getKey()),
                                join.foreignObject.get(e.getKey()),
                                join.primaryUsing, join.foreignUsing, join.primaryObjectName, join.foreignObjectName,
                                joinedMap);
                        toRemove.addAll(join.primaryObject.get(entry.getKey()).keySet());
                    }
                }));
            else
                for (final var e : indexDb.entrySet()) {
                    for (final var entry : join.primaryObject.entrySet()) {
                        loopJoinToMap(e, join.primaryObject.get(entry.getKey()),
                                join.foreignObject.get(e.getKey()),
                                join.primaryUsing, join.foreignUsing, join.primaryObjectName, join.foreignObjectName,
                                joinedMap);
                        toRemove.addAll(join.primaryObject.get(entry.getKey()).keySet());
                    }
                }
            indexDb.close();
        }

        joinedMap.keySet().removeAll(toRemove);
        return joinedMap;
    }

    private void loopJoinToCsv(final Entry<String, Map<Object, List<Object>>> e,
            final ChronicleMap<Object, Object> primaryObject, final ChronicleMap<Object, Object> foreignObject,
            final Object primaryUsing, final Object foreignUsing, final List<Object[]> rowList,
            final JoinMode joinMode)
            throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException {
        for (final var keyEntry : e.getValue().entrySet()) {
            if (keyEntry.getKey() != null) {
                final Object primary = primaryObject.getUsing(keyEntry.getKey(), primaryUsing);
                final Method primaryRowMethod = primary.getClass().getDeclaredMethod("row", Object.class);
                final var primaryRow = (Object[]) primaryRowMethod.invoke(primary, keyEntry.getKey());
                for (int i = 0; i < keyEntry.getValue().size(); i++) {
                    final Object foreign = foreignObject.getUsing(keyEntry.getValue().get(i), foreignUsing);
                    final Method foreignRowMethod = foreign.getClass().getDeclaredMethod("row", Object.class);
                    final var foreignRow = (Object[]) foreignRowMethod.invoke(foreign, keyEntry.getValue().get(i));
                    if (joinMode.equals(JoinMode.PRIMARY))
                        rowList.add(i, CHRONICLE_UTILS.copyArray(rowList.get(i), primaryRow));
                    else if (joinMode.equals(JoinMode.FOREIGN))
                        rowList.add(i, CHRONICLE_UTILS.copyArray(rowList.get(i), foreignRow));
                    else
                        rowList.add(CHRONICLE_UTILS.copyArray(primaryRow, foreignRow));
                }
            }
        }
    }

    private void addHeaders(final String[] objectHeaders, final String objectName, final List<String> headers) {
        for (final var h : objectHeaders) {
            final var name = objectName + "." + h;
            if (headers.indexOf(name) == -1) {
                headers.add(name);
            }
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
        final var headers = new ArrayList<String>();
        final var rowList = new ArrayList<Object[]>();
        final var joinedObjNames = new ArrayList<String>();

        for (final var join : joins) {
            if (!Files.exists(Paths.get(join.foreignKeyIndexPath))) {
                Logger.error("Index is missing for the foreign key: {}.", join.foreignKeyIndexPath);
                return null;
            }

            final HTreeMap<String, Map<Object, List<Object>>> indexDb = MAP_DB.getDb(join.foreignKeyIndexPath);
            final var primaryValue = join.primaryObject.values().stream().findFirst().get().values().stream()
                    .findFirst().get();
            final var foreignValue = join.foreignObject.values().stream().findFirst().get().values().stream()
                    .findFirst().get();
            final Method primaryHeaderMethod = primaryValue.getClass().getDeclaredMethod("header");
            final Method foreignHeaderMethod = foreignValue.getClass().getDeclaredMethod("header");
            final String[] headerListA = (String[]) primaryHeaderMethod.invoke(primaryValue);
            final String[] headerListB = (String[]) foreignHeaderMethod.invoke(foreignValue);
            addHeaders(headerListA, join.primaryObjectName, headers);
            addHeaders(headerListB, join.foreignObjectName, headers);
            final JoinMode mode = joinedObjNames.indexOf(join.primaryObjectName) == -1
                    && joinedObjNames.indexOf(join.foreignObjectName) == -1 ? JoinMode.FIRST
                            : joinedObjNames.indexOf(join.primaryObjectName) == -1 ? JoinMode.PRIMARY
                                    : JoinMode.FOREIGN;

            if (indexDb.keySet().size() > 3)
                indexDb.entrySet().parallelStream().forEach(HandleConsumer.handleConsumerBuilder(e -> {
                    for (final var entry : join.primaryObject.entrySet()) {
                        loopJoinToCsv(e, join.primaryObject.get(entry.getKey()), join.foreignObject.get(e.getKey()),
                                join.primaryUsing, join.foreignUsing, rowList, mode);
                    }
                }));
            else
                for (final var e : indexDb.entrySet()) {
                    for (final var entry : join.primaryObject.entrySet()) {
                        loopJoinToCsv(e, join.primaryObject.get(entry.getKey()), join.foreignObject.get(e.getKey()),
                                join.primaryUsing, join.foreignUsing, rowList, mode);
                    }
                }

            indexDb.close();
            joinedObjNames.add(join.primaryObjectName);
            joinedObjNames.add(join.foreignObjectName);
        }

        return new CsvObject(headers.toArray(new String[0]), rowList);
    }
}
