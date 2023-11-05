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
import java.util.HashMap;
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
import net.openhft.chronicle.map.ChronicleMapBuilder;

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
            final K averageKey, final V averageValue, final String filePath, final double maxBloatFactor)
            throws IOException {
        final File file = new File(filePath);
        final Class<K> keyClass = (Class<K>) averageKey.getClass();
        final Class<V> valueClass = (Class<V>) averageValue.getClass();

        if (file.exists()) {
            Logger.info("Fetching Chronicle DB at: {}", filePath);
            return ChronicleMapBuilder.of(keyClass, valueClass).maxBloatFactor(maxBloatFactor).createPersistedTo(file);
        }

        Logger.info("Creating Chronicle DB at: {}", filePath);
        return ChronicleMapBuilder.of(keyClass, valueClass).name(name).entries(entries).averageKey(averageKey)
                .averageValue(averageValue).maxBloatFactor(maxBloatFactor).createPersistedTo(file);
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
            preparedValues[i] = params[i].isEnum() ? CHRONICLE_UTILS.toEnum(params[i], values[i]) : values[i];
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

    private void setSingleChronicleRecords(final String daoClassName, final String dataPath,
            final Map<String, ChronicleMap<Object, Object>> records, final String foreignKeyName,
            final Map<String, Object> mapOfObjects)
            throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException, NoSuchFieldException,
            SecurityException, InstantiationException, InvocationTargetException, IOException {
        final var dao = CHRONICLE_DB.getSingleChronicleDao(daoClassName, dataPath);
        records.put("data", dao.db());

        mapOfObjects.put("foreignKeyIndexPath", dao.getIndexPath(foreignKeyName));
        mapOfObjects.put("using", dao.using());
        mapOfObjects.put("name", dao.name());
    }

    private void setMultiChronicleRecords(final String daoClassName, final String dataPath,
            final Map<String, ChronicleMap<Object, Object>> records, final String foreignKeyName,
            final Map<String, Object> mapOfObjects)
            throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException, NoSuchFieldException,
            SecurityException, InstantiationException, InvocationTargetException, IOException {
        final var dao = CHRONICLE_DB.getMultiChronicleDao(daoClassName,
                dataPath);
        final List<String> files = dao.getFiles();

        for (final var file : files) {
            records.put(file, dao.db(file));
        }

        mapOfObjects.put("foreignKeyIndexPath", dao.getIndexPath(foreignKeyName));
        mapOfObjects.put("using", dao.using());
        mapOfObjects.put("name", dao.name());
    }

    private void setRequiredObjects(final Map<String, ChronicleMap<Object, Object>> primaryRecords,
            final Map<String, ChronicleMap<Object, Object>> foreignRecords,
            final Map<String, Object> primaryMapOfObjects,
            final Map<String, Object> foreignMapOfObjects, final Join join)
            throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException,
            NoSuchFieldException, SecurityException, InstantiationException, InvocationTargetException, IOException {
        switch (join.joinObjMultiMode()) {
            case PRIMARY:
                setMultiChronicleRecords(join.primaryDaoClassName(), join.dataPath(), primaryRecords,
                        join.foreignKeyName(), primaryMapOfObjects);
                setSingleChronicleRecords(join.foreignDaoClassName(), join.dataPath(), foreignRecords,
                        join.foreignKeyName(), foreignMapOfObjects);
                break;
            case FOREIGN:
                setSingleChronicleRecords(join.primaryDaoClassName(), join.dataPath(), primaryRecords,
                        join.foreignKeyName(), primaryMapOfObjects);
                setMultiChronicleRecords(join.foreignDaoClassName(), join.dataPath(), foreignRecords,
                        join.foreignKeyName(), foreignMapOfObjects);
                break;
            case NONE:
                setSingleChronicleRecords(join.primaryDaoClassName(), join.dataPath(), primaryRecords,
                        join.foreignKeyName(), primaryMapOfObjects);
                setSingleChronicleRecords(join.foreignDaoClassName(), join.dataPath(), foreignRecords,
                        join.foreignKeyName(), foreignMapOfObjects);
                break;
            default:
                setMultiChronicleRecords(join.primaryDaoClassName(), join.dataPath(), primaryRecords,
                        join.foreignKeyName(), primaryMapOfObjects);
                setMultiChronicleRecords(join.foreignDaoClassName(), join.dataPath(), foreignRecords,
                        join.foreignKeyName(), foreignMapOfObjects);
                break;
        }
    }

    private void closeOpenDbs(final Map<String, ChronicleMap<Object, Object>> primaryRecords,
            final Map<String, ChronicleMap<Object, Object>> foreignRecords) {
        foreignRecords.forEach((k, v) -> {
            v.close();
        });
        primaryRecords.forEach((k, v) -> {
            v.close();
        });
    }

    private void loopJoinToMap(final Entry<String, Map<Object, List<Object>>> e,
            final ChronicleMap<Object, Object> primaryObject, final ChronicleMap<Object, Object> foreignObject,
            final Object primaryUsing, final Object foreignUsing, final String primaryObjectName,
            final String foreignObjectName, final ConcurrentMap<Object, Map<String, Object>> joinedMap)
            throws IllegalAccessException {
        for (final var keyEntry : e.getValue().entrySet()) {
            if (keyEntry.getKey() != null) {
                final var primaryPrev = joinedMap.get(keyEntry.getKey());

                for (final var key : keyEntry.getValue()) {
                    if (primaryPrev != null) {
                        final Object foreign = foreignObject.getUsing(key, foreignUsing);
                        final var foreignValue = CHRONICLE_UTILS.objectToMap(foreign, foreignObjectName, key);
                        final var primaryValue = new HashMap<>(primaryPrev);
                        primaryValue.putAll(foreignValue);
                        joinedMap.put(key, primaryValue);
                    } else {
                        final Object primary = primaryObject.getUsing(keyEntry.getKey(), primaryUsing);
                        final var primaryValue = CHRONICLE_UTILS.objectToMap(primary, primaryObjectName,
                                keyEntry.getKey());
                        final var foreignPrev = joinedMap.get(key);
                        if (foreignPrev != null) {
                            foreignPrev.putAll(primaryValue);
                            joinedMap.put(key, foreignPrev);
                        } else {
                            final Object foreign = foreignObject.getUsing(key, foreignUsing);
                            final var foreignValue = CHRONICLE_UTILS.objectToMap(foreign, foreignObjectName, key);
                            primaryValue.putAll(foreignValue);
                            joinedMap.put(key, primaryValue);
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
     * @throws IOException
     * @throws InstantiationException
     * @throws NoSuchFieldException
     * @throws ClassNotFoundException
     */
    public ConcurrentMap<Object, Map<String, Object>> joinToMap(final List<Join> joins)
            throws NoSuchMethodException, SecurityException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException, ClassNotFoundException, NoSuchFieldException,
            InstantiationException, IOException {
        final var joinedMap = new ConcurrentHashMap<Object, Map<String, Object>>();
        final var toRemove = new HashSet<>();

        for (final var join : joins) {
            final var primaryRecords = new HashMap<String, ChronicleMap<Object, Object>>();
            final var foreignRecords = new HashMap<String, ChronicleMap<Object, Object>>();
            final Map<String, Object> primaryMapOfObjects = new HashMap<String, Object>();
            final Map<String, Object> foreignMapOfObjects = new HashMap<String, Object>();

            setRequiredObjects(primaryRecords, foreignRecords, primaryMapOfObjects, foreignMapOfObjects, join);

            final var foreignKeyIndexPath = foreignMapOfObjects.get("foreignKeyIndexPath").toString();

            if (!Files.exists(Paths.get(foreignKeyIndexPath))) {
                Logger.error("Index is missing for the foreign key: {}.", foreignKeyIndexPath);
                return null;
            }

            final HTreeMap<String, Map<Object, List<Object>>> indexDb = MAP_DB.getDb(foreignKeyIndexPath);

            if (indexDb.keySet().size() > 3) {
                indexDb.entrySet().parallelStream().forEach(HandleConsumer.handleConsumerBuilder(e -> {
                    for (final var entry : primaryRecords.entrySet()) {
                        loopJoinToMap(e, primaryRecords.get(entry.getKey()),
                                foreignRecords.get(e.getKey()), primaryMapOfObjects.get("using"),
                                foreignMapOfObjects.get("using"), primaryMapOfObjects.get("name").toString(),
                                foreignMapOfObjects.get("name").toString(), joinedMap);
                        toRemove.addAll(primaryRecords.get(entry.getKey()).keySet());
                    }
                }));
            } else
                for (final var e : indexDb.entrySet()) {
                    for (final var entry : primaryRecords.entrySet()) {
                        loopJoinToMap(e, primaryRecords.get(entry.getKey()),
                                foreignRecords.get(e.getKey()), primaryMapOfObjects.get("using"),
                                foreignMapOfObjects.get("using"), primaryMapOfObjects.get("name").toString(),
                                foreignMapOfObjects.get("name").toString(), joinedMap);
                        toRemove.addAll(primaryRecords.get(entry.getKey()).keySet());
                    }
                }
            closeOpenDbs(primaryRecords, foreignRecords);
            indexDb.close();
        }

        joinedMap.keySet().removeAll(toRemove);
        return joinedMap;
    }

    private void loopJoinToCsv(final Entry<String, Map<Object, List<Object>>> e,
            final ChronicleMap<Object, Object> primaryObject, final ChronicleMap<Object, Object> foreignObject,
            final Object primaryUsing, final Object foreignUsing, final List<Object[]> rowList,
            final ConcurrentMap<Object, Integer> indexMap)
            throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException {
        for (final var keyEntry : e.getValue().entrySet()) {
            if (keyEntry.getKey() != null) {
                final Integer primaryIndex = indexMap.get(keyEntry.getKey());
                final var primaryPrev = primaryIndex != null ? rowList.get(primaryIndex) : null;

                for (int i = 0; i < keyEntry.getValue().size(); i++) {
                    if (primaryPrev != null) {
                        final Object foreign = foreignObject.getUsing(keyEntry.getValue().get(i), foreignUsing);
                        final var foreignRow = (Object[]) foreign.getClass().getDeclaredMethod("row", Object.class)
                                .invoke(foreign, keyEntry.getValue().get(i));
                        rowList.set(primaryIndex, CHRONICLE_UTILS.copyArray(primaryPrev, foreignRow));
                        indexMap.put(keyEntry.getValue().get(i), primaryIndex);
                    } else {
                        final Integer foreignIndex = indexMap.get(keyEntry.getValue().get(i));
                        final var foreignPrev = foreignIndex != null ? rowList.get(foreignIndex) : null;
                        final Object primary = primaryObject.getUsing(keyEntry.getKey(), primaryUsing);
                        final var primaryRow = (Object[]) primary.getClass().getDeclaredMethod("row", Object.class)
                                .invoke(primary, keyEntry.getKey());
                        if (foreignPrev != null) {
                            indexMap.put(keyEntry.getValue().get(i), foreignIndex);
                            rowList.set(foreignIndex, CHRONICLE_UTILS.copyArray(foreignPrev, primaryRow));
                        } else {
                            final Object foreign = foreignObject.getUsing(keyEntry.getValue().get(i), foreignUsing);
                            final var foreignRow = (Object[]) foreign.getClass().getDeclaredMethod("row", Object.class)
                                    .invoke(foreign, keyEntry.getValue().get(i));
                            rowList.add(CHRONICLE_UTILS.copyArray(primaryRow, foreignRow));
                            indexMap.put(keyEntry.getValue().get(i), rowList.size() - 1);
                        }
                    }
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
     * @throws IOException
     * @throws InstantiationException
     * @throws NoSuchFieldException
     * @throws ClassNotFoundException
     */
    public CsvObject joinToCsv(final List<Join> joins)
            throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, ClassNotFoundException, NoSuchFieldException, InstantiationException,
            IOException {
        final var headers = new ArrayList<String>();
        final var rowList = new ArrayList<Object[]>();
        final ConcurrentMap<Object, Integer> indexMap = new ConcurrentHashMap<>();

        for (final var join : joins) {
            final var primaryRecords = new HashMap<String, ChronicleMap<Object, Object>>();
            final var foreignRecords = new HashMap<String, ChronicleMap<Object, Object>>();
            final Map<String, Object> primaryMapOfObjects = new HashMap<String, Object>();
            final Map<String, Object> foreignMapOfObjects = new HashMap<String, Object>();

            setRequiredObjects(primaryRecords, foreignRecords, primaryMapOfObjects, foreignMapOfObjects, join);

            final var foreignKeyIndexPath = foreignMapOfObjects.get("foreignKeyIndexPath").toString();

            if (!Files.exists(Paths.get(foreignKeyIndexPath))) {
                Logger.error("Index is missing for the foreign key: {}.", foreignKeyIndexPath);
                return null;
            }

            final HTreeMap<String, Map<Object, List<Object>>> indexDb = MAP_DB.getDb(foreignKeyIndexPath);
            final var primaryValue = primaryRecords.values().stream().findFirst().get().values().stream()
                    .findFirst().get();
            final var foreignValue = foreignRecords.values().stream().findFirst().get().values().stream()
                    .findFirst().get();
            final Method primaryHeaderMethod = primaryValue.getClass().getDeclaredMethod("header");
            final Method foreignHeaderMethod = foreignValue.getClass().getDeclaredMethod("header");
            final String[] headerListA = (String[]) primaryHeaderMethod.invoke(primaryValue);
            final String[] headerListB = (String[]) foreignHeaderMethod.invoke(foreignValue);
            addHeaders(headerListA, primaryMapOfObjects.get("name").toString(), headers);
            addHeaders(headerListB, foreignMapOfObjects.get("name").toString(), headers);

            if (indexDb.keySet().size() > 3)
                indexDb.entrySet().parallelStream().forEach(HandleConsumer.handleConsumerBuilder(e -> {
                    for (final var entry : primaryRecords.entrySet()) {
                        loopJoinToCsv(e, primaryRecords.get(entry.getKey()), foreignRecords.get(e.getKey()),
                                primaryMapOfObjects.get("using"), foreignMapOfObjects.get("using"), rowList, indexMap);
                    }
                }));
            else
                for (final var e : indexDb.entrySet()) {
                    for (final var entry : primaryRecords.entrySet()) {
                        loopJoinToCsv(e, primaryRecords.get(entry.getKey()), foreignRecords.get(e.getKey()),
                                primaryMapOfObjects.get("using"), foreignMapOfObjects.get("using"), rowList, indexMap);
                    }
                }
            closeOpenDbs(primaryRecords, foreignRecords);
            indexDb.close();
        }

        return new CsvObject(headers.toArray(new String[0]), rowList);
    }
}
