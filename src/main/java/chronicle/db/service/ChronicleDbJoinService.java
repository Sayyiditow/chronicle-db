package chronicle.db.service;

import static chronicle.db.dao.ChronicleUtils.CHRONICLE_UTILS;
import static chronicle.db.service.ChronicleDb.CHRONICLE_DB;
import static chronicle.db.service.MapDb.MAP_DB;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.mapdb.HTreeMap;
import org.tinylog.Logger;

import chronicle.db.dao.MultiChronicleDao;
import chronicle.db.dao.SingleChronicleDao;
import chronicle.db.entity.CsvObject;
import chronicle.db.entity.Join;
import chronicle.db.entity.JoinFilter;

@SuppressWarnings({ "unchecked", "rawtypes" })
public final class ChronicleDbJoinService {
    private ChronicleDbJoinService() {
    }

    public static final ChronicleDbJoinService CHRONICLE_DB_JOIN_SERVICE = new ChronicleDbJoinService();

    private void setRecordsFromFilter(final Map<String, ConcurrentMap<?, ?>> recordValueMap,
            final MultiChronicleDao dao, final JoinFilter filter, final String file)
            throws IOException, NoSuchFieldException, SecurityException {
        ConcurrentMap<?, ?> dbToMap = null;

        if (filter != null) {
            if (filter.key() != null) {
                dbToMap = new ConcurrentHashMap<>() {
                    {
                        {
                            put(filter.key(), dao.get(filter.key(), file));
                        }
                    }
                };
            } else if (filter.keys() != null) {
                dbToMap = dao.get(filter.keys());
            } else if (filter.search() != null) {
                final var db = dao.db(file);
                dbToMap = new ConcurrentHashMap<>(db);
                db.close();

                for (final var search : filter.search()) {
                    if (Files.exists(Path.of(dao.getIndexPath(search.field())))) {
                        if (filter.limit() == 0)
                            dbToMap = dao.indexedSearch(dbToMap, search);
                        else
                            dbToMap = dao.indexedSearch(dbToMap, search, filter.limit());
                    } else {
                        if (filter.limit() == 0)
                            dbToMap = dao.search(dbToMap, search);
                        else
                            dbToMap = dao.search(dbToMap, search, filter.limit());
                    }
                }
            } else if (filter.subsetFields() != null && filter.subsetFields().length != 0) {
                final var db = dao.db(file);
                dbToMap = new ConcurrentHashMap<>(db);
                db.close();
            } else {
                final var db = dao.db(file);
                dbToMap = new ConcurrentHashMap<>(db);
                db.close();
                if (filter.limit() != 0)
                    dbToMap = dbToMap.entrySet().stream().limit(filter.limit())
                            .collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));
                recordValueMap.put(file, dbToMap);
            }

            if (filter.subsetFields() != null && filter.subsetFields().length != 0) {
                if (dbToMap == null) {
                    final var db = dao.db(file);
                    dbToMap = new ConcurrentHashMap<>(db);
                    db.close();
                }
                dbToMap = dao.subsetOfValues(dbToMap, filter.subsetFields());
            }
            recordValueMap.put(file, dbToMap);
        } else {
            final var db = dao.db(file);
            dbToMap = new ConcurrentHashMap<>(db);
            db.close();
            recordValueMap.put(file, dbToMap);
        }

    }

    private void setRecordsFromFilter(final Map<String, ConcurrentMap<?, ?>> recordValueMap,
            final SingleChronicleDao dao, final JoinFilter filter) throws IOException {
        ConcurrentMap<?, ?> db = null;

        if (filter != null) {
            if (filter.key() != null) {
                db = new ConcurrentHashMap<>() {
                    {
                        {
                            put(filter.key(), dao.get(filter.key()));
                        }
                    }
                };
            } else if (filter.keys() != null) {
                db = dao.get(filter.keys());
            } else if (filter.search() != null) {
                db = dao.fetch();

                for (final var search : filter.search()) {
                    if (Files.exists(Path.of(dao.getIndexPath(search.field())))) {
                        if (filter.limit() == 0)
                            db = dao.indexedSearch(db, search);
                        else
                            db = dao.indexedSearch(db, search, filter.limit());
                    } else {
                        if (filter.limit() == 0)
                            db = dao.search(db, search);
                        else
                            db = dao.search(db, search, filter.limit());
                    }
                }
            } else {
                db = dao.fetch();

                if (filter.limit() != 0)
                    db = db.entrySet().stream().limit(filter.limit())
                            .collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));
            }

            if (filter.subsetFields() != null && filter.subsetFields().length != 0) {
                if (db == null) {
                    db = dao.fetch();
                }
                db = dao.subsetOfValues(db, filter.subsetFields());
            }
            recordValueMap.put("data", db);
        } else {
            db = dao.fetch();
            recordValueMap.put("data", db);
        }
    }

    private void setSingleChronicleRecords(final String daoClassName, final String dataPath,
            final Map<String, Map<String, ConcurrentMap<?, ?>>> records, final String foreignKeyName,
            final Map<String, Map<String, Object>> mapOfObjects, final JoinFilter filter, final boolean isForeign)
            throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException, NoSuchFieldException,
            SecurityException, InstantiationException, InvocationTargetException, IOException {
        final var dao = CHRONICLE_DB.getSingleChronicleDao(daoClassName, dataPath);
        mapOfObjects.put(daoClassName, new HashMap<>() {
            {
                put("name", dao.name());
            }
        });
        final var indexPath = dao.getIndexPath(foreignKeyName);

        if (isForeign) {
            mapOfObjects.get(daoClassName).put("foreignKeyIndexPath", indexPath);

            if (!Files.exists(Paths.get(indexPath))) {
                Logger.info("Index is missing for the foreign key: {}. Initilizing.", indexPath);
                dao.initIndex(new String[] { foreignKeyName });
            }
        }

        if (records.get(daoClassName) == null) {
            final var recordValueMap = new HashMap<String, ConcurrentMap<?, ?>>();
            setRecordsFromFilter(recordValueMap, dao, filter);
            records.put(daoClassName, recordValueMap);
        }

    }

    private void setMultiChronicleRecords(final String daoClassName, final String dataPath,
            final Map<String, Map<String, ConcurrentMap<?, ?>>> records, final String foreignKeyName,
            final Map<String, Map<String, Object>> mapOfObjects, final JoinFilter filter, final boolean isForeign)
            throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException, NoSuchFieldException,
            SecurityException, InstantiationException, InvocationTargetException, IOException {
        final var dao = CHRONICLE_DB.getMultiChronicleDao(daoClassName, dataPath);
        mapOfObjects.put(daoClassName, new HashMap<>() {
            {
                put("name", dao.name());
            }
        });
        final var indexPath = dao.getIndexPath(foreignKeyName);

        if (isForeign) {
            mapOfObjects.get(daoClassName).put("foreignKeyIndexPath", indexPath);

            if (!Files.exists(Paths.get(indexPath))) {
                Logger.info("Index is missing for the foreign key: {}. Initilizing.", indexPath);
                dao.initIndex(new String[] { foreignKeyName });
            }
        }

        if (records.get(daoClassName) == null) {
            final List<String> files = dao.getFiles();
            final var recordValueMap = new HashMap<String, ConcurrentMap<?, ?>>();

            for (final var file : files) {
                setRecordsFromFilter(recordValueMap, dao, filter, file);
            }
            records.put(daoClassName, recordValueMap);
        }
    }

    private void setRequiredObjects(final Map<String, Map<String, ConcurrentMap<?, ?>>> records,
            final Map<String, Map<String, Object>> mapOfObjects, final Join join)
            throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException,
            NoSuchFieldException, SecurityException, InstantiationException, InvocationTargetException, IOException {
        switch (join.joinObjMultiMode()) {
            case MAIN:
                setMultiChronicleRecords(join.objDaoName(), join.objPath(), records,
                        join.foreignKeyName(), mapOfObjects, join.objFilter(), false);
                setSingleChronicleRecords(join.foreignKeyObjDaoName(), join.foreignKeyObjPath(), records,
                        join.foreignKeyName(), mapOfObjects, join.foreignKeyObjFilter(), true);
                break;
            case FOREIGN_KEY_OBJ:
                setSingleChronicleRecords(join.objDaoName(), join.objPath(), records,
                        join.foreignKeyName(), mapOfObjects, join.objFilter(), false);
                setMultiChronicleRecords(join.foreignKeyObjDaoName(), join.foreignKeyObjPath(), records,
                        join.foreignKeyName(), mapOfObjects, join.foreignKeyObjFilter(), true);
                break;
            case NONE:
                setSingleChronicleRecords(join.objDaoName(), join.objPath(), records,
                        join.foreignKeyName(), mapOfObjects, join.objFilter(), false);
                setSingleChronicleRecords(join.foreignKeyObjDaoName(), join.foreignKeyObjPath(), records,
                        join.foreignKeyName(), mapOfObjects, join.foreignKeyObjFilter(), true);
                break;
            default:
                setMultiChronicleRecords(join.objDaoName(), join.objPath(), records,
                        join.foreignKeyName(), mapOfObjects, join.objFilter(), false);
                setMultiChronicleRecords(join.foreignKeyObjDaoName(), join.foreignKeyObjPath(), records,
                        join.foreignKeyName(), mapOfObjects, join.foreignKeyObjFilter(), true);
                break;
        }
    }

    private void loopJoinToMap(final Entry<String, Map<Object, List<Object>>> e,
            final ConcurrentMap<?, ?> object, final ConcurrentMap<?, ?> foreignKeyObject,
            final String objectName, final String foreignKeyObjName,
            final ConcurrentMap<Object, Map<String, Object>> joinedMap) throws IllegalAccessException {
        for (final var keyEntry : e.getValue().entrySet()) {
            if (keyEntry.getKey() != null) {
                final var objPrev = joinedMap.get(keyEntry.getKey());

                for (final var key : keyEntry.getValue()) {
                    if (objPrev != null) {
                        final Object foreignObj = foreignKeyObject.get(key);
                        final var foreignValue = CHRONICLE_UTILS.objectToMap(foreignObj, foreignKeyObjName, key);
                        final var objValue = new HashMap<>(objPrev);
                        objValue.putAll(foreignValue);
                        joinedMap.put(key, objValue);
                    } else {
                        final Object obj = object.get(keyEntry.getKey());
                        final var objValue = CHRONICLE_UTILS.objectToMap(obj, objectName,
                                keyEntry.getKey());
                        final var foreignObjPrev = joinedMap.get(key);
                        if (foreignObjPrev != null) {
                            foreignObjPrev.putAll(objValue);
                            joinedMap.put(key, foreignObjPrev);
                        } else {
                            final Object foreignObj = foreignKeyObject.get(key);
                            final var foreignValue = CHRONICLE_UTILS.objectToMap(foreignObj, foreignKeyObjName, key);
                            objValue.putAll(foreignValue);
                            joinedMap.put(key, objValue);
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
        final var mapOfRecords = new HashMap<String, Map<String, ConcurrentMap<?, ?>>>();
        final var mapOfObjects = new HashMap<String, Map<String, Object>>();

        for (final var join : joins) {
            setRequiredObjects(mapOfRecords, mapOfObjects, join);
            final HTreeMap<String, Map<Object, List<Object>>> indexDb = MAP_DB
                    .getDb(mapOfObjects.get(join.foreignKeyObjDaoName()).get("foreignKeyIndexPath")
                            .toString());

            final var objRecords = mapOfRecords.get(join.objDaoName());
            final var foreignObjRecords = mapOfRecords.get(join.foreignKeyObjDaoName());

            if (indexDb.keySet().size() > 3) {
                indexDb.entrySet().parallelStream().forEach(HandleConsumer.handleConsumerBuilder(e -> {
                    for (final var entry : objRecords.entrySet()) {
                        loopJoinToMap(e, objRecords.get(entry.getKey()),
                                foreignObjRecords.get(e.getKey()),
                                mapOfObjects.get(join.objDaoName()).get("name").toString(),
                                mapOfObjects.get(join.foreignKeyObjDaoName()).get("name").toString(), joinedMap);
                        toRemove.addAll(objRecords.get(entry.getKey()).keySet());
                    }
                }));
            } else
                for (final var e : indexDb.entrySet()) {
                    for (final var entry : objRecords.entrySet()) {
                        loopJoinToMap(e, objRecords.get(entry.getKey()), foreignObjRecords.get(e.getKey()),
                                mapOfObjects.get(join.objDaoName()).get("name").toString(),
                                mapOfObjects.get(join.foreignKeyObjDaoName()).get("name").toString(), joinedMap);
                        toRemove.addAll(objRecords.get(entry.getKey()).keySet());
                    }
                }
            indexDb.close();
        }

        joinedMap.keySet().removeAll(toRemove);
        return joinedMap;
    }

    private Object[] createEmptyObject(final int length) {
        final var obj = new Object[length];

        for (int i = 0; i < length; i++) {
            obj[i] = null;
        }

        return obj;
    }

    private void loopJoinToCsv(final Entry<String, Map<Object, List<Object>>> e,
            final ConcurrentMap<?, ?> object, final ConcurrentMap<?, ?> foreignKeyObject,
            final List<Object[]> rowList, final ConcurrentMap<Object, Integer> indexMap,
            final int objSubsetLength, final int foreignKeyObjSubsetLength, final int headerSize)
            throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException {
        for (final var keyEntry : e.getValue().entrySet()) {
            final var objIndex = indexMap.get(keyEntry.getKey());
            final var objPrev = objIndex != null ? rowList.get(objIndex) : null;
            final var multiForeignValues = new Object[1];

            if (keyEntry.getKey() != null) {
                for (final var key : keyEntry.getValue()) {
                    if (objPrev != null) {
                        final Object foreignObj = foreignKeyObject.get(key);
                        final var foreignObjIsNull = foreignObj == null;

                        final var foreignObjRow = foreignKeyObjSubsetLength == 0 ? !foreignObjIsNull
                                ? (Object[]) foreignObj.getClass().getDeclaredMethod("row", Object.class)
                                        .invoke(foreignObj, key)
                                : createEmptyObject(foreignKeyObject.values().toArray()[0].getClass()
                                        .getDeclaredFields().length)
                                : !foreignObjIsNull ? ((LinkedHashMap) foreignObj).values().toArray()
                                        : createEmptyObject(foreignKeyObjSubsetLength);

                        if (multiForeignValues[0] == null) {
                            multiForeignValues[0] = foreignObjRow;
                        } else {
                            multiForeignValues[0] = CHRONICLE_UTILS.copyArray(multiForeignValues,
                                    foreignObjRow);
                        }
                        rowList.set(objIndex,
                                CHRONICLE_UTILS.copyArray(objPrev, multiForeignValues));
                        indexMap.put(key, objIndex);
                    } else {
                        final var foreignIndex = indexMap.get(key);
                        final var foreignObjPrev = foreignIndex != null ? rowList.get(foreignIndex) : null;
                        final Object obj = object.get(keyEntry.getKey());
                        final var objIsNull = obj == null;

                        final var objRow = objSubsetLength == 0
                                ? !objIsNull ? (Object[]) obj.getClass().getDeclaredMethod("row", Object.class)
                                        .invoke(obj, key)
                                        : createEmptyObject(object.values().toArray()[0].getClass()
                                                .getDeclaredFields().length)
                                : !objIsNull ? ((LinkedHashMap) obj).values().toArray()
                                        : createEmptyObject(objSubsetLength);
                        if (foreignObjPrev != null) {
                            indexMap.put(key, foreignIndex);
                            rowList.set(foreignIndex, CHRONICLE_UTILS.copyArray(foreignObjPrev, objRow));
                        } else {
                            final Object foreignObj = foreignKeyObject.get(key);
                            final var foreignObjIsNull = foreignObj == null;
                            if (foreignObjIsNull)
                                continue;

                            final var foreignObjRow = foreignKeyObjSubsetLength == 0 ? !foreignObjIsNull
                                    ? (Object[]) foreignObj.getClass().getDeclaredMethod("row", Object.class)
                                            .invoke(foreignObj, key)
                                    : createEmptyObject(foreignKeyObject.values().toArray()[0].getClass()
                                            .getDeclaredFields().length)
                                    : !foreignObjIsNull ? ((LinkedHashMap) foreignObj).values().toArray()
                                            : createEmptyObject(foreignKeyObjSubsetLength);
                            rowList.add(CHRONICLE_UTILS.copyArray(objRow, foreignObjRow));
                            indexMap.put(key, rowList.size() - 1);
                        }
                    }
                }
            }
        }
    }

    private void addHeaders(final String[] objectHeaders, final String objectName, final List<String> headers,
            final boolean mainObj) {
        for (final var h : objectHeaders) {
            final var name = mainObj ? objectName + "." + h : h;
            if (headers.indexOf(h) == -1) {
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
        final var mapOfRecords = new HashMap<String, Map<String, ConcurrentMap<?, ?>>>();
        final var mapOfObjects = new HashMap<String, Map<String, Object>>();

        for (final var join : joins) {
            setRequiredObjects(mapOfRecords, mapOfObjects, join);
            final HTreeMap<String, Map<Object, List<Object>>> indexDb = MAP_DB
                    .getDb(mapOfObjects.get(join.foreignKeyObjDaoName()).get("foreignKeyIndexPath")
                            .toString());

            final var objRecords = mapOfRecords.get(join.objDaoName());
            final var foreignKeyObjRecords = mapOfRecords.get(join.foreignKeyObjDaoName());
            final var objValue = objRecords.values().stream().findFirst().get().values().stream().findFirst()
                    .orElseGet(() -> null);
            final var foreignKeyObjValue = foreignKeyObjRecords.values().stream().findFirst().get().values().stream()
                    .findFirst()
                    .orElseGet(() -> null);
            if (objValue == null || foreignKeyObjValue == null) {
                System.out.println("Object returned null");
                continue;
            }
            String[] objSubsetFields = new String[] {};
            String[] foreignKeyObjSubsetFields = new String[] {};

            try {
                if (join.objFilter().subsetFields() != null)
                    objSubsetFields = join.objFilter().subsetFields();
            } catch (final NullPointerException e) {
                Logger.info("No subset fields set on object.");
            }
            try {
                if (join.foreignKeyObjFilter().subsetFields() != null)
                    foreignKeyObjSubsetFields = join.foreignKeyObjFilter().subsetFields();
            } catch (final NullPointerException e) {
                Logger.info("No subset fields set on foreign key object.");
            }
            final var objSubsetLength = objSubsetFields.length;
            final var foreignKeyObjSubsetLength = foreignKeyObjSubsetFields.length;
            final var objSubsetIsEmpty = objSubsetLength == 0;
            final var foreignKeyObjSubsetIsEmpty = foreignKeyObjSubsetLength == 0;

            final String[] headerListA = objSubsetIsEmpty
                    ? (String[]) objValue.getClass().getDeclaredMethod("header").invoke(objValue)
                    : objSubsetFields;
            final String[] headerListB = foreignKeyObjSubsetIsEmpty
                    ? (String[]) foreignKeyObjValue.getClass().getDeclaredMethod("header").invoke(foreignKeyObjValue)
                    : foreignKeyObjSubsetFields;
            addHeaders(headerListA, !objSubsetIsEmpty ? join.foreignKeyName()
                    : mapOfObjects.get(join.objDaoName()).get("name").toString(), headers, !objSubsetIsEmpty);
            addHeaders(headerListB, !foreignKeyObjSubsetIsEmpty ? join.foreignKeyName()
                    : mapOfObjects.get(join.foreignKeyObjDaoName()).get("name").toString(), headers,
                    !foreignKeyObjSubsetIsEmpty);

            if (indexDb.keySet().size() > 3) {
                indexDb.entrySet().parallelStream().forEach(HandleConsumer.handleConsumerBuilder(e -> {
                    for (final var entry : objRecords.entrySet()) {
                        loopJoinToCsv(e, objRecords.get(entry.getKey()), foreignKeyObjRecords.get(e.getKey()),
                                rowList, indexMap, objSubsetLength, foreignKeyObjSubsetLength, headers.size());
                    }
                }));
            } else
                for (final var e : indexDb.entrySet()) {
                    for (final var entry : objRecords.entrySet()) {
                        loopJoinToCsv(e, objRecords.get(entry.getKey()), foreignKeyObjRecords.get(e.getKey()),
                                rowList, indexMap, objSubsetLength, foreignKeyObjSubsetLength, headers.size());
                    }
                }
            indexDb.close();
        }

        return new CsvObject(headers.toArray(new String[0]), rowList);
    }
}
