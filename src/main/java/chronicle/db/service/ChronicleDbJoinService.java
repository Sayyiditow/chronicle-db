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
            final MultiChronicleDao dao, final JoinFilter filter, final String file,
            final Map<String, Map<String, Object>> mapOfObjects, final String daoClassName)
            throws IOException, NoSuchFieldException, SecurityException {
        ConcurrentMap<Object, Object> dbToMap = null;

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
            }

            if (filter.subsetFields() != null && filter.subsetFields().length != 0) {
                if (dbToMap == null) {
                    final var db = dao.db(file);
                    dbToMap = new ConcurrentHashMap<>(db);
                    db.close();
                }
                if (dbToMap.size() == 0) {
                    // add one object just to make sure the join works when no data is available
                    mapOfObjects.get(daoClassName).put("isEmpty", true);
                    dbToMap.put(dao.averageKey(), dao.averageValue());
                }
                dbToMap = dao.subsetOfValues(dbToMap, filter.subsetFields(), dao.name());
            }
            recordValueMap.put(file, dbToMap);
        } else {
            final var db = dao.db(file);
            dbToMap = new ConcurrentHashMap<>(db);
            db.close();
            if (dbToMap.size() == 0) {
                // add one object just to make sure the join works when no data is available
                mapOfObjects.get(daoClassName).put("isEmpty", true);
                dbToMap.put(dao.averageKey(), dao.averageValue());
            }
            recordValueMap.put(file, dbToMap);
        }

    }

    private void setRecordsFromFilter(final Map<String, ConcurrentMap<?, ?>> recordValueMap,
            final SingleChronicleDao dao, final JoinFilter filter, final Map<String, Map<String, Object>> mapOfObjects,
            final String daoClassName) throws IOException {
        ConcurrentMap<Object, Object> db = null;

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
                if (db.size() == 0) {
                    // add one object just to make sure the join works when no data is available
                    mapOfObjects.get(daoClassName).put("isEmpty", true);
                    db.put(dao.averageKey(), dao.averageValue());
                }
                db = dao.subsetOfValues(db, filter.subsetFields(), dao.name());
            }
            if (db.size() == 0) {
                // add one object just to make sure the join works when no data is available
                db.put(dao.averageKey(), dao.averageValue());
            }
            recordValueMap.put("data", db);
        } else {
            db = dao.fetch();
            if (db.size() == 0) {
                // add one object just to make sure the join works when no data is available
                mapOfObjects.get(daoClassName).put("isEmpty", true);
                db.put(dao.averageKey(), dao.averageValue());
            }
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
            setRecordsFromFilter(recordValueMap, dao, filter, mapOfObjects, daoClassName);
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
                setRecordsFromFilter(recordValueMap, dao, filter, file, mapOfObjects, daoClassName);
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

    private Object[] getRow(final Object obj, final boolean objIsNull, final Object key, final int subsetLength,
            final int objFieldLength)
            throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, SecurityException {
        if (!objIsNull) {
            if (obj instanceof LinkedHashMap) {
                return ((LinkedHashMap) obj).values().toArray();
            } else {
                return (Object[]) obj.getClass().getDeclaredMethod("row", Object.class)
                        .invoke(obj, key);
            }
        } else {
            if (subsetLength == 0) {
                return createEmptyObject(objFieldLength);
            } else {
                return createEmptyObject(subsetLength);
            }
        }
    }

    private void loopJoinToCsv(final Entry<String, Map<Object, List<Object>>> e,
            final ConcurrentMap<?, ?> object, final ConcurrentMap<?, ?> foreignKeyObject,
            final List<Object[]> rowList, final ConcurrentMap<Object, Integer> indexMap,
            final int objSubsetLength, final int foreignKeyObjSubsetLength, final int headerSize,
            final boolean isInnerJoin, final boolean foreignIsMainObject, final boolean isObjectEmpty)
            throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException {
        for (final var keyEntry : e.getValue().entrySet()) {
            if (keyEntry.getKey() != null) {
                final var objIndex = indexMap.get(keyEntry.getKey());
                final var objPrev = objIndex != null ? rowList.get(objIndex) : null;

                for (final var key : keyEntry.getValue()) {
                    if (objPrev != null) {
                        final var foreignObj = foreignKeyObject.get(key);
                        final var foreignObjIsNull = foreignObj == null;

                        final var foreignObjRow = getRow(foreignObj, foreignObjIsNull, key, foreignKeyObjSubsetLength,
                                foreignKeyObject.values().toArray()[0].getClass().getDeclaredFields().length);

                        rowList.set(objIndex, CHRONICLE_UTILS.copyArray(objPrev, foreignObjRow));
                        indexMap.put(key, objIndex);
                    } else {
                        final var foreignIndex = indexMap.get(key);
                        final var foreignObjPrev = foreignIndex != null ? rowList.get(foreignIndex) : null;
                        final var obj = object.get(keyEntry.getKey());
                        final var objIsNull = obj == null;

                        if (objIsNull && isInnerJoin)
                            continue;
                        else if (objIsNull && !foreignIsMainObject)
                            continue;

                        final var objRow = getRow(obj, objIsNull, key, objSubsetLength,
                                object.values().toArray()[0].getClass().getDeclaredFields().length);

                        if (foreignObjPrev != null) {
                            indexMap.put(key, foreignIndex);
                            rowList.set(foreignIndex, CHRONICLE_UTILS.copyArray(foreignObjPrev, objRow));
                        } else {
                            final var foreignObj = foreignKeyObject.get(key);
                            final var foreignObjIsNull = foreignObj == null;

                            if (foreignObjIsNull && isInnerJoin)
                                continue;
                            else if (foreignObjIsNull && foreignIsMainObject)
                                continue;

                            final var foreignObjRow = getRow(foreignObj, foreignObjIsNull, key,
                                    foreignKeyObjSubsetLength,
                                    foreignKeyObject.values().toArray()[0].getClass().getDeclaredFields().length);

                            rowList.add(CHRONICLE_UTILS.copyArray(objRow, foreignObjRow));
                            indexMap.put(key, rowList.size() - 1);
                        }
                    }
                }
            }
        }

        if (!isInnerJoin && !foreignIsMainObject) {
            if (e.getValue().size() != 0) {
                object.keySet().removeAll(e.getValue().keySet());

                for (final var key : object.keySet()) {
                    final var value = object.get(key);
                    final var foreignObj = foreignKeyObject.values().toArray()[0];
                    final var foreignObjRow = foreignKeyObjSubsetLength == 0
                            ? createEmptyObject(foreignObj.getClass()
                                    .getDeclaredFields().length)
                            : createEmptyObject(foreignKeyObjSubsetLength);

                    final var objRow = objSubsetLength == 0
                            ? (Object[]) value.getClass().getDeclaredMethod("row", Object.class)
                                    .invoke(value, key)
                            : ((LinkedHashMap) value).values().toArray();
                    rowList.add(CHRONICLE_UTILS.copyArray(objRow, foreignObjRow));
                    indexMap.put(key, rowList.size() - 1);
                }
            } else {
                if (!isObjectEmpty) {
                    for (final var o : object.entrySet()) {
                        final var foreignObj = foreignKeyObject.values().toArray()[0];
                        final var foreignObjRow = foreignKeyObjSubsetLength == 0
                                ? createEmptyObject(foreignObj.getClass()
                                        .getDeclaredFields().length)
                                : createEmptyObject(foreignKeyObjSubsetLength);

                        final var objRow = objSubsetLength == 0
                                ? (Object[]) o.getValue().getClass().getDeclaredMethod("row", Object.class)
                                        .invoke(o.getValue(), o.getKey())
                                : ((LinkedHashMap) o.getValue()).values().toArray();
                        rowList.add(CHRONICLE_UTILS.copyArray(objRow, foreignObjRow));
                        indexMap.put(o.getKey(), rowList.size() - 1);
                    }
                }
            }
        }
    }

    private void addHeaders(final String[] objectHeaders, final String objectName, final List<String> headers,
            final boolean addPrefix, final String foreignKeyName, final boolean mainObj) {
        for (final var h : objectHeaders) {
            var name = addPrefix ? objectName + "." + foreignKeyName + "." + h : h;
            if (h.toLowerCase().equals("id"))
                name = mainObj ? objectName + "." + foreignKeyName + "." + h : objectName + "." + h;
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
        final var mapOfRecords = new HashMap<String, Map<String, ConcurrentMap<?, ?>>>();
        final var mapOfObjects = new HashMap<String, Map<String, Object>>();
        final var objectHeaderList = new ArrayList<String>();

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
            if (objValue == null && foreignKeyObjValue == null) {
                Logger.info("Both objects for join are empty.");
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
            final var objectName = mapOfObjects.get(join.objDaoName()).get("name").toString();
            final var foreignKeyObjName = mapOfObjects.get(join.foreignKeyObjDaoName()).get("name").toString();
            final var isObjectEmpty = !String.valueOf(mapOfObjects.get(join.objDaoName()).get("isEmpty"))
                    .equals("null");

            if (objectHeaderList.indexOf(objectName + join.foreignKeyName()) == -1) {
                final String[] headerListA = objSubsetIsEmpty
                        ? (String[]) objValue.getClass().getDeclaredMethod("header").invoke(objValue)
                        : objSubsetFields;
                addHeaders(headerListA, objectName, headers, !objSubsetIsEmpty, join.foreignKeyName(), true);
                objectHeaderList.add(objectName + join.foreignKeyName());
            }

            if (objectHeaderList.indexOf(foreignKeyObjName) == -1) {
                final String[] headerListB = foreignKeyObjSubsetIsEmpty
                        ? (String[]) foreignKeyObjValue.getClass().getDeclaredMethod("header")
                                .invoke(foreignKeyObjValue)
                        : foreignKeyObjSubsetFields;
                addHeaders(headerListB, foreignKeyObjName, headers, false, join.foreignKeyName(), false);
                objectHeaderList.add(foreignKeyObjName);
            }

            if (indexDb.keySet().size() > 3) {
                indexDb.entrySet().parallelStream().forEach(HandleConsumer.handleConsumerBuilder(e -> {
                    for (final var entry : objRecords.entrySet()) {
                        loopJoinToCsv(e, objRecords.get(entry.getKey()), foreignKeyObjRecords.get(e.getKey()),
                                rowList, indexMap, objSubsetLength, foreignKeyObjSubsetLength, headers.size(),
                                join.isInnerJoin(), join.foreignIsMainObject(), isObjectEmpty);
                    }
                }));
            } else
                for (final var e : indexDb.entrySet()) {
                    for (final var entry : objRecords.entrySet()) {
                        loopJoinToCsv(e, objRecords.get(entry.getKey()), foreignKeyObjRecords.get(e.getKey()),
                                rowList, indexMap, objSubsetLength, foreignKeyObjSubsetLength, headers.size(),
                                join.isInnerJoin(), join.foreignIsMainObject(), isObjectEmpty);
                    }
                }
            indexDb.close();
        }

        return new CsvObject(headers.toArray(new String[0]), rowList);
    }
}
