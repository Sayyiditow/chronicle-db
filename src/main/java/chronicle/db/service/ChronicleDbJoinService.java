package chronicle.db.service;

import static chronicle.db.dao.ChronicleUtils.CHRONICLE_UTILS;
import static chronicle.db.service.ChronicleDb.CHRONICLE_DB;
import static chronicle.db.service.MapDb.MAP_DB;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.mapdb.HTreeMap;
import org.tinylog.Logger;

import chronicle.db.dao.ChronicleDao;
import chronicle.db.entity.CsvObject;
import chronicle.db.entity.Join;
import chronicle.db.entity.JoinFilter;

@SuppressWarnings({ "unchecked", "rawtypes" })
public final class ChronicleDbJoinService {
    private ChronicleDbJoinService() {
    }

    public static final ChronicleDbJoinService CHRONICLE_DB_JOIN_SERVICE = new ChronicleDbJoinService();

    private void setIsEmpty(final Map<Object, Object> db, final Map<String, Map<String, Object>> mapOfObjects,
            final String daoClassName, final ChronicleDao dao) {
        if (db.size() == 0) {
            // add one object just to make sure the join works when no data is available
            mapOfObjects.get(daoClassName).put("isEmpty", true);
            db.put(dao.averageKey(), dao.averageValue());
        }
    }

    private Map<Object, Object> setRecordsFromFilter(final ChronicleDao dao, final JoinFilter filter,
            final Map<String, Map<String, Object>> mapOfObjects, final String daoClassName) throws IOException {
        Map<Object, Object> db = new HashMap<>();
        if (filter != null) {
            if (filter.key() != null) {
                db = new HashMap<>() {
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
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            }

            if (filter.subsetFields() != null && filter.subsetFields().length != 0) {
                if (db == null) {
                    db = dao.fetch();
                }
                db = dao.subsetOfValues(db, filter.subsetFields());
            }
            setIsEmpty(db, mapOfObjects, daoClassName, dao);
        } else {
            db = dao.fetch();
            setIsEmpty(db, mapOfObjects, daoClassName, dao);
        }

        return db;
    }

    private void setChronicleRecords(final String daoClassName, final String dataPath,
            final Map<String, Map<?, ?>> records, final String foreignKeyName,
            final Map<String, Map<String, Object>> mapOfObjects, final JoinFilter filter, final boolean isForeign)
            throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException, NoSuchFieldException,
            SecurityException, InstantiationException, InvocationTargetException, IOException {
        final var dao = CHRONICLE_DB.getChronicleDao(daoClassName, dataPath);

        if (mapOfObjects.get(daoClassName) == null) {
            mapOfObjects.put(daoClassName, new HashMap<>() {
                {
                    put("name", dao.name());
                }
            });
            records.put(daoClassName, setRecordsFromFilter(dao, filter, mapOfObjects, daoClassName));
        }

        if (isForeign) {
            final var indexPath = dao.getIndexPath(foreignKeyName);
            mapOfObjects.get(daoClassName).put("foreignKeyIndexPath", indexPath);

            if (!Files.exists(Path.of(indexPath))) {
                Logger.info("Index is missing for the foreign key: {}. Initilizing.", indexPath);
                dao.initIndex(new String[] { foreignKeyName });
            }
        }
    }

    private void setRequiredObjects(final Map<String, Map<?, ?>> records,
            final Map<String, Map<String, Object>> mapOfObjects, final Join join)
            throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException,
            NoSuchFieldException, SecurityException, InstantiationException, InvocationTargetException, IOException {
        setChronicleRecords(join.objDaoName(), join.objPath(), records,
                join.foreignKeyName(), mapOfObjects, join.objFilter(), false);
        setChronicleRecords(join.foreignKeyObjDaoName(), join.foreignKeyObjPath(), records,
                join.foreignKeyName(), mapOfObjects, join.foreignKeyObjFilter(), true);
    }

    private <K> void loopJoinToMap(final Map<Object, List<K>> indexDb,
            final Map<?, ?> object, final Map<?, ?> foreignKeyObject,
            final String objectName, final String foreignKeyObjName,
            final Map<Object, Map<String, Object>> joinedMap) throws IllegalAccessException {
        for (final var keyEntry : indexDb.entrySet()) {
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
    public <K> Map<Object, Map<String, Object>> joinToMap(final List<Join> joins)
            throws NoSuchMethodException, SecurityException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException, ClassNotFoundException, NoSuchFieldException,
            InstantiationException, IOException {
        final var joinedMap = new HashMap<Object, Map<String, Object>>();
        final var toRemove = new HashSet<>();
        final var mapOfRecords = new HashMap<String, Map<?, ?>>();
        final var mapOfObjects = new HashMap<String, Map<String, Object>>();

        for (final var join : joins) {
            setRequiredObjects(mapOfRecords, mapOfObjects, join);
            final var indexPath = mapOfObjects.get(join.foreignKeyObjDaoName()).get("foreignKeyIndexPath")
                    .toString();
            final var objRecords = mapOfRecords.get(join.objDaoName());
            final var foreignObjRecords = mapOfRecords.get(join.foreignKeyObjDaoName());
            boolean mapDbOpen = false;

            try {
                final HTreeMap<Object, List<K>> indexDb = MAP_DB.getDb(indexPath);
                mapDbOpen = true;
                loopJoinToMap(indexDb, objRecords, foreignObjRecords,
                        mapOfObjects.get(join.objDaoName()).get("name").toString(),
                        mapOfObjects.get(join.foreignKeyObjDaoName()).get("name").toString(), joinedMap);
                toRemove.addAll(objRecords.keySet());
            } catch (final IllegalAccessException e) {
                Logger.error("Error joining to map for {} and {}.", join.objDaoName(), join.foreignKeyObjDaoName());
            } finally {
                if (mapDbOpen)
                    MAP_DB.closeDb(indexPath);
            }
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

    private <K> void loopJoinToCsv(final Map<Object, List<K>> indexDb,
            final Map<?, ?> object, final Map<?, ?> foreignKeyObject,
            final List<Object[]> rowList, final Map<Object, Integer> indexMap,
            final int objSubsetLength, final int foreignKeyObjSubsetLength, final int headerSize,
            final boolean isInnerJoin, final boolean foreignIsMainObject, final boolean isObjectEmpty)
            throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, SecurityException {
        for (final var keyEntry : indexDb.entrySet()) {
            final var keyEntryKey = keyEntry.getKey();
            if (keyEntryKey != null) {
                final var objIndex = indexMap.get(keyEntryKey);
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
                        final var obj = object.get(keyEntryKey);
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

        if (!isInnerJoin && !foreignIsMainObject && !isObjectEmpty) {
            if (indexDb.size() != 0) {
                object.keySet().removeAll(indexDb.keySet());
            }
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
    public <K> CsvObject joinToCsv(final List<Join> joins)
            throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, ClassNotFoundException, NoSuchFieldException, InstantiationException,
            IOException {
        final var headers = new ArrayList<String>();
        final var rowList = new ArrayList<Object[]>();
        final Map<Object, Integer> indexMap = new HashMap<>();
        final var mapOfRecords = new HashMap<String, Map<?, ?>>();
        final var mapOfObjects = new HashMap<String, Map<String, Object>>();
        final var objectHeaderList = new ArrayList<String>();

        for (final var join : joins) {
            setRequiredObjects(mapOfRecords, mapOfObjects, join);
            final var indexPath = mapOfObjects.get(join.foreignKeyObjDaoName()).get("foreignKeyIndexPath")
                    .toString();
            final var objRecords = mapOfRecords.get(join.objDaoName());
            final var foreignKeyObjRecords = mapOfRecords.get(join.foreignKeyObjDaoName());
            final var objValue = objRecords.values().stream().findFirst().orElse(null);
            final var foreignKeyObjValue = foreignKeyObjRecords.values().stream().findFirst().orElse(null);
            if (objValue == null && foreignKeyObjValue == null) {
                Logger.info("Both objects for join are empty.");
                continue;
            }
            String[] objSubsetFields = new String[] {};
            String[] foreignKeyObjSubsetFields = new String[] {};
            final var objectName = mapOfObjects.get(join.objDaoName()).get("name").toString();
            final var foreignKeyObjName = mapOfObjects.get(join.foreignKeyObjDaoName()).get("name").toString();

            try {
                if (join.objFilter().subsetFields() != null)
                    objSubsetFields = join.objFilter().subsetFields();
            } catch (final NullPointerException e) {
                Logger.info("No subset fields set on object: {}.", objectName);
            }
            try {
                if (join.foreignKeyObjFilter().subsetFields() != null)
                    foreignKeyObjSubsetFields = join.foreignKeyObjFilter().subsetFields();
            } catch (final NullPointerException e) {
                Logger.info("No subset fields set on foreign key object: {}.", foreignKeyObjName);
            }
            final var objSubsetLength = objSubsetFields.length;
            final var foreignKeyObjSubsetLength = foreignKeyObjSubsetFields.length;
            final var objSubsetIsEmpty = objSubsetLength == 0;
            final var foreignKeyObjSubsetIsEmpty = foreignKeyObjSubsetLength == 0;
            final var isObjectEmpty = Boolean
                    .parseBoolean(String.valueOf(mapOfObjects.get(join.objDaoName()).get("isEmpty")));

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
            boolean mapDbOpen = false;

            try {
                final HTreeMap<Object, List<K>> indexDb = MAP_DB.getDb(indexPath);
                mapDbOpen = true;
                loopJoinToCsv(indexDb, objRecords, foreignKeyObjRecords, rowList, indexMap,
                        objSubsetLength, foreignKeyObjSubsetLength, headers.size(), join.isInnerJoin(),
                        join.foreignIsMainObject(), isObjectEmpty);
            } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
                Logger.error("Error joining to csv for {} and {}.", join.objDaoName(), join.foreignKeyObjDaoName());
            } finally {
                if (mapDbOpen)
                    MAP_DB.closeDb(indexPath);
            }
        }

        return new CsvObject(headers.toArray(new String[0]), rowList);
    }
}
