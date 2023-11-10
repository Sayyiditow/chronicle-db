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
        if (filter != null) {
            if (filter.key() != null) {
                recordValueMap.put(file, new ConcurrentHashMap<>() {
                    {
                        {
                            put(filter.key(), dao.get(filter.key(), file));
                        }
                    }
                });
            } else if (filter.keys() != null) {
                recordValueMap.put(file, dao.get(filter.keys()));
            } else if (filter.search() != null) {
                final var db = dao.db(file);
                ConcurrentMap<?, ?> dbToMap = new ConcurrentHashMap<>(db);
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
                recordValueMap.put(file, dbToMap);
            } else if (filter.subsetFields() != null && filter.subsetFields().length != 0) {
                final var db = dao.db(file);
                final ConcurrentMap<?, ?> dbToMap = new ConcurrentHashMap<>(db);
                db.close();
                recordValueMap.put(file, dao.subsetOfValues(dbToMap, filter.subsetFields()));
            } else {
                final var db = dao.db(file);
                ConcurrentMap<?, ?> dbToMap = new ConcurrentHashMap<>(db);
                db.close();
                if (filter.limit() != 0)
                    dbToMap = dbToMap.entrySet().stream().limit(filter.limit())
                            .collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));
                recordValueMap.put(file, dbToMap);
            }
        } else {
            final var db = dao.db(file);
            final ConcurrentMap<?, ?> dbToMap = new ConcurrentHashMap<>(db);
            db.close();
            recordValueMap.put(file, dbToMap);
        }
    }

    private void setRecordsFromFilter(final Map<String, ConcurrentMap<?, ?>> recordValueMap,
            final SingleChronicleDao dao, final JoinFilter filter) throws IOException {
        if (filter != null) {
            if (filter.key() != null) {
                recordValueMap.put("data", new ConcurrentHashMap<>() {
                    {
                        {
                            put(filter.key(), dao.get(filter.key()));
                        }
                    }
                });
            } else if (filter.keys() != null) {
                recordValueMap.put("data", dao.get(filter.keys()));
            } else if (filter.search() != null) {
                ConcurrentMap<?, ?> db = dao.fetch();

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
                recordValueMap.put("data", db);
            } else if (filter.subsetFields() != null && filter.subsetFields().length != 0) {
                final ConcurrentMap<?, ?> db = dao.fetch();
                recordValueMap.put("data", dao.subsetOfValues(db, filter.subsetFields()));
            } else {
                ConcurrentMap<?, ?> db = dao.fetch();

                if (filter.limit() != 0)
                    db = db.entrySet().stream().limit(filter.limit())
                            .collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));
                recordValueMap.put("data", db);
            }
        } else
            recordValueMap.put("data", dao.fetch());
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
            case PRIMARY:
                setMultiChronicleRecords(join.primaryDaoClassName(), join.primaryPath(), records,
                        join.foreignKeyName(), mapOfObjects, join.primaryFilter(), false);
                setSingleChronicleRecords(join.foreignDaoClassName(), join.foreignPath(), records,
                        join.foreignKeyName(), mapOfObjects, join.foreignFilter(), true);
                break;
            case FOREIGN:
                setSingleChronicleRecords(join.primaryDaoClassName(), join.primaryPath(), records,
                        join.foreignKeyName(), mapOfObjects, join.primaryFilter(), false);
                setMultiChronicleRecords(join.foreignDaoClassName(), join.foreignPath(), records,
                        join.foreignKeyName(), mapOfObjects, join.foreignFilter(), true);
                break;
            case NONE:
                setSingleChronicleRecords(join.primaryDaoClassName(), join.primaryPath(), records,
                        join.foreignKeyName(), mapOfObjects, join.primaryFilter(), false);
                setSingleChronicleRecords(join.foreignDaoClassName(), join.foreignPath(), records,
                        join.foreignKeyName(), mapOfObjects, join.foreignFilter(), true);
                break;
            default:
                setMultiChronicleRecords(join.primaryDaoClassName(), join.primaryPath(), records,
                        join.foreignKeyName(), mapOfObjects, join.primaryFilter(), false);
                setMultiChronicleRecords(join.foreignDaoClassName(), join.foreignPath(), records,
                        join.foreignKeyName(), mapOfObjects, join.foreignFilter(), true);
                break;
        }
    }

    private void loopJoinToMap(final Entry<String, Map<Object, List<Object>>> e,
            final ConcurrentMap<?, ?> primaryObject, final ConcurrentMap<?, ?> foreignObject,
            final String primaryObjectName, final String foreignObjectName,
            final ConcurrentMap<Object, Map<String, Object>> joinedMap)
            throws IllegalAccessException {
        for (final var keyEntry : e.getValue().entrySet()) {
            if (keyEntry.getKey() != null) {
                final var primaryPrev = joinedMap.get(keyEntry.getKey());

                for (final var key : keyEntry.getValue()) {
                    if (primaryPrev != null) {
                        final Object foreign = foreignObject.get(key);
                        final var foreignValue = CHRONICLE_UTILS.objectToMap(foreign, foreignObjectName, key);
                        final var primaryValue = new HashMap<>(primaryPrev);
                        primaryValue.putAll(foreignValue);
                        joinedMap.put(key, primaryValue);
                    } else {
                        final Object primary = primaryObject.get(keyEntry.getKey());
                        final var primaryValue = CHRONICLE_UTILS.objectToMap(primary, primaryObjectName,
                                keyEntry.getKey());
                        final var foreignPrev = joinedMap.get(key);
                        if (foreignPrev != null) {
                            foreignPrev.putAll(primaryValue);
                            joinedMap.put(key, foreignPrev);
                        } else {
                            final Object foreign = foreignObject.get(key);
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
        final var mapOfRecords = new HashMap<String, Map<String, ConcurrentMap<?, ?>>>();
        final var mapOfObjects = new HashMap<String, Map<String, Object>>();

        for (final var join : joins) {
            setRequiredObjects(mapOfRecords, mapOfObjects, join);
            final HTreeMap<String, Map<Object, List<Object>>> indexDb = MAP_DB
                    .getDb(mapOfObjects.get(join.foreignDaoClassName()).get("foreignKeyIndexPath")
                            .toString());

            if (indexDb.keySet().size() > 3) {
                indexDb.entrySet().parallelStream().forEach(HandleConsumer.handleConsumerBuilder(e -> {
                    final var primaryRecords = mapOfRecords.get(join.primaryDaoClassName());
                    final var foreignRecords = mapOfRecords.get(join.foreignDaoClassName());
                    for (final var entry : primaryRecords.entrySet()) {
                        loopJoinToMap(e, primaryRecords.get(entry.getKey()),
                                foreignRecords.get(e.getKey()),
                                mapOfObjects.get(join.primaryDaoClassName()).get("name").toString(),
                                mapOfObjects.get(join.foreignDaoClassName()).get("name").toString(), joinedMap);
                        toRemove.addAll(primaryRecords.get(entry.getKey()).keySet());
                    }
                }));
            } else
                for (final var e : indexDb.entrySet()) {
                    final var primaryRecords = mapOfRecords.get(join.primaryDaoClassName());
                    final var foreignRecords = mapOfRecords.get(join.foreignDaoClassName());
                    for (final var entry : primaryRecords.entrySet()) {
                        loopJoinToMap(e, primaryRecords.get(entry.getKey()), foreignRecords.get(e.getKey()),
                                mapOfObjects.get(join.primaryDaoClassName()).get("name").toString(),
                                mapOfObjects.get(join.foreignDaoClassName()).get("name").toString(), joinedMap);
                        toRemove.addAll(primaryRecords.get(entry.getKey()).keySet());
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
            final ConcurrentMap<?, ?> primaryObject, final ConcurrentMap<?, ?> foreignObject,
            final List<Object[]> rowList, final ConcurrentMap<Object, Integer> indexMap,
            final String[] primarySubsetFields, final String[] foreignSubsetFields)
            throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException {
        for (final var keyEntry : e.getValue().entrySet()) {
            if (keyEntry.getKey() != null) {
                final Integer primaryIndex = indexMap.get(keyEntry.getKey());
                final var primaryPrev = primaryIndex != null ? rowList.get(primaryIndex) : null;

                for (int i = 0; i < keyEntry.getValue().size(); i++) {
                    if (primaryPrev != null) {
                        final Object foreign = foreignObject.get(keyEntry.getValue().get(i));
                        final var foreignIsNull = foreign == null;

                        final var foreignRow = !foreignIsNull ? foreignSubsetFields.length == 0
                                ? (Object[]) foreign.getClass().getDeclaredMethod("row", Object.class)
                                        .invoke(foreign, keyEntry.getValue().get(i))
                                : createEmptyObject(foreignObject.values().toArray()[0].getClass()
                                        .getDeclaredFields().length)
                                : !foreignIsNull ? ((LinkedHashMap) foreign).values().toArray()
                                        : createEmptyObject(foreignSubsetFields.length);
                        rowList.set(primaryIndex, CHRONICLE_UTILS.copyArray(primaryPrev, foreignRow));
                        indexMap.put(keyEntry.getValue().get(i), primaryIndex);
                    } else {
                        final Integer foreignIndex = indexMap.get(keyEntry.getValue().get(i));
                        final var foreignPrev = foreignIndex != null ? rowList.get(foreignIndex) : null;
                        final Object primary = primaryObject.get(keyEntry.getKey());
                        final var primaryIsNull = primary == null;

                        final var primaryRow = primarySubsetFields.length == 0
                                ? !primaryIsNull ? (Object[]) primary.getClass().getDeclaredMethod("row", Object.class)
                                        .invoke(primary, keyEntry.getValue().get(i))
                                        : createEmptyObject(primaryObject.values().toArray()[0].getClass()
                                                .getDeclaredFields().length)
                                : !primaryIsNull ? ((LinkedHashMap) primary).values().toArray()
                                        : createEmptyObject(primarySubsetFields.length);
                        if (foreignPrev != null) {
                            indexMap.put(keyEntry.getValue().get(i), foreignIndex);
                            rowList.set(foreignIndex, CHRONICLE_UTILS.copyArray(foreignPrev, primaryRow));
                        } else {
                            final Object foreign = foreignObject.get(keyEntry.getValue().get(i));
                            final var foreignIsNull = foreign == null;

                            final var foreignRow = !foreignIsNull ? foreignSubsetFields.length == 0
                                    ? (Object[]) foreign.getClass().getDeclaredMethod("row", Object.class)
                                            .invoke(foreign, keyEntry.getValue().get(i))
                                    : createEmptyObject(foreignObject.values().toArray()[0].getClass()
                                            .getDeclaredFields().length)
                                    : !foreignIsNull ? ((LinkedHashMap) foreign).values().toArray()
                                            : createEmptyObject(foreignSubsetFields.length);
                            rowList.add(CHRONICLE_UTILS.copyArray(primaryRow, foreignRow));
                            indexMap.put(keyEntry.getValue().get(i), rowList.size() - 1);
                        }
                    }
                }
            }
        }
    }

    private void addHeaders(final String[] objectHeaders, final String objectName, final List<String> headers,
            final boolean isPrimary) {
        for (final var h : objectHeaders) {
            final var name = isPrimary ? objectName + "." + h : h;
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

        for (final var join : joins) {
            setRequiredObjects(mapOfRecords, mapOfObjects, join);
            final HTreeMap<String, Map<Object, List<Object>>> indexDb = MAP_DB
                    .getDb(mapOfObjects.get(join.foreignDaoClassName()).get("foreignKeyIndexPath")
                            .toString());

            final var primaryRecords = mapOfRecords.get(join.primaryDaoClassName());
            final var foreignRecords = mapOfRecords.get(join.foreignDaoClassName());
            final var primaryValue = primaryRecords.values().stream().findFirst().get().values().stream().findFirst()
                    .orElseGet(() -> null);
            final var foreignValue = foreignRecords.values().stream().findFirst().get().values().stream().findFirst()
                    .orElseGet(() -> null);
            if (primaryValue == null || foreignValue == null)
                continue;
            String[] primarySubsetFields = new String[] {};
            String[] foreignSubsetFields = new String[] {};

            try {
                primarySubsetFields = join.primaryFilter().subsetFields();
                foreignSubsetFields = join.foreignFilter().subsetFields();
            } catch (final NullPointerException e) {
                Logger.info("No subset fields set on either primary or foreign or both objects.");
            }

            final String[] headerListA = primarySubsetFields.length == 0
                    ? (String[]) primaryValue.getClass().getDeclaredMethod("header").invoke(primaryValue)
                    : primarySubsetFields;
            final String[] headerListB = foreignSubsetFields.length == 0
                    ? (String[]) foreignValue.getClass().getDeclaredMethod("header").invoke(foreignValue)
                    : foreignSubsetFields;
            addHeaders(headerListA, join.foreignKeyName(), headers, true);
            addHeaders(headerListB, mapOfObjects.get(join.foreignDaoClassName()).get("name").toString(), headers,
                    false);

            if (indexDb.keySet().size() > 3) {
                final var finalPrimarySubsetFields = primarySubsetFields;
                final var finalForeignSubsetFields = foreignSubsetFields;
                indexDb.entrySet().parallelStream().forEach(HandleConsumer.handleConsumerBuilder(e -> {
                    for (final var entry : primaryRecords.entrySet()) {
                        loopJoinToCsv(e, primaryRecords.get(entry.getKey()), foreignRecords.get(e.getKey()),
                                rowList, indexMap, finalPrimarySubsetFields, finalForeignSubsetFields);
                    }
                }));
            } else
                for (final var e : indexDb.entrySet()) {
                    for (final var entry : primaryRecords.entrySet()) {
                        loopJoinToCsv(e, primaryRecords.get(entry.getKey()), foreignRecords.get(e.getKey()),
                                rowList, indexMap, primarySubsetFields, foreignSubsetFields);
                    }
                }
            indexDb.close();
        }

        return new CsvObject(headers.toArray(new String[0]), rowList);
    }
}
