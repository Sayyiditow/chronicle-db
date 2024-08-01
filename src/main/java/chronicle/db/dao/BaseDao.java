package chronicle.db.dao;

import static chronicle.db.dao.ChronicleUtils.CHRONICLE_UTILS;
import static chronicle.db.dao.ChronicleUtils.getFileList;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.tinylog.Logger;

import com.jsoniter.spi.TypeLiteral;

import chronicle.db.entity.Search;
import chronicle.db.entity.Search.SearchType;

/**
 *
 * @param <K> Type of the unique identifier
 * @param <V> Type of the single element
 */
interface BaseDao<K, V> {
    /**
     * Name of db for logging purposes
     */
    default String name() {
        return averageValue().getClass().getSimpleName();
    }

    /**
     * Max entries per file for multiple mode, intiial size for single mode.
     */
    long entries();

    /**
     * The average key value
     */
    K averageKey();

    /**
     * The average key value
     */
    V averageValue();

    /**
     * Path to the directory where the files will reside
     */
    String dataPath();

    /**
     * Reusable value object
     */
    V using();

    /**
     * Typeliteral to be used when casting a json object into the required java
     * class
     */
    TypeLiteral<V> jsonType();

    void initIndex(final String[] fields) throws IOException;

    /**
     * The bloatFactor is used when the file contents can grow much more than the
     * average value,
     * defaults to 1
     */
    default double bloatFactor() {
        return 1;
    }

    /**
     * Create the folders required on init
     */
    default void createDataDirs() {
        if (!Files.exists(Path.of(dataPath()))) {
            try {
                Files.createDirectories(Path.of(dataPath() + "/" + "data"));
                Files.createDirectories(Path.of(dataPath() + "/" + "indexes"));
            } catch (final IOException e) {
                Logger.error(e.getMessage());
            }
        }
    }

    /**
     * Search the chronicle map based on values
     * 
     * @param db     the map
     * @param search object search
     * @return a map of the fitting values
     * @throws IOException
     */
    default ConcurrentMap<K, V> search(final ConcurrentMap<K, V> db, final Search search) throws IOException {
        Logger.info("Searching DB at {} for {}.", dataPath(), search);
        final ConcurrentMap<K, V> map = new ConcurrentHashMap<>();

        for (final var entry : db.entrySet()) {
            try {
                CHRONICLE_UTILS.search(search, entry.getKey(), entry.getValue(), map);
            } catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException e) {
                Logger.error("No such field: {} exists on searching {}. {}", search.field(), name(), e);
                break;
            }
        }

        return map;
    }

    /**
     * Search the chronicle map based on values
     * 
     * @param search object search
     * @return a map of the fitting values
     * @throws IOException
     */
    default ConcurrentMap<K, V> search(final ConcurrentMap<K, V> db, final Search search, final int limit)
            throws IOException {
        Logger.info("Searching DB at {} for {} with limit {}.", dataPath(), search, limit);
        final ConcurrentMap<K, V> map = new ConcurrentHashMap<>();

        for (final var entry : db.entrySet()) {
            try {
                CHRONICLE_UTILS.search(search, entry.getKey(), entry.getValue(), map);
            } catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException e) {
                Logger.error("No such field: {} exists on searching {}. {}", search.field(), name(), e);
                break;
            }
            if (map.size() == limit) {
                break;
            }
        }

        return map;
    }

    /**
     * If this database object contains indexes
     * 
     * @throws IOException
     */
    default boolean containsIndexes() throws IOException {
        return ChronicleUtils.getFileList(dataPath() + "/indexes/").size() > 0;
    }

    default List<String> indexFileNames() throws IOException {
        return ChronicleUtils.getFileList(dataPath() + "/indexes/");
    }

    /**
     * Delete and rerun all indexes. Faster when inserting a lot of records.
     * 
     * @throws IOException
     */
    default String[] deleteIndexes() throws IOException {
        final var available = indexFileNames();
        available.forEach(f -> {
            CHRONICLE_UTILS.deleteFileIfExists(dataPath() + "/indexes/" + f);
        });

        return available.toArray(new String[0]);
    }

    /**
     * Get the index map to use
     * 
     * @param field the field of the V value object
     * @return map of the index
     * @throws IOException
     */
    default String getIndexPath(final String field) {
        return dataPath() + "/indexes/" + field;
    }

    /**
     * Initialize indexes at dao creation
     * 
     * @param fields the fields required
     */
    default void initDefaultIndexes(final String[] fields) throws IOException {
        if (getFileList(dataPath() + "/data").size() != 0) {
            final var indexFiles = indexFileNames();
            if (indexFiles.size() != fields.length) {
                final var toIndex = new ArrayList<String>();

                for (final var field : fields) {
                    if (indexFiles.indexOf(field) == -1) {
                        toIndex.add(field);
                    }
                }

                if (toIndex.size() > 0) {
                    initIndex(toIndex.toArray(new String[0]));
                }
            }
        }
    }

    private void addSearchedValues(final List<K> keys, final ConcurrentMap<K, V> db, final ConcurrentMap<K, V> match) {
        if (keys != null)
            for (final var key : keys) {
                final var value = db.get(key);
                if (value != null)
                    match.put(key, value);
            }
    }

    private void addSearchedValues(final List<K> keys, final ConcurrentMap<K, V> db, final ConcurrentMap<K, V> match,
            final int limit) {
        if (keys != null)
            for (final var key : keys) {
                final var value = db.get(key);
                if (value != null)
                    match.put(key, value);

                if (match.size() == limit) {
                    break;
                }
            }
    }

    /**
     * Searches the objects using an index, without needed to loop over every record
     * Only useful for @code SearchType.EQUAL and @code SearchType.NOT_EQUAL
     * 
     * @param search the Search object
     * @param key    the key of the map
     * @param db     the db to search
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    default ConcurrentMap<K, V> indexedSearch(final Search search, final ConcurrentMap<K, V> db,
            final Map<Object, List<K>> index) throws IOException {
        Logger.info("Index searching DB at {} for {}.", dataPath(), search);
        final var match = new ConcurrentHashMap<K, V>();
        if (index != null) {
            final var keys = new ArrayList<K>();
            final var keySet = index.keySet();
            List<Object> searchTermList = new ArrayList<>();

            if (keySet.size() > 0) {
                final var fieldClass = keySet.stream().filter(Objects::nonNull).findFirst().get().getClass();
                final Object searchTerm = CHRONICLE_UTILS.setSearchTerm(search.searchTerm(), fieldClass);
                if (List.of(SearchType.IN, SearchType.NOT_IN).indexOf(search.searchType()) != -1) {
                    searchTermList = CHRONICLE_UTILS.setSearchTerm((List<Object>) search.searchTerm(), fieldClass);
                }

                switch (search.searchType()) {
                    case EQUAL:
                        addSearchedValues(index.get(searchTerm), db, match);
                        break;
                    case NOT_EQUAL:
                        index.keySet().remove(searchTerm);
                        for (final var list : index.entrySet()) {
                            addSearchedValues(list.getValue(), db, match);
                        }
                        break;
                    case LESS:
                        for (final var entry : index.entrySet()) {
                            if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) < 0)
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case GREATER:
                        for (final var entry : index.entrySet()) {
                            if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) > 0)
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case LESS_OR_EQUAL:
                        for (final var entry : index.entrySet()) {
                            if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) <= 0)
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case GREATER_OR_EQUAL:
                        for (final var entry : index.entrySet()) {
                            if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) >= 0)
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case LIKE:
                        for (final var entry : index.entrySet()) {
                            if (CHRONICLE_UTILS.containsIgnoreCase(entry.getKey(), searchTerm))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case NOT_LIKE:
                        for (final var entry : index.entrySet()) {
                            if (!CHRONICLE_UTILS.containsIgnoreCase(entry.getKey(), searchTerm))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case CONTAINS:
                        for (final var entry : index.entrySet()) {
                            if (Collections.singleton(entry.getKey()).contains(searchTerm))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case NOT_CONTAINS:
                        for (final var entry : index.entrySet()) {
                            if (!Collections.singleton(entry.getKey()).contains(searchTerm))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case STARTS_WITH:
                        for (final var entry : index.entrySet()) {
                            if (String.valueOf(entry.getKey()).startsWith(String.valueOf(searchTerm)))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case ENDS_WITH:
                        for (final var entry : index.entrySet()) {
                            if (String.valueOf(entry.getKey()).endsWith(String.valueOf(searchTerm)))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case IN:
                        for (final var entry : index.entrySet()) {
                            if (searchTermList.contains(entry.getKey()))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case NOT_IN:
                        for (final var entry : index.entrySet()) {
                            if (!searchTermList.contains(entry.getKey()))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                }
            }
        }
        return match;
    }

    /**
     * Searches the objects using an index, without needed to loop over every record
     * Only useful for @code SearchType.EQUAL and @code SearchType.NOT_EQUAL
     * 
     * @param search the Search object
     * @param key    the key of the map
     * @param db     the db to search
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    default ConcurrentMap<K, V> indexedSearch(final Search search, final ConcurrentMap<K, V> db,
            final Map<Object, List<K>> index, final int limit) throws IOException {
        Logger.info("Index searching DB at {} for {} with limit {}.", dataPath(), search, limit);
        final var match = new ConcurrentHashMap<K, V>();
        if (index != null) {
            final var keys = new ArrayList<K>();
            final var keySet = index.keySet();
            List<Object> searchTermList = new ArrayList<>();

            if (keySet.size() > 0) {
                final var fieldClass = keySet.iterator().next().getClass();
                final Object searchTerm = CHRONICLE_UTILS.setSearchTerm(search.searchTerm(), fieldClass);
                if (List.of(SearchType.IN, SearchType.NOT_IN).indexOf(search.searchType()) != -1) {
                    searchTermList = CHRONICLE_UTILS.setSearchTerm((List<Object>) search.searchTerm(), fieldClass);
                }

                switch (search.searchType()) {
                    case EQUAL:
                        addSearchedValues(index.get(searchTerm), db, match, limit);
                        break;
                    case NOT_EQUAL:
                        index.keySet().remove(searchTerm);
                        for (final var list : index.entrySet()) {
                            addSearchedValues(list.getValue(), db, match, limit);
                        }
                        break;
                    case LESS:
                        for (final var entry : index.entrySet()) {
                            if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) < 0)
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match, limit);
                        break;
                    case GREATER:
                        for (final var entry : index.entrySet()) {
                            if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) > 0)
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match, limit);
                        break;
                    case LESS_OR_EQUAL:
                        for (final var entry : index.entrySet()) {
                            if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) <= 0)
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match, limit);
                        break;
                    case GREATER_OR_EQUAL:
                        for (final var entry : index.entrySet()) {
                            if (CHRONICLE_UTILS.compare(entry.getKey(), searchTerm) >= 0)
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match, limit);
                        break;
                    case LIKE:
                        for (final var entry : index.entrySet()) {
                            if (CHRONICLE_UTILS.containsIgnoreCase(entry.getKey(), searchTerm))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match, limit);
                        break;
                    case NOT_LIKE:
                        for (final var entry : index.entrySet()) {
                            if (!CHRONICLE_UTILS.containsIgnoreCase(entry.getKey(), searchTerm))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match, limit);
                        break;
                    case CONTAINS:
                        for (final var entry : index.entrySet()) {
                            if (Collections.singleton(entry.getKey()).contains(searchTerm))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match, limit);
                        break;
                    case NOT_CONTAINS:
                        for (final var entry : index.entrySet()) {
                            if (!Collections.singleton(entry.getKey()).contains(searchTerm))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match, limit);
                        break;
                    case STARTS_WITH:
                        for (final var entry : index.entrySet()) {
                            if (String.valueOf(entry.getKey()).startsWith(String.valueOf(searchTerm)))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match, limit);
                        break;
                    case ENDS_WITH:
                        for (final var entry : index.entrySet()) {
                            if (String.valueOf(entry.getKey()).endsWith(String.valueOf(searchTerm)))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match, limit);
                        break;
                    case IN:
                        for (final var entry : index.entrySet()) {
                            if (searchTermList.contains(entry.getKey()))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                    case NOT_IN:
                        for (final var entry : index.entrySet()) {
                            if (!searchTermList.contains(entry.getKey()))
                                keys.addAll(entry.getValue());
                        }
                        addSearchedValues(keys, db, match);
                        break;
                }
            }
        }
        return match;
    }

    private void subsetOfValues(final String[] fields, final Map.Entry<K, V> entry,
            final ConcurrentMap<K, LinkedHashMap<String, Object>> map, final String objectName) {
        Field field = null;
        final var valueMap = new LinkedHashMap<String, Object>();
        for (final var f : fields) {
            if (f.equals("id")) {
                valueMap.put(objectName + ".id", entry.getKey());
            } else {
                try {
                    field = entry.getValue().getClass().getField(f);
                    if (Objects.nonNull(field)) {
                        valueMap.put(f, field.get(entry.getValue()));
                    }
                } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException e) {
                    Logger.error("No such field: {} when making a subset of {}. {}", f, name(), e);
                }
            }
        }
        map.put(entry.getKey(), valueMap);
    }

    /**
     * Cases where the data being selected is a subset of the whole object
     * this will be used to return a map of key, map of required fields and the
     * values
     * 
     * @param initialMap the map containing the whole object fields
     * @param fields     the required fields
     */
    default ConcurrentMap<K, LinkedHashMap<String, Object>> subsetOfValues(final ConcurrentMap<K, V> initialMap,
            final String[] fields, final String objectName) {
        final var map = new ConcurrentHashMap<K, LinkedHashMap<String, Object>>();

        for (final var entry : initialMap.entrySet()) {
            subsetOfValues(fields, entry, map, objectName);
        }
        return map;
    }
}
