package chronicle.db.dao;

import static chronicle.db.dao.ChronicleUtils.CHRONICLE_UTILS;

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
        try {
            Files.createDirectories(Path.of(dataPath() + "/" + "data"));
            Files.createDirectories(Path.of(dataPath() + "/" + "indexes"));
        } catch (final IOException e) {
            Logger.error(e.getMessage());
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
        final ConcurrentMap<K, V> map = new ConcurrentHashMap<>();

        for (final var entry : db.entrySet()) {
            try {
                CHRONICLE_UTILS.search(search, entry.getKey(), entry.getValue(), map);
            } catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException e) {
                Logger.error("No such field: {} exists when searching. {}", search.field(), e);
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
        final ConcurrentMap<K, V> map = new ConcurrentHashMap<>();

        for (final var entry : db.entrySet()) {
            try {
                CHRONICLE_UTILS.search(search, entry.getKey(), entry.getValue(), map);
            } catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException e) {
                Logger.error("No such field: {} exists when searching. {}", search.field(), e);
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
     */
    default boolean containsIndexes() {
        return ChronicleUtils.getFileList(dataPath() + "/indexes/").size() > 0;
    }

    default List<String> indexFileNames() {
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
    default ConcurrentMap<K, V> indexedSearch(final Search search, final ConcurrentMap<K, V> db,
            final Map<Object, List<K>> index) throws IOException {
        final var match = new ConcurrentHashMap<K, V>();
        final var keys = new ArrayList<K>();

        switch (search.searchType()) {
            case EQUAL:
                addSearchedValues(index.get(search.searchTerm()), db, match);
                break;
            case NOT_EQUAL:
                index.keySet().remove(search.searchTerm());
                for (final var list : index.entrySet()) {
                    addSearchedValues(list.getValue(), db, match);
                }
                break;
            case LESS:
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.compare(entry.getKey(), search.searchTerm()) < 0)
                        keys.addAll(entry.getValue());
                }
                addSearchedValues(keys, db, match);
                break;
            case GREATER:
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.compare(entry.getKey(), search.searchTerm()) > 0)
                        keys.addAll(entry.getValue());
                }
                addSearchedValues(keys, db, match);
                break;
            case LESS_OR_EQUAL:
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.compare(entry.getKey(), search.searchTerm()) <= 0)
                        keys.addAll(entry.getValue());
                }
                addSearchedValues(keys, db, match);
                break;
            case GREATER_OR_EQUAL:
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.compare(entry.getKey(), search.searchTerm()) >= 0)
                        keys.addAll(entry.getValue());
                }
                addSearchedValues(keys, db, match);
                break;
            case LIKE:
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.containsIgnoreCase(entry.getKey(), search.searchTerm()))
                        keys.addAll(entry.getValue());
                }
                addSearchedValues(keys, db, match);
                break;
            case NOT_LIKE:
                for (final var entry : index.entrySet()) {
                    if (!CHRONICLE_UTILS.containsIgnoreCase(entry.getKey(), search.searchTerm()))
                        keys.addAll(entry.getValue());
                }
                addSearchedValues(keys, db, match);
                break;
            case CONTAINS:
                for (final var entry : index.entrySet()) {
                    if (Collections.singleton(entry.getKey()).contains(search.searchTerm()))
                        keys.addAll(entry.getValue());
                }
                addSearchedValues(keys, db, match);
                break;
            case NOT_CONTAINS:
                for (final var entry : index.entrySet()) {
                    if (!Collections.singleton(entry.getKey()).contains(search.searchTerm()))
                        keys.addAll(entry.getValue());
                }
                addSearchedValues(keys, db, match);
                break;
            case STARTS_WITH:
                for (final var entry : index.entrySet()) {
                    if (String.valueOf(entry.getKey()).startsWith(String.valueOf(search.searchTerm())))
                        keys.addAll(entry.getValue());
                }
                addSearchedValues(keys, db, match);
                break;
            case ENDS_WITH:
                for (final var entry : index.entrySet()) {
                    if (String.valueOf(entry.getKey()).endsWith(String.valueOf(search.searchTerm())))
                        keys.addAll(entry.getValue());
                }
                addSearchedValues(keys, db, match);
                break;
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
    default ConcurrentMap<K, V> indexedSearch(final Search search, final ConcurrentMap<K, V> db,
            final Map<Object, List<K>> index, final int limit) throws IOException {
        final var match = new ConcurrentHashMap<K, V>();
        final var keys = new ArrayList<K>();

        switch (search.searchType()) {
            case EQUAL:
                addSearchedValues(index.get(search.searchTerm()), db, match, limit);
                break;
            case NOT_EQUAL:
                index.keySet().remove(search.searchTerm());
                for (final var list : index.entrySet()) {
                    addSearchedValues(list.getValue(), db, match, limit);
                }
                break;
            case LESS:
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.compare(entry.getKey(), search.searchTerm()) < 0)
                        keys.addAll(entry.getValue());
                }
                addSearchedValues(keys, db, match, limit);
                break;
            case GREATER:
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.compare(entry.getKey(), search.searchTerm()) > 0)
                        keys.addAll(entry.getValue());
                }
                addSearchedValues(keys, db, match, limit);
                break;
            case LESS_OR_EQUAL:
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.compare(entry.getKey(), search.searchTerm()) <= 0)
                        keys.addAll(entry.getValue());
                }
                addSearchedValues(keys, db, match, limit);
                break;
            case GREATER_OR_EQUAL:
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.compare(entry.getKey(), search.searchTerm()) >= 0)
                        keys.addAll(entry.getValue());
                }
                addSearchedValues(keys, db, match, limit);
                break;
            case LIKE:
                for (final var entry : index.entrySet()) {
                    if (CHRONICLE_UTILS.containsIgnoreCase(entry.getKey(), search.searchTerm()))
                        keys.addAll(entry.getValue());
                }
                addSearchedValues(keys, db, match, limit);
                break;
            case NOT_LIKE:
                for (final var entry : index.entrySet()) {
                    if (!CHRONICLE_UTILS.containsIgnoreCase(entry.getKey(), search.searchTerm()))
                        keys.addAll(entry.getValue());
                }
                addSearchedValues(keys, db, match, limit);
                break;
            case CONTAINS:
                for (final var entry : index.entrySet()) {
                    if (Collections.singleton(entry.getKey()).contains(search.searchTerm()))
                        keys.addAll(entry.getValue());
                }
                addSearchedValues(keys, db, match, limit);
                break;
            case NOT_CONTAINS:
                for (final var entry : index.entrySet()) {
                    if (!Collections.singleton(entry.getKey()).contains(search.searchTerm()))
                        keys.addAll(entry.getValue());
                }
                addSearchedValues(keys, db, match, limit);
                break;
            case STARTS_WITH:
                for (final var entry : index.entrySet()) {
                    if (String.valueOf(entry.getKey()).startsWith(String.valueOf(search.searchTerm())))
                        keys.addAll(entry.getValue());
                }
                addSearchedValues(keys, db, match, limit);
                break;
            case ENDS_WITH:
                for (final var entry : index.entrySet()) {
                    if (String.valueOf(entry.getKey()).endsWith(String.valueOf(search.searchTerm())))
                        keys.addAll(entry.getValue());
                }
                addSearchedValues(keys, db, match, limit);
                break;
        }

        return match;
    }

    private void subsetOfValues(final String[] fields, final Map.Entry<K, V> entry,
            final ConcurrentMap<K, LinkedHashMap<String, Object>> map) {
        Field field = null;
        final var valueMap = new LinkedHashMap<String, Object>();
        for (final var f : fields) {
            try {
                field = entry.getValue().getClass().getField(f);
                if (Objects.nonNull(field)) {
                    valueMap.put(f, field.get(entry.getValue()));
                }
            } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException e) {
                Logger.error("No such field: {} when making a subset of {}. {}", f,
                        entry.getValue().getClass().getSimpleName(), e);
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
            final String[] fields) {
        final var map = new ConcurrentHashMap<K, LinkedHashMap<String, Object>>();

        for (final var entry : initialMap.entrySet()) {
            subsetOfValues(fields, entry, map);
        }
        return map;
    }
}
