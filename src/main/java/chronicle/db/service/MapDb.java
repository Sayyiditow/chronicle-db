package chronicle.db.service;

import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerCompressionWrapper;
import org.tinylog.Logger;

public final class MapDb {
    private MapDb() {
    }

    public static final MapDb MAP_DB = new MapDb();
    private static final ConcurrentMap<String, MapEntry> mapCache = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, TreeEntry> treeCache = new ConcurrentHashMap<>();
    public static final int MAP_DB_SEGMENTS = Runtime.getRuntime().availableProcessors() * 2;
    public static final String INDEX_DELIMITER = "\u0001";
    public static final String NON_CHAR = "\uFFFF";
    public static final String ACII_0 = "\u0000";

    private static class TreeEntry {
        final DB db;
        final NavigableSet<String> index;
        final LongAdder refCount;

        TreeEntry(final DB db, final NavigableSet<String> index) {
            this.db = db;
            this.index = index;
            this.refCount = new LongAdder();
            this.refCount.increment(); // Start with a reference count of 1
        }
    }

    private static class MapEntry {
        final HTreeMap<String, String> map;
        final LongAdder refCount;

        MapEntry(final HTreeMap<String, String> map) {
            this.map = map;
            this.refCount = new LongAdder();
            this.refCount.increment(); // Start with a reference count of 1
        }
    }

    /**
     * Opens a shared MapDB instance. Call close(filePath) to release it.
     * Do not use try-with-resources as it will prematurely close the shared
     * instance.
     * 
     * @param filePath filepath to create
     * 
     * @return HTreeMap or null, if null do not run close()
     */
    public HTreeMap<String, String> openMap(final String filePath) {
        final var entry = mapCache.compute(filePath, (k, existingEntry) -> {
            if (existingEntry != null) {
                // Increment reference count for existing entry
                existingEntry.refCount.increment();
                return existingEntry;
            }

            // Create a new entry
            try {
                final var map = DBMaker.fileDB(filePath)
                        .allocateStartSize(128 * 1024 * 1024) // initial size
                        .allocateIncrement(48 * 1024 * 1024) // Grow by 48 MB
                        .closeOnJvmShutdown()
                        .fileLockDisable()
                        .fileMmapEnableIfSupported()
                        .fileMmapPreclearDisable()
                        .cleanerHackEnable()
                        .concurrencyScale(MAP_DB_SEGMENTS)
                        .make()
                        .hashMap("map")
                        .keySerializer(new SerializerCompressionWrapper<>(Serializer.STRING))
                        .valueSerializer(new SerializerCompressionWrapper<>(Serializer.STRING))
                        .createOrOpen();
                return new MapEntry((HTreeMap<String, String>) map);
            } catch (final Exception e) {
                Logger.error("MapDB initialization failed for [{}]. {}", filePath, e);
                return null;
            }
        });

        if (entry != null) {
            return entry.map;
        }

        return null;
    }

    /**
     * Closes the MapDB instance for the given filePath when no longer in use.
     * 
     * @param filePath filepath to close
     */
    public void closeMap(final String filePath) {
        mapCache.computeIfPresent(filePath, (k, entry) -> {
            entry.refCount.decrement();
            if (entry.refCount.sum() == 0) {
                // If the reference count reaches 0, close the map and remove the entry
                entry.map.close();
                return null; // Remove the entry from the map
            }
            return entry; // Otherwise, keep the entry
        });
    }

    /**
     * Opens a shared MapDB TreeSet instance. Call close(filePath) to release it.
     * Do not use try-with-resources as it will prematurely close the shared
     * instance.
     *
     * @param filePath filepath to create
     * @return NavigableSet or null, if null do not run close()
     */
    public NavigableSet<String> openIndex(final String filePath) {
        final var entry = treeCache.compute(filePath, (k, existingEntry) -> {
            if (existingEntry != null) {
                // Increment reference count for existing entry
                existingEntry.refCount.increment();
                return existingEntry;
            }

            // Create a new entry
            try {
                final var db = DBMaker.fileDB(filePath)
                        .allocateStartSize(64 * 1024 * 1024)
                        .allocateIncrement(32 * 1024 * 1024)
                        .closeOnJvmShutdown()
                        .fileLockDisable()
                        .fileMmapEnableIfSupported()
                        .fileMmapPreclearDisable()
                        .cleanerHackEnable()
                        .concurrencyScale(MAP_DB_SEGMENTS)
                        .make();
                final var tree = db.treeSet("index")
                        .serializer(new SerializerCompressionWrapper<>(Serializer.STRING))
                        .createOrOpen();
                return new TreeEntry(db, tree);
            } catch (final Exception e) {
                Logger.error("Index DB initialization failed for [{}]. {}", filePath, e);
                return null;
            }
        });

        if (entry != null) {
            return entry.index;
        }

        return null;
    }

    /**
     * Closes the MapDB instance for the given filePath when no longer in use.
     *
     * @param filePath filepath to close
     */
    public void closeIndex(final String filePath) {
        treeCache.computeIfPresent(filePath, (k, entry) -> {
            entry.refCount.decrement();
            if (entry.refCount.sum() == 0) {
                // If the reference count reaches 0, close the map and remove the entry
                entry.db.close();
                return null; // Remove the entry from the map
            }
            return entry; // Otherwise, keep the entry
        });
    }

    /**
     * Utility to create a composite key using INDEX_DELIMITER.
     *
     * @param value the value part (e.g., "NEW")
     * @param key   the key part (e.g., "123")
     * @return composite key (e.g., "NEW\u0001123")
     */
    public String createIndexKey(final String value, final String key) {
        return (value + INDEX_DELIMITER + key).intern();
    }

    /**
     * Utility to extract the key part from a composite key.
     *
     * @param compositeKey the composite key (e.g., "NEW\u0001123")
     * @return the key part (e.g., "123")
     */
    public String extractIndexKey(final String indexKey, final String value) {
        return indexKey.substring(value.length() + INDEX_DELIMITER.length());
    }

    public String extractIndexKey(final String indexKey) {
        return indexKey.substring(indexKey.indexOf(INDEX_DELIMITER) + INDEX_DELIMITER.length());
    }

    public String extractIndexValue(final String indexKey) {
        return indexKey.substring(0, indexKey.indexOf(INDEX_DELIMITER));
    }

    /**
     * Utility to extract the SortedSet using a value.
     *
     * @param index
     * @param value
     * @return SortedSet<String>
     */
    public SortedSet<String> getExactIndexSubset(final NavigableSet<String> index, final String value) {
        final String prefix = value + INDEX_DELIMITER;
        return index.subSet(prefix + Character.MIN_VALUE, prefix + Character.MAX_VALUE);
    }

    /**
     * Utility to extract the SortedSet using a value.
     *
     * @param index
     * @param value
     * @return SortedSet<String>
     */
    public NavigableSet<String> getKeysBeforeIndexSubset(final NavigableSet<String> index, final String value) {
        return index.subSet("", false, value + INDEX_DELIMITER, false);
    }

    /**
     * Utility to extract the SortedSet using a value.
     *
     * @param index
     * @param value
     * @return SortedSet<String>
     */
    public NavigableSet<String> getKeysAfterIndexSubset(final NavigableSet<String> index, final String value) {
        return index.subSet(value + INDEX_DELIMITER + ACII_0, true, NON_CHAR, false);
    }

    /**
     * Utility to extract keys less than the value.
     *
     * @param index NavigableSet containing composite keys (value\u0001key)
     * @param value The value to compare against
     * @return SortedSet of keys < value
     */
    public NavigableSet<String> getLessThanIndexSubset(final NavigableSet<String> index, final String value) {
        return index.headSet(value + INDEX_DELIMITER, false);
    }

    /**
     * Utility to extract keys less than or equal to the value.
     *
     * @param index NavigableSet containing composite keys (value\u0001key)
     * @param value The value to compare against
     * @return SortedSet of keys <= value
     */
    public NavigableSet<String> getLessThanOrEqualIndexSubset(final NavigableSet<String> index, final String value) {
        return index.headSet(value + INDEX_DELIMITER + NON_CHAR, true);
    }

    /**
     * Utility to extract keys greater than the value.
     *
     * @param index NavigableSet containing composite keys (value\u0001key)
     * @param value The value to compare against
     * @return SortedSet of keys > value
     */
    public NavigableSet<String> getGreaterThanIndexSubset(final NavigableSet<String> index, final String value) {
        return index.tailSet(value + INDEX_DELIMITER + NON_CHAR, false);
    }

    /**
     * Utility to extract keys greater than or equal to the value.
     *
     * @param index NavigableSet containing composite keys (value\u0001key)
     * @return SortedSet of keys >= value
     */
    public NavigableSet<String> getGreaterThanOrEqualIndexSubset(final NavigableSet<String> index, final String value) {
        return index.tailSet(value + INDEX_DELIMITER, true);
    }
}