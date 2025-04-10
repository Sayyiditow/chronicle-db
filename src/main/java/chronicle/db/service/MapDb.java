package chronicle.db.service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.tinylog.Logger;

@SuppressWarnings("unchecked")
public final class MapDb {
    private MapDb() {
    }

    public static final MapDb MAP_DB = new MapDb();
    private static final ConcurrentMap<String, MapEntry> mapCache = new ConcurrentHashMap<>();

    private static class MapEntry {
        final HTreeMap<?, ?> map;
        final LongAdder refCount;

        MapEntry(final HTreeMap<?, ?> map) {
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
    public <K, V> HTreeMap<K, V> open(final String filePath) {
        final var entry = mapCache.compute(filePath, (k, existingEntry) -> {
            if (existingEntry != null) {
                // Increment reference count for existing entry
                existingEntry.refCount.increment();
                return existingEntry;
            }

            // Create a new entry
            try {
                final var map = DBMaker.fileDB(filePath)
                        .allocateStartSize(20 * 1024 * 1024) // 20 MB initial size
                        .allocateIncrement(10 * 1024 * 1024) // Grow by 10 MB
                        .closeOnJvmShutdown()
                        .fileLockDisable()
                        .fileMmapEnableIfSupported()
                        .fileMmapPreclearDisable()
                        .cleanerHackEnable()
                        .make()
                        .hashMap("map")
                        .createOrOpen();
                return new MapEntry(map);
            } catch (final Exception e) {
                Logger.error("MapDB initialization failed for [{}]. {}", filePath, e);
                return null;
            }
        });

        if (entry != null) {
            return (HTreeMap<K, V>) entry.map;
        }

        return null;
    }

    /**
     * Closes the MapDB instance for the given filePath when no longer in use.
     * 
     * @param filePath filepath to close
     */
    public void close(final String filePath) {
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

    public <K, V> HTreeMap<K, V> getMemoryDirectDb() {
        return (HTreeMap<K, V>) DBMaker.memoryDirectDB()
                .allocateStartSize(20 * 1024 * 1024) // 20 MB initial size
                .allocateIncrement(10 * 1024 * 1024) // Grow by 10 MB
                .closeOnJvmShutdown()
                .make()
                .hashMap("memory-direct")
                .create();
    }
}