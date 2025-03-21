package chronicle.db.service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.tinylog.Logger;

public final class MapDb {
    private MapDb() {
    }

    public static final MapDb MAP_DB = new MapDb();
    private static final ConcurrentHashMap<String, MapEntry> mapCache = new ConcurrentHashMap<>();

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
     * Opens a MapDB for reads and writes (or reads only if readOnly is true).
     * Returns an HTreeMap—call close(filePath) to release resources and unlock.
     * Uses mmap if supported for maximum performance.
     */
    @SuppressWarnings("unchecked")
    public <K, V> HTreeMap<K, V> getDb(final String filePath) {
        final var entry = mapCache.compute(filePath, (k, existingEntry) -> {
            if (existingEntry != null) {
                // Increment reference count for existing entry
                existingEntry.refCount.increment();
                return existingEntry;
            }

            // Create a new entry
            try {
                final var map = DBMaker.fileDB(filePath)
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
                Logger.error("MapDB initialization failed for {}.", filePath);
                Logger.error(e);
                return null;
            }
        });

        if (entry != null) {
            return (HTreeMap<K, V>) entry.map;
        }

        // returns null if any error occured, to prevent close() running when no
        // increment was done
        return null;
    }

    /**
     * Closes the MapDB instance for the given filePath when no longer in use.
     */
    public void close(final String filePath) {
        mapCache.computeIfPresent(filePath, (k, entry) -> {
            entry.refCount.decrement();
            if (entry.refCount.sum() == 0) {
                // If the reference count reaches 0, close the map and remove the entry
                if (!entry.map.isClosed()) {
                    entry.map.close();
                }
                return null; // Remove the entry from the map
            }
            return entry; // Otherwise, keep the entry
        });
    }
}