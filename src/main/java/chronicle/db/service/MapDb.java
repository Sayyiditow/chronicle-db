package chronicle.db.service;

import static chronicle.db.utils.ChronicleUtils.CHRONICLE_UTILS;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.mapdb.DB;
import org.mapdb.DBException;
import org.mapdb.DBMaker;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.tinylog.Logger;

/**
 * Service for managing MapDB-based key mappings and secondary indexes for
 * ChronicleDao.
 * <p>
 * This singleton provides two main functionalities:
 * <ul>
 * <li><b>Key Mapping:</b> Maps primary keys to shard file paths (for file
 * rotation)</li>
 * <li><b>Secondary Indexes:</b> Maintains sorted indexes on entity fields for
 * fast lookups</li>
 * </ul>
 * </p>
 * <p>
 * <b>Index Structure:</b> Indexes use composite keys in the format
 * {@code [fieldValue][separator][primaryKey]}
 * where the separator is {@code 0x1F}. This allows efficient range queries and
 * prefix searches.
 * </p>
 * <p>
 * <b>Reference Counting:</b> Like ChronicleDb, this service uses reference
 * counting to safely
 * share MapDB instances across multiple callers. Always call {@code close()} on
 * returned
 * SharedKeyMap and SharedIndexSet instances.
 * </p>
 * <p>
 * <b>Search Operations:</b> Provides indexed search methods for various
 * comparison types:
 * EQUAL, LESS, GREATER, STARTS_WITH, IN, BETWEEN, etc.
 * </p>
 * <p>
 * Usage example:
 * 
 * <pre>{@code
 * // Open an index
 * SharedIndexSet idx = MAP_DB.openIndex("/path/to/index.db");
 * try {
 *     // Add an entry: field value "active" for key "user123"
 *     byte[] compositeKey = MAP_DB.createIndexKey("active", "user123");
 *     idx.index.add(compositeKey);
 *     idx.commit();
 * } finally {
 *     idx.close();
 * }
 * }</pre>
 * </p>
 */
public final class MapDb {
    public static final MapDb MAP_DB = new MapDb();
    private static final ConcurrentMap<String, SharedKeyMap> mapCache = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, SharedIndexSet> treeCache = new ConcurrentHashMap<>();
    private static final byte indexSep = 0x1F;
    private static final byte upperByte = (byte) 0xFF;

    private MapDb() {
    }

    public static record KeyMapValue(String primaryKey, String fileName) {
        // 1. Define the instance here (this is the "variable" MapDB is looking for)
        public static final Serializer<KeyMapValue> KEY_MAP_VALUE_SERIALIZER = new SerializerImpl();

        // 2. Keep the class as the implementation logic
        private static class SerializerImpl implements Serializer<KeyMapValue> {
            @Override
            public void serialize(final DataOutput2 out, final KeyMapValue value) throws IOException {
                out.writeUTF(value.primaryKey());
                out.writeUTF(value.fileName());
            }

            @Override
            public KeyMapValue deserialize(final DataInput2 in, final int available) throws IOException {
                return new KeyMapValue(in.readUTF(), in.readUTF());
            }
        }
    }

    /**
     * Wrapper for a shared MapDB HTreeMap instance with reference counting.
     * <p>
     * Used for key-to-file mappings in ChronicleDao's file rotation system.
     * The map stores primary keys and their corresponding shard file paths.
     * </p>
     */
    public static class SharedKeyMap implements AutoCloseable {
        /** The underlying MapDB HTreeMap */
        public final HTreeMap<byte[], KeyMapValue> map;

        private final AtomicInteger refCount;
        private final String filePath; // Track file path for cleanup

        SharedKeyMap(final HTreeMap<byte[], KeyMapValue> map, final String filePath) {
            this.map = map;
            this.filePath = filePath;
            this.refCount = new AtomicInteger(1);
        }

        /**
         * Increments the reference count when sharing this map.
         * 
         * @return This SharedKeyMap instance for chaining
         */
        SharedKeyMap retain() {
            refCount.incrementAndGet();
            return this;
        }

        /**
         * Decrements the reference count and closes the map if no references remain.
         */
        @Override
        public void close() {
            mapCache.computeIfPresent(filePath, (k, entry) -> {
                if (entry.refCount.decrementAndGet() == 0) {
                    entry.map.close();
                    return null;
                }
                return entry; // Keep the entry
            });
        }
    }

    /**
     * Wrapper for a shared MapDB NavigableSet (TreeSet) instance with reference
     * counting.
     * <p>
     * Used for secondary indexes in ChronicleDao. The index stores composite keys
     * in the format {@code [fieldValue][0x1F][primaryKey]} to enable efficient
     * range queries and prefix searches.
     * </p>
     * <p>
     * <b>Important:</b> Call {@link #commit()} after modifications to persist
     * changes.
     * </p>
     */
    public static class SharedIndexSet implements AutoCloseable {
        /** The underlying MapDB NavigableSet (sorted index) */
        public final NavigableSet<byte[]> index;

        private final DB db;
        private final AtomicInteger refCount;
        private final String filePath; // Track file path for cleanup

        SharedIndexSet(final DB db, final NavigableSet<byte[]> index, final String filePath) {
            this.db = db;
            this.index = index;
            this.filePath = filePath;
            this.refCount = new AtomicInteger(1); // Start with a reference count of 1
        }

        /**
         * Increments the reference count when sharing this index.
         * 
         * @return This SharedIndexSet instance for chaining
         */
        SharedIndexSet retain() {
            refCount.incrementAndGet();
            return this;
        }

        /**
         * Decrements the reference count and closes the index if no references remain.
         * <p>
         * Automatically commits pending changes before closing.
         * </p>
         */
        @Override
        public void close() {
            treeCache.computeIfPresent(filePath, (k, entry) -> {
                if (entry.refCount.decrementAndGet() == 0) {
                    entry.db.close();
                    return null;
                }
                return entry; // Keep the entry
            });
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
    public SharedKeyMap openMap(final String filePath) {
        final var entry = mapCache.compute(filePath, (k, existingEntry) -> {
            if (existingEntry != null) {
                return existingEntry.retain();
            }

            // Create a new entry
            try {
                final HTreeMap<byte[], KeyMapValue> map = DBMaker.fileDB(filePath)
                        .allocateStartSize(512 * 1024 * 1024)
                        .allocateIncrement(256 * 1024 * 1024)
                        .closeOnJvmShutdown()
                        .fileLockDisable()
                        .fileMmapEnableIfSupported()
                        .fileMmapPreclearDisable()
                        .cleanerHackEnable()
                        .make()
                        .hashMap("map")
                        .keySerializer(Serializer.BYTE_ARRAY)
                        .valueSerializer(KeyMapValue.KEY_MAP_VALUE_SERIALIZER)
                        .createOrOpen();

                return new SharedKeyMap(map, filePath);
            } catch (final DBException.DataCorruption | DBException.VolumeEOF | NegativeArraySizeException
                    | DBException.WrongFormat e) {
                CHRONICLE_UTILS.deleteFileIfExists(filePath); // let it reindex
                Logger.info("Reinitializing KeyMap at [{}]", filePath);
                throw new RuntimeException(e);
            } catch (final Exception e) {
                Logger.error("Failed to open KeyMap at [{}]", filePath);
                throw new RuntimeException(e);
            }
        });

        return entry;
    }

    /**
     * Use this only when jvm hangs on shutdown
     */
    public void closeMap(final String filePath) {
        mapCache.computeIfPresent(filePath, (k, mapEntry) -> {
            mapEntry.map.close();
            Logger.info("Closed KeyMap at [{}]", filePath);
            return null;
        });
    }

    /**
     * Use this only when jvm hangs on shutdown
     */
    public void closeAllMaps() {
        for (final var entry : mapCache.entrySet()) {
            final String filePath = entry.getKey();
            final SharedKeyMap mapEntry = entry.getValue();
            mapEntry.map.close();
            Logger.info("Closed KeyMap at [{}]", filePath);
        }
        mapCache.clear(); // Clear all cached entries
        Logger.info("All KeyMaps have been closed and mapCache cleared.");
    }

    /**
     * Opens a shared MapDB TreeSet instance. Call close(filePath) to release it.
     * Do not use try-with-resources as it will prematurely close the shared
     * instance.
     *
     * @param filePath filepath to create
     * @return NavigableSet or null, if null do not run close()
     */
    public SharedIndexSet openIndex(final String filePath) {
        final var entry = treeCache.compute(filePath, (k, existingEntry) -> {
            if (existingEntry != null) {
                return existingEntry.retain();
            }

            // Create a new entry
            try {
                final var db = DBMaker.fileDB(filePath)
                        .allocateStartSize(512 * 1024 * 1024)
                        .allocateIncrement(256 * 1024 * 1024)
                        .closeOnJvmShutdown()
                        .fileLockDisable()
                        .fileMmapEnableIfSupported()
                        .fileMmapPreclearDisable()
                        .cleanerHackEnable()
                        .make();
                final var tree = db.treeSet("index")
                        .serializer(Serializer.BYTE_ARRAY_DELTA)
                        .createOrOpen();

                return new SharedIndexSet(db, tree, filePath);
            } catch (final DBException.DataCorruption | DBException.VolumeEOF | NegativeArraySizeException
                    | DBException.WrongFormat e) {
                CHRONICLE_UTILS.deleteFileIfExists(filePath); // let it reindex
                Logger.error("Reinitializing Index at [{}]", filePath);
                throw new RuntimeException(e);
            } catch (final Exception e) {
                Logger.error("Failed to open Index at [{}]", filePath);
                throw new RuntimeException(e);
            }
        });

        return entry;
    }

    /**
     * Use this only when jvm hangs on shutdown
     */
    public void closeIndex(final String filePath) {
        treeCache.computeIfPresent(filePath, (k, treeEntry) -> {
            treeEntry.db.close();
            Logger.info("Closed Index at [{}]", filePath);
            return null;
        });
    }

    /**
     * Use this only when jvm hangs on shutdown
     */
    public void closeAllIndexes() {
        for (final var entry : treeCache.entrySet()) {
            final String filePath = entry.getKey();
            final var treeEntry = entry.getValue();
            treeEntry.db.close();
            Logger.info("Closed Index at [{}]", filePath);
        }
        treeCache.clear(); // Clear all cached entries
        Logger.info("All Indexes have been closed and treeCache cleared.");
    }

    private String sanitize(final Object input) {
        if (input == null)
            return "null";
        final String str = input.toString();
        return str.indexOf(indexSep) >= 0 ? str.replace((char) indexSep, ' ') : str;
    }

    public byte[] getSanitizedByte(final String value) {
        return sanitize(value).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] getSanitizedByte(final Object value) {
        return sanitize(value).getBytes(StandardCharsets.UTF_8);
    }

    public byte[] createIndexKey(final Object fieldValue, final byte[] keyBytes) {
        final byte[] fieldBytes = getSanitizedByte(fieldValue);
        final byte[] result = new byte[fieldBytes.length + 1 + keyBytes.length];
        System.arraycopy(fieldBytes, 0, result, 0, fieldBytes.length); // Copy 1st part
        result[fieldBytes.length] = indexSep; // Insert separator
        System.arraycopy(keyBytes, 0, result, fieldBytes.length + 1, keyBytes.length); // Copy 2nd part
        return result;
    }

    /**
     * Extracts the primary key hash from a composite index key.
     * The hash is always the last 16 bytes of the composite key.
     *
     * @param compositeKey The composite index key [fieldValue][0x1F][16-byte hash]
     * @return The 16-byte hash of the primary key
     */
    public byte[] extractIndexKey(final byte[] compositeKey) {
        // O(1) extraction - always last 16 bytes
        final byte[] hash = new byte[16];
        System.arraycopy(compositeKey, compositeKey.length - 16, hash, 0, 16);
        return hash;
    }

    public String extractIndexValue(final byte[] compositeKey) {
        for (int i = 0; i < compositeKey.length; i++) {
            if (compositeKey[i] == indexSep) {
                return new String(compositeKey, 0, i, StandardCharsets.UTF_8);
            }
        }
        return new String(compositeKey, StandardCharsets.UTF_8); // fallback: no separator found
    }

    /**
     * Extracts both the field value and primary key hash from a composite index key.
     *
     * @param compositeKey The composite index key [fieldValue][0x1F][16-byte hash]
     * @return Object array where [0] is the field value (String) and [1] is the 16-byte hash (byte[])
     */
    public Object[] extractIndexValueAndKey(final byte[] compositeKey) {
        final var result = new Object[2];
        // Value is everything before last 17 bytes (separator + 16-byte hash)
        final int valueEnd = compositeKey.length - 17;
        result[0] = valueEnd > 0
                ? new String(compositeKey, 0, valueEnd, StandardCharsets.UTF_8)
                : "";
        // Key hash is last 16 bytes
        final byte[] hash = new byte[16];
        System.arraycopy(compositeKey, compositeKey.length - 16, hash, 0, 16);
        result[1] = hash;
        return result;
    }

    public int fastCount(final Iterable<byte[]> result) {
        if (result instanceof final Collection<?> collection) {
            return collection.size();
        }

        int count = 0;
        final var it = result.iterator();

        // Process 8 at a time (loop unrolling)
        while (it.hasNext()) {
            it.next();
            count++;
            if (!it.hasNext())
                break;
            it.next();
            count++;
            if (!it.hasNext())
                break;
            it.next();
            count++;
            if (!it.hasNext())
                break;
            it.next();
            count++;
            if (!it.hasNext())
                break;
            it.next();
            count++;
            if (!it.hasNext())
                break;
            it.next();
            count++;
            if (!it.hasNext())
                break;
            it.next();
            count++;
            if (!it.hasNext())
                break;
            it.next();
            count++;
        }

        return count;
    }

    /**
     * Result of an index search containing primary key hashes.
     * Use KeyMap to resolve hashes back to original String keys.
     */
    public record SearchResult(Iterable<byte[]> results) {
    }

    private SearchResult getSearchResult(final NavigableSet<byte[]> result, final int limit) {
        final Iterable<byte[]> iterable = () -> new Iterator<>() {
            private final Iterator<byte[]> it = result.iterator();
            private int remaining = limit == -1 ? Integer.MAX_VALUE : limit;

            @Override
            public boolean hasNext() {
                return remaining > 0 && it.hasNext();
            }

            @Override
            public byte[] next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                remaining--;
                return extractIndexKey(it.next());
            }
        };

        return new SearchResult(iterable);
    }

    private SearchResult getSearchResult(final NavigableSet<byte[]> result, final int limit,
            final Set<byte[]> excludedKeyHashes) {
        final Iterable<byte[]> iterable = () -> new Iterator<>() {
            private final Iterator<byte[]> it = result.iterator();
            private int remaining = limit == -1 ? Integer.MAX_VALUE : limit;
            private byte[] nextHash = null;

            @Override
            public boolean hasNext() {
                while (nextHash == null && remaining > 0 && it.hasNext()) {
                    final var hash = extractIndexKey(it.next());
                    if (!containsHash(excludedKeyHashes, hash)) {
                        nextHash = hash;
                    }
                }
                return nextHash != null;
            }

            @Override
            public byte[] next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                remaining--;
                final var result = nextHash;
                nextHash = null;
                return result;
            }
        };

        return new SearchResult(iterable);
    }

    private boolean containsHash(final Set<byte[]> set, final byte[] hash) {
        for (final byte[] h : set) {
            if (Arrays.equals(h, hash)) {
                return true;
            }
        }
        return false;
    }

    private byte[] createLowerBoundKey(final byte[] fieldBytes) {
        final byte[] key = new byte[fieldBytes.length + 1];
        System.arraycopy(fieldBytes, 0, key, 0, fieldBytes.length);
        key[fieldBytes.length] = indexSep;
        return key;
    }

    private byte[] createUpperBoundKey(final byte[] fieldBytes) {
        final byte[] key = new byte[fieldBytes.length + 2];
        System.arraycopy(fieldBytes, 0, key, 0, fieldBytes.length);
        key[fieldBytes.length] = indexSep;
        key[fieldBytes.length + 1] = upperByte;
        return key;
    }

    private NavigableSet<byte[]> getEqualIndexSubset(final NavigableSet<byte[]> index, final String searchTerm) {
        final byte[] fieldBytes = getSanitizedByte(searchTerm);
        return index.subSet(createLowerBoundKey(fieldBytes), true, createUpperBoundKey(fieldBytes), false);
    }

    public SearchResult getEqualIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        return getSearchResult(getEqualIndexSubset(index, searchTerm), limit);
    }

    public SearchResult getEqualIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit, final Set<byte[]> excludedKeyHashes) {
        return getSearchResult(getEqualIndexSubset(index, searchTerm), limit, excludedKeyHashes);
    }

    public SearchResult getBeforeIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final byte[] upperKey = createLowerBoundKey(getSanitizedByte(searchTerm));
        final byte[] lowerKey = new byte[] { 0 }; // Minimal key
        return getSearchResult(index.subSet(lowerKey, true, upperKey, false), limit);
    }

    public SearchResult getBeforeIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit, final Set<byte[]> excludedKeyHashes) {
        final byte[] upperKey = createLowerBoundKey(getSanitizedByte(searchTerm));
        final byte[] lowerKey = new byte[] { 0 }; // Minimal key
        return getSearchResult(index.subSet(lowerKey, true, upperKey, false), limit, excludedKeyHashes);
    }

    public SearchResult getAfterIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final byte[] upperKey = createUpperBoundKey(getSanitizedByte(searchTerm));
        final byte[] lowerKey = new byte[] { upperByte, upperByte };
        return getSearchResult(index.subSet(lowerKey, false, upperKey, false), limit);
    }

    public SearchResult getAfterIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit, final Set<byte[]> excludedKeyHashes) {
        final byte[] upperKey = createUpperBoundKey(getSanitizedByte(searchTerm));
        final byte[] lowerKey = new byte[] { upperByte, upperByte };
        return getSearchResult(index.subSet(lowerKey, false, upperKey, false), limit, excludedKeyHashes);
    }

    public SearchResult getLessThanIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final byte[] upperKey = createLowerBoundKey(createLowerBoundKey(getSanitizedByte(searchTerm)));
        return getSearchResult(index.headSet(upperKey, false), limit);
    }

    public SearchResult getLessThanIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit, final Set<byte[]> excludedKeyHashes) {
        final byte[] upperKey = createLowerBoundKey(createLowerBoundKey(getSanitizedByte(searchTerm)));
        return getSearchResult(index.headSet(upperKey, false), limit, excludedKeyHashes);
    }

    public SearchResult getLessThanOrEqualIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final byte[] upperKey = createUpperBoundKey(getSanitizedByte(searchTerm));
        return getSearchResult(index.headSet(upperKey, true), limit);
    }

    public SearchResult getLessThanOrEqualIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit, final Set<byte[]> excludedKeyHashes) {
        final byte[] upperKey = createUpperBoundKey(getSanitizedByte(searchTerm));
        return getSearchResult(index.headSet(upperKey, true), limit, excludedKeyHashes);
    }

    public SearchResult getGreaterThanIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final byte[] lowerKey = createUpperBoundKey(getSanitizedByte(searchTerm));
        return getSearchResult(index.tailSet(lowerKey, false), limit);
    }

    public SearchResult getGreaterThanIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit, final Set<byte[]> excludedKeyHashes) {
        final byte[] lowerKey = createUpperBoundKey(getSanitizedByte(searchTerm));
        return getSearchResult(index.tailSet(lowerKey, false), limit, excludedKeyHashes);
    }

    public SearchResult getGreaterThanOrEqualIndexSearch(final NavigableSet<byte[]> index,
            final String searchTerm, final int limit) {
        final byte[] lowerKey = createLowerBoundKey(createLowerBoundKey(getSanitizedByte(searchTerm)));
        return getSearchResult(index.tailSet(lowerKey, true), limit);
    }

    public SearchResult getGreaterThanOrEqualIndexSearch(final NavigableSet<byte[]> index,
            final String searchTerm, final int limit, final Set<byte[]> excludedKeyHashes) {
        final byte[] lowerKey = createLowerBoundKey(createLowerBoundKey(getSanitizedByte(searchTerm)));
        return getSearchResult(index.tailSet(lowerKey, true), limit, excludedKeyHashes);
    }

    public SearchResult getStartsWithIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final byte[] termBytes = getSanitizedByte(searchTerm);
        final byte[] upperKey = Arrays.copyOf(termBytes, termBytes.length + 1);
        upperKey[termBytes.length] = upperByte;
        return getSearchResult(index.subSet(termBytes, true, upperKey, false), limit);
    }

    public SearchResult getStartsWithIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit, final Set<byte[]> excludedKeyHashes) {
        final byte[] termBytes = getSanitizedByte(searchTerm);
        final byte[] upperKey = Arrays.copyOf(termBytes, termBytes.length + 1);
        upperKey[termBytes.length] = upperByte;
        return getSearchResult(index.subSet(termBytes, true, upperKey, false), limit, excludedKeyHashes);
    }

    public SearchResult getBetweenIndexSearch(final NavigableSet<byte[]> index, final String lowerBound,
            final String upperBound, final int limit) {
        final byte[] lowerKey = createLowerBoundKey(getSanitizedByte(lowerBound));
        final byte[] upperKey = createUpperBoundKey(getSanitizedByte(upperBound));
        return getSearchResult(index.subSet(lowerKey, true, upperKey, true), limit);
    }

    public SearchResult getBetweenIndexSearch(final NavigableSet<byte[]> index, final String lowerBound,
            final String upperBound, final int limit, final Set<byte[]> excludedKeyHashes) {
        final byte[] lowerKey = createLowerBoundKey(getSanitizedByte(lowerBound));
        final byte[] upperKey = createUpperBoundKey(getSanitizedByte(upperBound));
        return getSearchResult(index.subSet(lowerKey, true, upperKey, true), limit, excludedKeyHashes);
    }

    public SearchResult getLikeIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final Iterable<byte[]> iterable = () -> new Iterator<>() {
            private final Iterator<byte[]> it = index.iterator();
            private byte[] nextMatch = null;
            private int returned = 0;

            @Override
            public boolean hasNext() {
                if (limit != -1 && returned >= limit)
                    return false;

                if (nextMatch != null) {
                    return true;
                }

                while (it.hasNext()) {
                    final var indexValueAndKey = extractIndexValueAndKey(it.next());

                    if (CHRONICLE_UTILS.containsIgnoreCase(indexValueAndKey[0], searchTerm)) {
                        nextMatch = (byte[]) indexValueAndKey[1];
                        return true;
                    }
                }

                return false;
            }

            @Override
            public byte[] next() {
                if (nextMatch == null && !hasNext())
                    throw new NoSuchElementException();
                returned++;
                final var result = nextMatch;
                nextMatch = null;
                return result;
            }
        };

        return new SearchResult(iterable);
    }

    public SearchResult getLikeIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit, final Set<byte[]> excludedKeyHashes) {
        final Iterable<byte[]> iterable = () -> new Iterator<>() {
            private final Iterator<byte[]> it = index.iterator();
            private byte[] nextMatch = null;
            private int returned = 0;

            @Override
            public boolean hasNext() {
                if (limit != -1 && returned >= limit)
                    return false;

                if (nextMatch != null) {
                    return true;
                }

                while (it.hasNext()) {
                    final var indexValueAndKey = extractIndexValueAndKey(it.next());
                    final byte[] keyHash = (byte[]) indexValueAndKey[1];

                    if (containsHash(excludedKeyHashes, keyHash))
                        continue;

                    if (CHRONICLE_UTILS.containsIgnoreCase(indexValueAndKey[0], searchTerm)) {
                        nextMatch = keyHash;
                        return true;
                    }
                }

                return false;
            }

            @Override
            public byte[] next() {
                if (nextMatch == null && !hasNext())
                    throw new NoSuchElementException();
                returned++;
                final var result = nextMatch;
                nextMatch = null;
                return result;
            }
        };

        return new SearchResult(iterable);
    }

    public SearchResult getNotLikeIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final Iterable<byte[]> iterable = () -> new Iterator<>() {
            private final Iterator<byte[]> it = index.iterator();
            private byte[] nextMatch = null;
            private int returned = 0;

            @Override
            public boolean hasNext() {
                if (limit != -1 && returned >= limit)
                    return false;

                if (nextMatch != null) {
                    return true;
                }

                while (it.hasNext()) {
                    final var indexValueAndKey = extractIndexValueAndKey(it.next());

                    if (!CHRONICLE_UTILS.containsIgnoreCase(indexValueAndKey[0], searchTerm)) {
                        nextMatch = (byte[]) indexValueAndKey[1];
                        return true;
                    }
                }

                return false;
            }

            @Override
            public byte[] next() {
                if (nextMatch == null && !hasNext())
                    throw new NoSuchElementException();
                returned++;
                final var result = nextMatch;
                nextMatch = null;
                return result;
            }
        };

        return new SearchResult(iterable);
    }

    public SearchResult getNotLikeIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit, final Set<byte[]> excludedKeyHashes) {
        final Iterable<byte[]> iterable = () -> new Iterator<>() {
            private final Iterator<byte[]> it = index.iterator();
            private byte[] nextMatch = null;
            private int returned = 0;

            @Override
            public boolean hasNext() {
                if (limit != -1 && returned >= limit)
                    return false;

                if (nextMatch != null) {
                    return true;
                }

                while (it.hasNext()) {
                    final var indexValueAndKey = extractIndexValueAndKey(it.next());
                    final byte[] keyHash = (byte[]) indexValueAndKey[1];

                    if (containsHash(excludedKeyHashes, keyHash))
                        continue;

                    if (!CHRONICLE_UTILS.containsIgnoreCase(indexValueAndKey[0], searchTerm)) {
                        nextMatch = keyHash;
                        return true;
                    }
                }

                return false;
            }

            @Override
            public byte[] next() {
                if (nextMatch == null && !hasNext())
                    throw new NoSuchElementException();
                returned++;
                final var result = nextMatch;
                nextMatch = null;
                return result;
            }
        };

        return new SearchResult(iterable);
    }

    public SearchResult getInIndexSearch(final NavigableSet<byte[]> index, final Set<String> searchTerms,
            final int limit) {
        final Iterable<byte[]> lazyResults = () -> new Iterator<>() {
            private final Iterator<String> termIterator = searchTerms.iterator();
            private Iterator<byte[]> currentTermResults = Collections.emptyIterator();
            private int returned = 0;

            @Override
            public boolean hasNext() {
                if (limit != -1 && returned >= limit)
                    return false;

                while (!currentTermResults.hasNext() && termIterator.hasNext()) {
                    final String term = termIterator.next();
                    final NavigableSet<byte[]> matches = getEqualIndexSubset(index, term);
                    currentTermResults = new Iterator<>() {
                        final Iterator<byte[]> it = matches.iterator();

                        @Override
                        public boolean hasNext() {
                            return it.hasNext();
                        }

                        @Override
                        public byte[] next() {
                            return extractIndexKey(it.next());
                        }
                    };
                }

                return currentTermResults.hasNext();
            }

            @Override
            public byte[] next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                returned++;
                return currentTermResults.next();
            }
        };

        return new SearchResult(lazyResults);
    }

    public SearchResult getInIndexSearch(final NavigableSet<byte[]> index, final Set<String> searchTerms,
            final int limit, final Set<byte[]> excludedKeyHashes) {
        final Iterable<byte[]> lazyResults = () -> new Iterator<>() {
            private final Iterator<String> termIterator = searchTerms.iterator();
            private Iterator<byte[]> currentTermResults = Collections.emptyIterator();
            private int returned = 0;

            @Override
            public boolean hasNext() {
                if (limit != -1 && returned >= limit)
                    return false;

                while (!currentTermResults.hasNext() && termIterator.hasNext()) {
                    final String term = termIterator.next();
                    final NavigableSet<byte[]> matches = getEqualIndexSubset(index, term);
                    currentTermResults = new Iterator<>() {
                        final Iterator<byte[]> it = matches.iterator();
                        byte[] nextHash = null;

                        @Override
                        public boolean hasNext() {
                            while (nextHash == null && it.hasNext()) {
                                final var hash = extractIndexKey(it.next());
                                if (!containsHash(excludedKeyHashes, hash)) {
                                    nextHash = hash;
                                }
                            }
                            return nextHash != null;
                        }

                        @Override
                        public byte[] next() {
                            if (!hasNext())
                                throw new NoSuchElementException();
                            final var result = nextHash;
                            nextHash = null;
                            return result;
                        }
                    };
                }

                return currentTermResults.hasNext();
            }

            @Override
            public byte[] next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                returned++;
                return currentTermResults.next();
            }
        };

        return new SearchResult(lazyResults);
    }

    public SearchResult getNotInIndexSearch(final NavigableSet<byte[]> index, final Set<String> searchTerms,
            final int limit) {
        final Iterable<byte[]> iterable = () -> new Iterator<>() {
            final Iterator<byte[]> it = index.iterator();
            byte[] nextValid = null;
            int returned = 0;

            @Override
            public boolean hasNext() {
                if (limit != -1 && returned >= limit)
                    return false;

                if (nextValid != null)
                    return true;

                while (it.hasNext()) {
                    final var indexValueAndKey = extractIndexValueAndKey(it.next());
                    if (!searchTerms.contains(indexValueAndKey[0])) {
                        nextValid = (byte[]) indexValueAndKey[1];
                        return true;
                    }
                }

                return false;
            }

            @Override
            public byte[] next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                returned++;
                final var result = nextValid;
                nextValid = null;
                return result;
            }
        };

        return new SearchResult(iterable);
    }

    public SearchResult getNotInIndexSearch(final NavigableSet<byte[]> index, final Set<String> searchTerms,
            final int limit, final Set<byte[]> excludedKeyHashes) {
        final Iterable<byte[]> iterable = () -> new Iterator<>() {
            final Iterator<byte[]> it = index.iterator();
            byte[] nextValid = null;
            int returned = 0;

            @Override
            public boolean hasNext() {
                if (limit != -1 && returned >= limit)
                    return false;

                if (nextValid != null)
                    return true;

                while (it.hasNext()) {
                    final var indexValueAndKey = extractIndexValueAndKey(it.next());
                    final byte[] keyHash = (byte[]) indexValueAndKey[1];

                    if (containsHash(excludedKeyHashes, keyHash))
                        continue;

                    if (!searchTerms.contains(indexValueAndKey[0])) {
                        nextValid = keyHash;
                        return true;
                    }
                }

                return false;
            }

            @Override
            public byte[] next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                returned++;
                final var result = nextValid;
                nextValid = null; // reset
                return result;
            }
        };

        return new SearchResult(iterable);
    }

    public SearchResult getEndsWithIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final Iterable<byte[]> iterable = () -> new Iterator<>() {
            private final Iterator<byte[]> it = index.iterator();
            private byte[] nextItem = null;
            private boolean hasNextComputed = false;
            private int returned = 0;

            @Override
            public boolean hasNext() {
                if (hasNextComputed) {
                    return nextItem != null;
                }

                while (it.hasNext()) {
                    if (limit != -1 && returned >= limit) {
                        nextItem = null;
                        hasNextComputed = true;
                        return false;
                    }

                    final byte[] key = it.next();
                    final var indexValueAndKey = extractIndexValueAndKey(key);
                    final String fieldValue = (String) indexValueAndKey[0];

                    if (CHRONICLE_UTILS.endsWithIgnoreCase(fieldValue, searchTerm)) {
                        nextItem = (byte[]) indexValueAndKey[1];
                        hasNextComputed = true;
                        return true;
                    }
                }

                nextItem = null;
                hasNextComputed = true;
                return false;
            }

            @Override
            public byte[] next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                returned++;
                hasNextComputed = false;
                final byte[] result = nextItem;
                nextItem = null;
                return result;
            }
        };

        return new SearchResult(iterable);
    }

    public SearchResult getEndsWithIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit, final Set<byte[]> excludedKeyHashes) {
        final Iterable<byte[]> iterable = () -> new Iterator<>() {
            private final Iterator<byte[]> it = index.iterator();
            private byte[] nextItem = null;
            private boolean hasNextComputed = false;
            private int returned = 0;

            @Override
            public boolean hasNext() {
                if (hasNextComputed) {
                    return nextItem != null;
                }

                while (it.hasNext()) {
                    if (limit != -1 && returned >= limit) {
                        nextItem = null;
                        hasNextComputed = true;
                        return false;
                    }

                    final byte[] key = it.next();
                    final var indexValueAndKey = extractIndexValueAndKey(key);
                    final String fieldValue = (String) indexValueAndKey[0];
                    final byte[] keyHash = (byte[]) indexValueAndKey[1];

                    if (containsHash(excludedKeyHashes, keyHash)) {
                        continue;
                    }

                    if (CHRONICLE_UTILS.endsWithIgnoreCase(fieldValue, searchTerm)) {
                        nextItem = keyHash;
                        hasNextComputed = true;
                        return true;
                    }
                }

                nextItem = null;
                hasNextComputed = true;
                return false;
            }

            @Override
            public byte[] next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                returned++;
                hasNextComputed = false;
                final byte[] result = nextItem;
                nextItem = null;
                return result;
            }
        };

        return new SearchResult(iterable);
    }
}