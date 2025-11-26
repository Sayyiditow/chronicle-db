package chronicle.db.service;

import static chronicle.db.dao.ChronicleUtils.CHRONICLE_UTILS;

import java.nio.ByteBuffer;
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
 * SharedKeyMap and SharedIndexMap instances.
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
 * SharedIndexMap idx = MAP_DB.openIndex("/path/to/index.db");
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
    private static final ConcurrentMap<String, SharedIndexMap> treeCache = new ConcurrentHashMap<>();
    private static final byte indexSep = 0x1F;
    private static final byte upperByte = (byte) 0xFF;

    private MapDb() {
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
        public final HTreeMap<String, String> map;

        private final AtomicInteger refCount;
        private final String filePath; // Track file path for cleanup

        SharedKeyMap(final HTreeMap<String, String> map, final String filePath) {
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
    public static class SharedIndexMap implements AutoCloseable {
        /** The underlying MapDB NavigableSet (sorted index) */
        public final NavigableSet<byte[]> index;

        private final DB db;
        private final AtomicInteger refCount;
        private final String filePath; // Track file path for cleanup

        SharedIndexMap(final DB db, final NavigableSet<byte[]> index, final String filePath) {
            this.db = db;
            this.index = index;
            this.filePath = filePath;
            this.refCount = new AtomicInteger(1); // Start with a reference count of 1
        }

        /**
         * Increments the reference count when sharing this index.
         * 
         * @return This SharedIndexMap instance for chaining
         */
        SharedIndexMap retain() {
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
                    entry.db.commit();
                    entry.db.close();
                    return null;
                }
                return entry; // Keep the entry
            });
        }

        /**
         * Commits pending changes to the index.
         * <p>
         * Call this after adding or removing entries to persist changes to disk.
         * </p>
         */
        public void commit() {
            this.db.commit();
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
                final HTreeMap<String, String> map = DBMaker.fileDB(filePath)
                        .allocateStartSize(512 * 1024 * 1024) // initial size
                        .allocateIncrement(128 * 1024 * 1024) // Grow by 48 MB
                        .closeOnJvmShutdown()
                        .fileLockDisable()
                        .fileMmapEnableIfSupported()
                        .fileMmapPreclearDisable()
                        .cleanerHackEnable()
                        .make()
                        .hashMap("map")
                        .keySerializer(Serializer.STRING)
                        .valueSerializer(Serializer.STRING)
                        .createOrOpen();

                return new SharedKeyMap(map, filePath);
            } catch (final DBException.DataCorruption | DBException.VolumeEOF | NegativeArraySizeException e) {
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
    public SharedIndexMap openIndex(final String filePath) {
        final var entry = treeCache.compute(filePath, (k, existingEntry) -> {
            if (existingEntry != null) {
                return existingEntry.retain();
            }

            // Create a new entry
            try {
                final var db = DBMaker.fileDB(filePath)
                        .allocateStartSize(512 * 1024 * 1024)
                        .allocateIncrement(128 * 1024 * 1024)
                        .closeOnJvmShutdown()
                        .fileLockDisable()
                        .fileMmapEnableIfSupported()
                        .fileMmapPreclearDisable()
                        .cleanerHackEnable()
                        .transactionEnable()
                        .make();
                final var tree = db.treeSet("index")
                        .serializer(Serializer.BYTE_ARRAY)
                        .createOrOpen();

                return new SharedIndexMap(db, tree, filePath);
            } catch (final DBException.DataCorruption | DBException.VolumeEOF | NegativeArraySizeException e) {
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

    private byte[] getSanitizedByte(final Object value) {
        return sanitize(value).getBytes(StandardCharsets.UTF_8);
    }

    public byte[] createIndexKey(final Object fieldValue, final String primaryKey) {
        final byte[] fieldBytes = getSanitizedByte(fieldValue);
        final byte[] keyBytes = getSanitizedByte(primaryKey);
        final byte[] result = new byte[fieldBytes.length + 1 + keyBytes.length];
        System.arraycopy(fieldBytes, 0, result, 0, fieldBytes.length); // Copy 1st part
        result[fieldBytes.length] = indexSep; // Insert separator
        System.arraycopy(keyBytes, 0, result, fieldBytes.length + 1, keyBytes.length); // Copy 2nd part
        return result;
    }

    public String extractIndexKey(final byte[] compositeKey) {
        for (int i = 0; i < compositeKey.length; i++) {
            if (compositeKey[i] == indexSep) {
                return new String(Arrays.copyOfRange(compositeKey, i + 1, compositeKey.length), StandardCharsets.UTF_8);
            }
        }
        return new String(compositeKey, StandardCharsets.UTF_8); // fallback: no separator found
    }

    public String extractIndexValue(final byte[] compositeKey) {
        for (int i = 0; i < compositeKey.length; i++) {
            if (compositeKey[i] == indexSep) {
                return new String(compositeKey, 0, i, StandardCharsets.UTF_8);
            }
        }
        return new String(compositeKey, StandardCharsets.UTF_8); // fallback: no separator found
    }

    public String[] extractIndexValueAndKey(final byte[] compositeKey) {
        final var keyAndValue = new String[2];
        for (int i = 0; i < compositeKey.length; i++) {
            if (compositeKey[i] == indexSep) {
                keyAndValue[0] = new String(compositeKey, 0, i, StandardCharsets.UTF_8);
                keyAndValue[1] = new String(Arrays.copyOfRange(compositeKey, i + 1, compositeKey.length),
                        StandardCharsets.UTF_8);
                return keyAndValue;
            }
        }
        return keyAndValue;
    }

    public int fastCount(final Iterable<String> result) {
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

    public record SearchResult(Iterable<String> results) {
    }

    private SearchResult getSearchResult(final NavigableSet<byte[]> result, final int limit) {
        final Iterable<String> iterable = () -> new Iterator<>() {
            private final Iterator<byte[]> it = result.iterator();
            private int remaining = limit == -1 ? Integer.MAX_VALUE : limit;

            @Override
            public boolean hasNext() {
                return remaining > 0 && it.hasNext();
            }

            @Override
            public String next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                remaining--;
                return extractIndexKey(it.next());
            }
        };

        return new SearchResult(iterable);
    }

    private SearchResult getSearchResult(final NavigableSet<byte[]> result, final int limit,
            final Set<String> excludedKeys) {
        final Iterable<String> iterable = () -> new Iterator<>() {
            private final Iterator<byte[]> it = result.iterator();
            private int remaining = limit == -1 ? Integer.MAX_VALUE : limit;

            @Override
            public boolean hasNext() {
                return remaining > 0 && it.hasNext();
            }

            @Override
            public String next() {
                while (hasNext()) {
                    final var key = extractIndexKey(it.next());
                    if (!excludedKeys.contains(key)) {
                        remaining--;
                        return key;
                    }
                }
                throw new NoSuchElementException();
            }

        };

        return new SearchResult(iterable);
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
            final int limit, final Set<String> excludedKeys) {
        return getSearchResult(getEqualIndexSubset(index, searchTerm), limit, excludedKeys);
    }

    public SearchResult getBeforeIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final byte[] upperKey = createLowerBoundKey(getSanitizedByte(searchTerm));
        final byte[] lowerKey = new byte[] { 0 }; // Minimal key
        return getSearchResult(index.subSet(lowerKey, true, upperKey, false), limit);
    }

    public SearchResult getBeforeIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit, final Set<String> excludedKeys) {
        final byte[] upperKey = createLowerBoundKey(getSanitizedByte(searchTerm));
        final byte[] lowerKey = new byte[] { 0 }; // Minimal key
        return getSearchResult(index.subSet(lowerKey, true, upperKey, false), limit, excludedKeys);
    }

    public SearchResult getAfterIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final byte[] upperKey = createUpperBoundKey(getSanitizedByte(searchTerm));
        final byte[] lowerKey = new byte[] { upperByte, upperByte };
        return getSearchResult(index.subSet(lowerKey, false, upperKey, false), limit);
    }

    public SearchResult getAfterIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit, final Set<String> excludedKeys) {
        final byte[] upperKey = createUpperBoundKey(getSanitizedByte(searchTerm));
        final byte[] lowerKey = new byte[] { upperByte, upperByte };
        return getSearchResult(index.subSet(lowerKey, false, upperKey, false), limit, excludedKeys);
    }

    public SearchResult getLessThanIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final byte[] upperKey = createLowerBoundKey(createLowerBoundKey(getSanitizedByte(searchTerm)));
        return getSearchResult(index.headSet(upperKey, false), limit);
    }

    public SearchResult getLessThanIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit, final Set<String> excludedKeys) {
        final byte[] upperKey = createLowerBoundKey(createLowerBoundKey(getSanitizedByte(searchTerm)));
        return getSearchResult(index.headSet(upperKey, false), limit, excludedKeys);
    }

    public SearchResult getLessThanOrEqualIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final byte[] upperKey = createUpperBoundKey(getSanitizedByte(searchTerm));
        return getSearchResult(index.headSet(upperKey, true), limit);
    }

    public SearchResult getLessThanOrEqualIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit, final Set<String> excludedKeys) {
        final byte[] upperKey = createUpperBoundKey(getSanitizedByte(searchTerm));
        return getSearchResult(index.headSet(upperKey, true), limit, excludedKeys);
    }

    public SearchResult getGreaterThanIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final byte[] lowerKey = createUpperBoundKey(getSanitizedByte(searchTerm));
        return getSearchResult(index.tailSet(lowerKey, false), limit);
    }

    public SearchResult getGreaterThanIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit, final Set<String> excludedKeys) {
        final byte[] lowerKey = createUpperBoundKey(getSanitizedByte(searchTerm));
        return getSearchResult(index.tailSet(lowerKey, false), limit, excludedKeys);
    }

    public SearchResult getGreaterThanOrEqualIndexSearch(final NavigableSet<byte[]> index,
            final String searchTerm, final int limit) {
        final byte[] lowerKey = createLowerBoundKey(createLowerBoundKey(getSanitizedByte(searchTerm)));
        return getSearchResult(index.tailSet(lowerKey, true), limit);
    }

    public SearchResult getGreaterThanOrEqualIndexSearch(final NavigableSet<byte[]> index,
            final String searchTerm, final int limit, final Set<String> excludedKeys) {
        final byte[] lowerKey = createLowerBoundKey(createLowerBoundKey(getSanitizedByte(searchTerm)));
        return getSearchResult(index.tailSet(lowerKey, true), limit, excludedKeys);
    }

    public SearchResult getStartsWithIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final byte[] termBytes = getSanitizedByte(searchTerm);
        final byte[] upperKey = Arrays.copyOf(termBytes, termBytes.length + 1);
        upperKey[termBytes.length] = upperByte;
        return getSearchResult(index.subSet(termBytes, true, upperKey, false), limit);
    }

    public SearchResult getStartsWithIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit, final Set<String> excludedKeys) {
        final byte[] termBytes = getSanitizedByte(searchTerm);
        final byte[] upperKey = Arrays.copyOf(termBytes, termBytes.length + 1);
        upperKey[termBytes.length] = upperByte;
        return getSearchResult(index.subSet(termBytes, true, upperKey, false), limit, excludedKeys);
    }

    public SearchResult getBetweenIndexSearch(final NavigableSet<byte[]> index, final String lowerBound,
            final String upperBound, final int limit) {
        final byte[] lowerKey = createLowerBoundKey(getSanitizedByte(lowerBound));
        final byte[] upperKey = createUpperBoundKey(getSanitizedByte(upperBound));
        return getSearchResult(index.subSet(lowerKey, true, upperKey, true), limit);
    }

    public SearchResult getBetweenIndexSearch(final NavigableSet<byte[]> index, final String lowerBound,
            final String upperBound, final int limit, final Set<String> excludedKeys) {
        final byte[] lowerKey = createLowerBoundKey(getSanitizedByte(lowerBound));
        final byte[] upperKey = createUpperBoundKey(getSanitizedByte(upperBound));
        return getSearchResult(index.subSet(lowerKey, true, upperKey, true), limit, excludedKeys);
    }

    public SearchResult getLikeIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final Iterable<String> iterable = () -> new Iterator<>() {
            private final Iterator<byte[]> it = index.iterator();
            private String nextMatch = null;
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
                        nextMatch = indexValueAndKey[1];
                        return true;
                    }
                }

                return false;
            }

            @Override
            public String next() {
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
            final int limit, final Set<String> excludedKeys) {
        final Iterable<String> iterable = () -> new Iterator<>() {
            private final Iterator<byte[]> it = index.iterator();
            private String nextMatch = null;
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

                    if (excludedKeys.contains(indexValueAndKey[1]))
                        continue;

                    if (CHRONICLE_UTILS.containsIgnoreCase(indexValueAndKey[0], searchTerm)) {
                        nextMatch = indexValueAndKey[1];
                        return true;
                    }
                }

                return false;
            }

            @Override
            public String next() {
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
        final Iterable<String> iterable = () -> new Iterator<>() {
            private final Iterator<byte[]> it = index.iterator();
            private String nextMatch = null;
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
                        nextMatch = indexValueAndKey[0];
                        return true;
                    }
                }

                return false;
            }

            @Override
            public String next() {
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
            final int limit, final Set<String> excludedKeys) {
        final Iterable<String> iterable = () -> new Iterator<>() {
            private final Iterator<byte[]> it = index.iterator();
            private String nextMatch = null;
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

                    if (excludedKeys.contains(indexValueAndKey[1]))
                        continue;

                    if (!CHRONICLE_UTILS.containsIgnoreCase(indexValueAndKey[0], searchTerm)) {
                        nextMatch = indexValueAndKey[1];
                        return true;
                    }
                }

                return false;
            }

            @Override
            public String next() {
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
        final Iterable<String> lazyResults = () -> new Iterator<>() {
            private final Iterator<String> termIterator = searchTerms.iterator();
            private Iterator<String> currentTermResults = Collections.emptyIterator();
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
                        public String next() {
                            return extractIndexKey(it.next());
                        }
                    };
                }

                return currentTermResults.hasNext();
            }

            @Override
            public String next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                returned++;
                return currentTermResults.next();
            }
        };

        return new SearchResult(lazyResults);
    }

    public SearchResult getInIndexSearch(final NavigableSet<byte[]> index, final Set<String> searchTerms,
            final int limit, final Set<String> excludedKeys) {
        final Iterable<String> lazyResults = () -> new Iterator<>() {
            private final Iterator<String> termIterator = searchTerms.iterator();
            private Iterator<String> currentTermResults = Collections.emptyIterator();
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
                        public String next() {
                            while (it.hasNext()) {
                                final var key = extractIndexKey(it.next());
                                if (!excludedKeys.contains(key)) {
                                    return key;
                                }
                            }
                            throw new NoSuchElementException();
                        }
                    };
                }

                return currentTermResults.hasNext();
            }

            @Override
            public String next() {
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
        final Iterable<String> iterable = () -> new Iterator<>() {
            final Iterator<byte[]> it = index.iterator();
            String nextValid = null;
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
                        nextValid = indexValueAndKey[1];
                        return true;
                    }
                }

                return false;
            }

            @Override
            public String next() {
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
            final int limit, final Set<String> excludedKeys) {
        final Iterable<String> iterable = () -> new Iterator<>() {
            final Iterator<byte[]> it = index.iterator();
            String nextValid = null;
            int returned = 0;

            @Override
            public boolean hasNext() {
                if (limit != -1 && returned >= limit)
                    return false;

                if (nextValid != null)
                    return true;

                while (it.hasNext()) {
                    final var indexValueAndKey = extractIndexValueAndKey(it.next());

                    if (excludedKeys.contains(indexValueAndKey[1]))
                        continue;

                    if (!searchTerms.contains(indexValueAndKey[0])) {
                        nextValid = indexValueAndKey[1];
                        return true;
                    }
                }

                return false;
            }

            @Override
            public String next() {
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
        final byte[] suffix = getSanitizedByte(searchTerm);
        final byte[] suffixWithSep = ByteBuffer.allocate(suffix.length + 1)
                .put(suffix).put(indexSep).array();
        final String suffixWithSepStr = new String(suffixWithSep, StandardCharsets.UTF_8);

        final Iterable<String> iterable = () -> new Iterator<>() {
            private final Iterator<byte[]> it = index.iterator();
            private String nextItem = null;
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
                    final String keyStr = new String(key, StandardCharsets.UTF_8);
                    final int sepIndex = keyStr.indexOf((char) indexSep);

                    if (sepIndex == -1) {
                        continue; // Malformed entry
                    }

                    final int suffixStart = keyStr.length() - suffixWithSepStr.length()
                            - (keyStr.length() - sepIndex - 1);

                    if (suffixStart >= 0 &&
                            keyStr.regionMatches(suffixStart, suffixWithSepStr, 0, suffixWithSepStr.length())) {
                        nextItem = extractIndexKey(key); // Extract as String
                        hasNextComputed = true;
                        return true;
                    }
                }

                nextItem = null;
                hasNextComputed = true;
                return false;
            }

            @Override
            public String next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                returned++;
                hasNextComputed = false;
                final String result = nextItem;
                nextItem = null;
                return result;
            }
        };

        return new SearchResult(iterable);
    }

    public SearchResult getEndsWithIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit, final Set<String> excludedKeys) {
        final byte[] suffix = getSanitizedByte(searchTerm);
        final byte[] suffixWithSep = ByteBuffer.allocate(suffix.length + 1)
                .put(suffix).put(indexSep).array();
        final String suffixWithSepStr = new String(suffixWithSep, StandardCharsets.UTF_8);

        final Iterable<String> iterable = () -> new Iterator<>() {
            private final Iterator<byte[]> it = index.iterator();
            private String nextItem = null;
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
                    final String keyStr = new String(key, StandardCharsets.UTF_8);
                    final int sepIndex = keyStr.indexOf((char) indexSep);
                    if (sepIndex == -1) {
                        continue;
                    }

                    final int suffixStart = keyStr.length() - suffixWithSepStr.length()
                            - (keyStr.length() - sepIndex - 1);

                    if (suffixStart >= 0 &&
                            keyStr.regionMatches(suffixStart, suffixWithSepStr, 0, suffixWithSepStr.length())) {
                        final String primary = keyStr.substring(sepIndex + 1);
                        if (!excludedKeys.contains(primary)) {
                            nextItem = primary;
                            hasNextComputed = true;
                            return true;
                        }
                    }
                }

                nextItem = null;
                hasNextComputed = true;
                return false;
            }

            @Override
            public String next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                returned++;
                hasNextComputed = false;
                final String result = nextItem;
                nextItem = null;
                return result;
            }
        };

        return new SearchResult(iterable);
    }
}