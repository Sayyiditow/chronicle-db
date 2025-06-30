package chronicle.db.service;

import static chronicle.db.dao.ChronicleUtils.CHRONICLE_UTILS;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import org.mapdb.DB;
import org.mapdb.DBException;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.tinylog.Logger;

public final class MapDb {
    private MapDb() {
    }

    public static final MapDb MAP_DB = new MapDb();
    private static final ConcurrentMap<String, MapEntry> mapCache = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, TreeEntry> treeCache = new ConcurrentHashMap<>();
    public static final int MAP_DB_SEGMENTS = 8;
    public static final byte SEP = 0x1F;
    public static final String NON_CHAR = "\uFFFF";
    public static final String ASCII_0 = "\u0000";

    private static class TreeEntry {
        final DB db;
        final NavigableSet<byte[]> index;
        final LongAdder refCount;

        TreeEntry(final DB db, final NavigableSet<byte[]> index) {
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
                        .keySerializer(Serializer.STRING)
                        .valueSerializer(Serializer.STRING)
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
    public NavigableSet<byte[]> openIndex(final String filePath) {
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
                        .serializer(Serializer.BYTE_ARRAY)
                        .createOrOpen();
                return new TreeEntry(db, tree);
            } catch (final DBException.DataCorruption e) {
                CHRONICLE_UTILS.deleteFileIfExists(filePath); // let it reindex
                return null;
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

    private String sanitize(final Object input) {
        if (input == null)
            return "null";
        if (input.toString().indexOf(SEP) == -1)
            return input.toString();
        return input.toString().replace((char) SEP, ' ');
    }

    public byte[] createIndexKey(final Object fieldValue, final String primaryKey) {
        final byte[] fieldBytes = sanitize(fieldValue).getBytes(StandardCharsets.UTF_8);
        final byte[] keyBytes = sanitize(primaryKey).getBytes(StandardCharsets.UTF_8);
        final ByteBuffer buf = ByteBuffer.allocate(fieldBytes.length + 1 + keyBytes.length);
        buf.put(fieldBytes).put(SEP).put(keyBytes);
        return buf.array();
    }

    public String[] decodeKey(final byte[] keyBytes) {
        int sepIndex = -1;
        for (int i = 0; i < keyBytes.length; i++) {
            if (keyBytes[i] == SEP) {
                sepIndex = i;
                break;
            }
        }
        if (sepIndex == -1) {
            throw new IllegalArgumentException("Separator byte not found in key");
        }

        final String field = new String(keyBytes, 0, sepIndex, StandardCharsets.UTF_8);
        final String primaryKey = new String(keyBytes, sepIndex + 1, keyBytes.length - sepIndex - 1,
                StandardCharsets.UTF_8);

        return new String[] { field, primaryKey };
    }

    public String extractIndexKey(final byte[] indexKey) {
        final int sepIndex = findSeparator(indexKey);
        return new String(indexKey, sepIndex + 1, indexKey.length - sepIndex - 1, StandardCharsets.UTF_8);
    }

    public String extractIndexValue(final byte[] indexKey) {
        final int sepIndex = findSeparator(indexKey);
        return new String(indexKey, 0, sepIndex, StandardCharsets.UTF_8);
    }

    private String extractKey(final byte[] composite) {
        int delimPos = -1;
        for (int i = 0; i < composite.length; i++) {
            if (composite[i] == (byte) 0xFF) {
                delimPos = i;
                break;
            }
        }
        if (delimPos == -1 || delimPos == composite.length - 1) {
            return ""; // No key portion exists
        }
        return new String(composite, delimPos + 1, composite.length - delimPos - 1,
                StandardCharsets.UTF_8);
    }

    private int findSeparator(final byte[] data) {
        for (int i = 0; i < data.length; i++) {
            if (data[i] == SEP)
                return i;
        }
        throw new IllegalArgumentException("Separator byte not found in key");
    }

    public NavigableSet<byte[]> getEqualIndexSubset(final NavigableSet<byte[]> index, final String searchTerm) {
        final byte[] fieldBytes = searchTerm.getBytes(StandardCharsets.UTF_8);
        final byte[] lowerKey = ByteBuffer.allocate(fieldBytes.length + 1).put(fieldBytes).put(SEP).array();
        final byte[] upperKey = ByteBuffer.allocate(fieldBytes.length + 2).put(fieldBytes).put(SEP).put((byte) 0xFF)
                .array();
        return index.subSet(lowerKey, true, upperKey, false);
    }

    public record SearchResult<T>(Iterable<T> results, AtomicInteger size) {
    }

    private SearchResult<byte[]> getSearchResult(final NavigableSet<byte[]> result, final int limit) {
        final AtomicInteger count = new AtomicInteger(0);

        final Iterator<byte[]> iterator = result.iterator();
        final Iterator<byte[]> limitedIterator = new Iterator<>() {
            @Override
            public boolean hasNext() {
                return (limit == -1 || count.get() < limit) && iterator.hasNext();
            }

            @Override
            public byte[] next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                count.incrementAndGet();
                return iterator.next();
            }
        };

        return new SearchResult<>(() -> limitedIterator, count);
    }

    public SearchResult<byte[]> getEqualIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        return getSearchResult(getEqualIndexSubset(index, searchTerm), limit);
    }

    public SearchResult<byte[]> getBeforeIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final byte[] upperKey = createIndexKey(searchTerm, "");
        final byte[] lowerKey = new byte[] { 0 }; // Minimal key
        return getSearchResult(index.subSet(lowerKey, true, upperKey, false), limit);
    }

    public SearchResult<byte[]> getAfterIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final byte[] upperKey = createIndexKey(searchTerm, NON_CHAR);
        final byte[] lowerKey = new byte[] { (byte) 0xFF, (byte) 0xFF }; // NON_CHAR
        return getSearchResult(index.subSet(lowerKey, false, upperKey, false), limit);
    }

    public SearchResult<byte[]> getLessThanIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final byte[] upperKey = createIndexKey(searchTerm, "");
        return getSearchResult(index.headSet(upperKey, false), limit);
    }

    public SearchResult<byte[]> getLessThanOrEqualIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final byte[] upperKey = createIndexKey(searchTerm, NON_CHAR);
        return getSearchResult(index.headSet(upperKey, true), limit);
    }

    public SearchResult<byte[]> getGreaterThanIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final byte[] lowerKey = createIndexKey(searchTerm, NON_CHAR);
        return getSearchResult(index.tailSet(lowerKey, false), limit);
    }

    public SearchResult<byte[]> getGreaterThanOrEqualIndexSearch(final NavigableSet<byte[]> index,
            final String searchTerm, final int limit) {
        final byte[] lowerKey = createIndexKey(searchTerm, "");
        return getSearchResult(index.tailSet(lowerKey, true), limit);
    }

    public SearchResult<byte[]> getStartsWithIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final byte[] lowerKey = searchTerm.getBytes(StandardCharsets.UTF_8);
        final byte[] upperKey = (searchTerm + NON_CHAR).getBytes(StandardCharsets.UTF_8);
        return getSearchResult(index.subSet(lowerKey, true, upperKey, false), limit);
    }

    public SearchResult<byte[]> getBetweenIndexSearch(final NavigableSet<byte[]> index, final String lowerBound,
            final String upperBound, final int limit) {
        // Use createPrefixKey for bounds
        final byte[] lowerKey = createIndexKey(lowerBound, "");
        final byte[] upperKey = createIndexKey(upperBound, NON_CHAR);
        return getSearchResult(index.subSet(lowerKey, true, upperKey, true), limit);
    }

    public SearchResult<byte[]> getLikeIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final AtomicInteger count = new AtomicInteger(0);

        final Iterable<byte[]> iterable = () -> new Iterator<>() {
            private final Iterator<byte[]> it = index.iterator();
            private byte[] nextMatch = null;

            @Override
            public boolean hasNext() {
                if (limit != -1 && count.get() >= limit)
                    return false;

                while (it.hasNext()) {
                    final byte[] key = it.next();
                    final String[] decoded = decodeKey(key);
                    final String fieldValue = decoded[0];

                    if (CHRONICLE_UTILS.containsIgnoreCase(fieldValue, searchTerm)) {
                        nextMatch = key;
                        return true;
                    }
                }

                return false;
            }

            @Override
            public byte[] next() {
                if (nextMatch == null && !hasNext())
                    throw new NoSuchElementException();
                count.incrementAndGet();
                final byte[] result = nextMatch;
                nextMatch = null;
                return result;
            }
        };

        return new SearchResult<byte[]>(iterable, count);
    }

    public SearchResult<byte[]> getNotLikeIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final AtomicInteger count = new AtomicInteger(0);

        final Iterable<byte[]> iterable = () -> new Iterator<>() {
            private final Iterator<byte[]> it = index.iterator();
            private byte[] nextMatch = null;

            @Override
            public boolean hasNext() {
                if (limit != -1 && count.get() >= limit)
                    return false;

                while (it.hasNext()) {
                    final byte[] key = it.next();
                    final String[] decoded = decodeKey(key);
                    final String fieldValue = decoded[0];

                    if (!CHRONICLE_UTILS.containsIgnoreCase(fieldValue, searchTerm)) {
                        nextMatch = key;
                        return true;
                    }
                }

                return false;
            }

            @Override
            public byte[] next() {
                if (nextMatch == null && !hasNext())
                    throw new NoSuchElementException();
                count.incrementAndGet();
                final byte[] result = nextMatch;
                nextMatch = null;
                return result;
            }
        };

        return new SearchResult<byte[]>(iterable, count);
    }

    public SearchResult<byte[]> getInIndexSearch(final NavigableSet<byte[]> index, final Set<String> searchTerms,
            final int limit) {
        final AtomicInteger size = new AtomicInteger(0);
        Iterable<byte[]> results = Collections::emptyIterator;

        for (final String term : searchTerms) {
            final var resultSet = getEqualIndexSubset(index, term);
            size.addAndGet(resultSet.size());

            results = CHRONICLE_UTILS.concatIterable(results, resultSet);

            if (limit != -1 && size.get() >= limit) {
                break;
            }
        }

        return new SearchResult<>(results, size);
    }

    public SearchResult<byte[]> getNotInIndexSearch(final NavigableSet<byte[]> index,
            final Set<String> searchTerms,
            final int limit) {
        final AtomicInteger count = new AtomicInteger(0);
        final Iterable<byte[]> iterable = () -> new Iterator<>() {
            final Iterator<byte[]> it = index.iterator();
            byte[] nextValid = null;
            boolean hasNextComputed = false;

            @Override
            public boolean hasNext() {
                if (hasNextComputed)
                    return nextValid != null;

                while (it.hasNext()) {
                    if (limit != -1 && count.get() >= limit)
                        break;

                    final byte[] key = it.next();
                    final String[] decoded = decodeKey(key);
                    if (!searchTerms.contains(decoded[0])) {
                        nextValid = key;
                        hasNextComputed = true;
                        return true;
                    }
                }

                nextValid = null;
                hasNextComputed = true;
                return false;
            }

            @Override
            public byte[] next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                count.incrementAndGet();
                hasNextComputed = false;
                return nextValid;
            }
        };

        return new SearchResult<>(iterable, count);
    }

    public SearchResult<byte[]> getEndsWithIndexSearch(final NavigableSet<byte[]> index, final String searchTerm,
            final int limit) {
        final byte[] suffix = searchTerm.getBytes(StandardCharsets.UTF_8);
        final byte[] suffixWithSep = ByteBuffer.allocate(suffix.length + 1)
                .put(suffix).put(SEP).array();

        final AtomicInteger count = new AtomicInteger(0);
        final String suffixWithSepStr = new String(suffixWithSep, StandardCharsets.UTF_8);

        final Iterable<byte[]> iterable = () -> new Iterator<>() {
            private final Iterator<byte[]> it = index.iterator();
            private byte[] nextItem = null;
            private boolean hasNextComputed = false;

            @Override
            public boolean hasNext() {
                if (hasNextComputed) {
                    return nextItem != null;
                }

                while (it.hasNext()) {
                    if (limit != -1 && count.get() >= limit) {
                        nextItem = null;
                        hasNextComputed = true;
                        return false;
                    }

                    final byte[] key = it.next();
                    final String keyStr = new String(key, StandardCharsets.UTF_8);

                    // Skip invalid format
                    final int sepIndex = keyStr.indexOf((char) SEP);
                    if (sepIndex == -1) {
                        continue;
                    }

                    final int suffixStart = keyStr.length() - suffixWithSepStr.length()
                            - (keyStr.length() - sepIndex - 1);
                    if (suffixStart >= 0
                            && keyStr.regionMatches(suffixStart, suffixWithSepStr, 0, suffixWithSepStr.length())) {

                        nextItem = key;
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
                count.incrementAndGet();
                final byte[] result = nextItem;
                nextItem = null;
                hasNextComputed = false;
                return result;
            }
        };

        return new SearchResult<byte[]>(iterable, count);
    }

    public boolean isLessThanIndexMatch(final NavigableSet<byte[]> index, final String searchTerm, final String key) {
        final byte[] lowerKey = createIndexKey("", ""); // Minimum bound
        final byte[] upperKey = createIndexKey(searchTerm, ""); // searchTerm + SEP
        final NavigableSet<byte[]> range = index.subSet(lowerKey, true, upperKey, false);

        for (final byte[] indexKey : range) {
            if (decodeKey(indexKey)[1].equals(key)) {
                return true;
            }
        }
        return false;
    }

    public boolean isLessThanOrEqualIndexMatch(final NavigableSet<byte[]> index, final String searchTerm,
            final String key) {
        final byte[] lowerKey = createIndexKey("", "");
        final byte[] upperKey = createIndexKey(searchTerm, NON_CHAR); // Include up to searchTerm + high byte
        final NavigableSet<byte[]> range = index.subSet(lowerKey, true, upperKey, true);

        for (final byte[] indexKey : range) {
            if (decodeKey(indexKey)[1].equals(key)) {
                return true;
            }
        }
        return false;
    }

    public boolean isGreaterThanIndexMatch(final NavigableSet<byte[]> index, final String searchTerm,
            final String key) {
        final byte[] lowerKey = createIndexKey(searchTerm, "");
        final byte[] upperKey = createIndexKey(NON_CHAR, "");
        final NavigableSet<byte[]> range = index.subSet(lowerKey, false, upperKey, true);

        for (final byte[] indexKey : range) {
            if (decodeKey(indexKey)[1].equals(key)) {
                return true;
            }
        }
        return false;
    }

    public boolean isGreaterThanOrEqualIndexMatch(final NavigableSet<byte[]> index, final String searchTerm,
            final String key) {
        final byte[] lowerKey = createIndexKey(searchTerm, "");
        final byte[] upperKey = createIndexKey(NON_CHAR, "");
        final NavigableSet<byte[]> range = index.subSet(lowerKey, true, upperKey, true);

        for (final byte[] indexKey : range) {
            if (decodeKey(indexKey)[1].equals(key)) {
                return true;
            }
        }
        return false;
    }

    public boolean isLikeIndexMatch(final NavigableSet<byte[]> index, final String searchTerm, final String key) {
        // Optimized for cases where searchTerm appears early in keys
        for (final byte[] composite : index) {
            final String foundKey = extractKey(composite);
            if (foundKey.contains(searchTerm)) {
                return foundKey.equals(key);
            }
        }
        return false;
    }

    public boolean isStartsWithIndexMatch(final NavigableSet<byte[]> index, final String searchTerm, final String key) {
        final byte[] lowerKey = createIndexKey(searchTerm, "");
        final byte[] upperKey = createIndexKey(searchTerm, NON_CHAR);
        final NavigableSet<byte[]> range = index.subSet(lowerKey, true, upperKey, false);

        for (final byte[] indexKey : range) {
            if (decodeKey(indexKey)[1].equals(key)) {
                return true;
            }
        }
        return false;
    }

    public boolean isEndsWithIndexMatch(final NavigableSet<byte[]> index, final String searchTerm,
            final String primaryKey) {
        final String searchTermLower = searchTerm.toLowerCase();
        final byte[] searchTermBytes = searchTermLower.getBytes(StandardCharsets.UTF_8);
        final byte[] primaryKeyBytes = primaryKey.getBytes(StandardCharsets.UTF_8);
        final byte[] sep = new byte[] { SEP }; // 0x1F

        // Lower bound: 0x00 + searchTerm + SEP + primaryKey
        final ByteBuffer lowerBuf = ByteBuffer.allocate(1 + searchTermBytes.length + 1 + primaryKeyBytes.length);
        lowerBuf.put((byte) 0x00).put(searchTermBytes).put(sep).put(primaryKeyBytes);
        final byte[] lowerKey = lowerBuf.array();

        // Upper bound: 0xFF + searchTerm + SEP + primaryKey
        final ByteBuffer upperBuf = ByteBuffer.allocate(1 + searchTermBytes.length + 1 + primaryKeyBytes.length);
        upperBuf.put((byte) 0xFF).put(searchTermBytes).put(sep).put(primaryKeyBytes);
        final byte[] upperKey = upperBuf.array();

        final NavigableSet<byte[]> subset = index.subSet(lowerKey, true, upperKey, true);
        boolean match = false;

        for (final byte[] key : subset) {
            final String[] parts = decodeKey(key);
            if (parts[1].equals(primaryKey) && parts[0].toLowerCase().endsWith(searchTermLower)) {
                match = true;
            }
        }

        return match;
    }

    public boolean isBetweenIndexMatch(final NavigableSet<byte[]> index, final String lowerBound,
            final String upperBound, final String key) {
        // Use the SAME key format as getBetweenIndexSubset
        final byte[] lowerKey = createIndexKey(lowerBound, ""); // Prefix-based lower bound
        final byte[] upperKey = createIndexKey(upperBound, NON_CHAR); // Prefix-based upper bound

        // Get all keys in the range (like getBetweenIndexSubset)
        final NavigableSet<byte[]> range = index.subSet(lowerKey, true, upperKey, true);

        // Check if any key in the range ends with SEP + key
        for (final byte[] indexKey : range) {
            if (decodeKey(indexKey)[1].equals(key)) {
                return true;
            }
        }
        return false;
    }
}