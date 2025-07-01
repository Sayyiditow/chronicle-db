package chronicle.db.service;

import static chronicle.db.dao.ChronicleUtils.CHRONICLE_UTILS;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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
    private static final byte[] upperBoundByte = new byte[] { (byte) 0xFF };
    private static final byte[] lowerBoundByte = new byte[] { 0x00 };
    private static final byte[] zeroByte = new byte[0];

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

    public byte[] getSanitizedByte(final Object value) {
        return sanitize(value).getBytes(StandardCharsets.UTF_8);
    }

    public byte[] createIndexKey(final byte[] fieldBytes, final byte[] keyBytes) {
        return ByteBuffer.allocate(fieldBytes.length + 1 + keyBytes.length)
                .put(fieldBytes).put(SEP).put(keyBytes).array();
    }

    public byte[] createIndexKey(final Object fieldValue, final String primaryKey) {
        final byte[] fieldBytes = getSanitizedByte(fieldValue);
        final byte[] keyBytes = getSanitizedByte(primaryKey);
        return ByteBuffer.allocate(fieldBytes.length + 1 + keyBytes.length)
                .put(fieldBytes).put(SEP).put(keyBytes).array();
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

    public String extractIndexKeyFromCompositeKey(final byte[] indexKey) {
        final int sepIndex = findSeparator(indexKey);
        return new String(indexKey, sepIndex + 1, indexKey.length - sepIndex - 1, StandardCharsets.UTF_8);
    }

    public String extractIndexKey(final byte[] keyBytes) {
        return new String(keyBytes, StandardCharsets.UTF_8);
    }

    public String extractIndexValue(final byte[] indexKey) {
        final int sepIndex = findSeparator(indexKey);
        return new String(indexKey, 0, sepIndex, StandardCharsets.UTF_8);
    }

    public byte[] extractIndexKeyBytes(final byte[] compositeKey) {
        for (int i = 0; i < compositeKey.length; i++) {
            if (compositeKey[i] == SEP) {
                return Arrays.copyOfRange(compositeKey, i + 1, compositeKey.length);
            }
        }
        return compositeKey; // fallback: no separator found
    }

    private int findSeparator(final byte[] data) {
        for (int i = 0; i < data.length; i++) {
            if (data[i] == SEP)
                return i;
        }
        throw new IllegalArgumentException("Separator byte not found in key");
    }

    private boolean matchesPrimaryKey(final byte[] indexKey, final byte[] keyBytes) {
        int sepIndex = -1;
        for (int i = 0; i < indexKey.length; i++) {
            if (indexKey[i] == SEP) {
                sepIndex = i;
                break;
            }
        }

        if (sepIndex == -1)
            return false;
        final int keyLength = keyBytes.length;
        if (indexKey.length - sepIndex - 1 != keyLength)
            return false;

        for (int i = 0; i < keyLength; i++) {
            if (indexKey[sepIndex + 1 + i] != keyBytes[i])
                return false;
        }
        return true;
    }

    public record SearchResult<T>(Iterable<T> results, AtomicInteger size) {
    }

    private SearchResult<byte[]> getSearchResult(final NavigableSet<byte[]> result, final int limit) {
        final AtomicInteger count = new AtomicInteger(0); // Updated during iteration
        final Iterable<byte[]> iterable = () -> new Iterator<byte[]>() {
            private final Iterator<byte[]> it = result.iterator();
            private int remainingLimit = limit == -1 ? Integer.MAX_VALUE : limit;

            @Override
            public boolean hasNext() {
                return (remainingLimit > 0) && it.hasNext();
            }

            @Override
            public byte[] next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                remainingLimit--;
                count.incrementAndGet();
                return extractIndexKeyBytes(it.next()); // Extract only the primary key
            }
        };

        return new SearchResult<>(iterable, count);
    }

    public NavigableSet<byte[]> getEqualIndexSubset(final NavigableSet<byte[]> index, final String searchTerm) {
        final byte[] fieldBytes = searchTerm.getBytes(StandardCharsets.UTF_8);
        final byte[] lowerKey = ByteBuffer.allocate(fieldBytes.length + 1).put(fieldBytes).put(SEP).array();
        final byte[] upperKey = ByteBuffer.allocate(fieldBytes.length + 2).put(fieldBytes).put(SEP).put((byte) 0xFF)
                .array();
        return index.subSet(lowerKey, true, upperKey, false);
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
                    final String fieldValue = extractIndexValue(key);

                    if (CHRONICLE_UTILS.containsIgnoreCase(fieldValue, searchTerm)) {
                        nextMatch = extractIndexKeyBytes(key);
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
                    final String fieldValue = extractIndexValue(key);

                    if (!CHRONICLE_UTILS.containsIgnoreCase(fieldValue, searchTerm)) {
                        nextMatch = extractIndexKeyBytes(key);
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

    public SearchResult<byte[]> getInIndexSearch(final NavigableSet<byte[]> index,
            final Set<String> searchTerms, final int limit) {

        final AtomicInteger size = new AtomicInteger(0);

        final Iterable<byte[]> lazyResults = () -> new Iterator<>() {
            private final Iterator<String> termIterator = searchTerms.iterator();
            private Iterator<byte[]> currentTermResults = Collections.emptyIterator();

            @Override
            public boolean hasNext() {
                if (limit > 0 && size.get() >= limit)
                    return false;

                while (!currentTermResults.hasNext() && termIterator.hasNext()) {
                    final String term = termIterator.next();
                    final NavigableSet<byte[]> matches = getEqualIndexSubset(index, term);
                    // ↓ Strip to just primary key
                    currentTermResults = matches.stream()
                            .map(MAP_DB::extractIndexKeyBytes) // <-- this line replaces full composite key
                            .iterator();
                }

                return currentTermResults.hasNext();
            }

            @Override
            public byte[] next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                size.incrementAndGet();
                return currentTermResults.next();
            }
        };

        return new SearchResult<>(lazyResults, size);
    }

    public SearchResult<byte[]> getNotInIndexSearch(final NavigableSet<byte[]> index,
            final Set<String> searchTerms, final int limit) {
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
                    if (!searchTerms.contains(extractIndexValue(key))) {
                        nextValid = extractIndexKeyBytes(key);
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

                        nextItem = extractIndexKeyBytes(key);
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

    public boolean isLessThanIndexMatch(final NavigableSet<byte[]> index, final byte[] searchTerm, final byte[] key) {
        final byte[] lowerKey = createIndexKey(zeroByte, zeroByte); // Minimum bound
        final byte[] upperKey = createIndexKey(searchTerm, upperBoundByte);
        final NavigableSet<byte[]> range = index.subSet(lowerKey, true, upperKey, false);

        for (final byte[] indexKey : range) {
            if (matchesPrimaryKey(indexKey, key)) {
                return true;
            }
        }
        return false;
    }

    public boolean isLessThanOrEqualIndexMatch(final NavigableSet<byte[]> index, final byte[] searchTerm,
            final byte[] key) {
        final byte[] lowerKey = createIndexKey(zeroByte, zeroByte); // Minimum bound
        final byte[] upperKey = createIndexKey(searchTerm, upperBoundByte);
        final NavigableSet<byte[]> range = index.subSet(lowerKey, true, upperKey, true);

        for (final byte[] indexKey : range) {
            if (matchesPrimaryKey(indexKey, key)) {
                return true;
            }
        }
        return false;
    }

    public boolean isGreaterThanIndexMatch(final NavigableSet<byte[]> index, final byte[] searchTerm,
            final byte[] key) {
        final byte[] lowerKey = createIndexKey(searchTerm, lowerBoundByte);
        final byte[] upperKey = createIndexKey(upperBoundByte, zeroByte);
        final NavigableSet<byte[]> range = index.subSet(lowerKey, false, upperKey, true);
        for (final byte[] indexKey : range) {
            if (matchesPrimaryKey(indexKey, key)) {
                return true;
            }
        }
        return false;
    }

    public boolean isGreaterThanOrEqualIndexMatch(final NavigableSet<byte[]> index, final byte[] searchTerm,
            final byte[] key) {
        final byte[] lowerKey = createIndexKey(searchTerm, lowerBoundByte);
        final byte[] upperKey = createIndexKey(upperBoundByte, zeroByte);
        final NavigableSet<byte[]> range = index.subSet(lowerKey, true, upperKey, true);
        for (final byte[] indexKey : range) {
            if (matchesPrimaryKey(indexKey, key)) {
                return true;
            }
        }
        return false;
    }

    private int indexOfSep(final byte[] data) {
        for (int i = 0; i < data.length; i++) {
            if (data[i] == SEP)
                return i;
        }
        return -1;
    }

    private boolean containsSubarray(final byte[] array, final byte[] sub) {
        outer: for (int i = 0; i <= array.length - sub.length; i++) {
            for (int j = 0; j < sub.length; j++) {
                if (array[i + j] != sub[j]) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }

    public boolean isLikeIndexMatch(final NavigableSet<byte[]> index, final byte[] searchTerm,
            final byte[] key) {
        for (final byte[] composite : index) {
            final int sep = indexOfSep(composite);
            if (sep == -1)
                continue;

            final int keyLen = composite.length - sep - 1;
            if (keyLen <= 0)
                continue;

            final byte[] actualKey = Arrays.copyOfRange(composite, sep + 1, composite.length);

            if (containsSubarray(actualKey, searchTerm) && Arrays.equals(actualKey, key)) {
                return true;
            }
        }
        return false;
    }

    public boolean isStartsWithIndexMatch(final NavigableSet<byte[]> index, final byte[] searchTerm, final byte[] key) {
        final byte[] lowerKey = createIndexKey(searchTerm, zeroByte);
        final byte[] upperKey = createIndexKey(searchTerm, upperBoundByte);

        final NavigableSet<byte[]> range = index.subSet(lowerKey, true, upperKey, false);

        for (final byte[] indexKey : range) {
            if (matchesPrimaryKey(indexKey, key)) {
                return true;
            }
        }
        return false;
    }

    private static byte toLower(final byte b) {
        if (b >= 'A' && b <= 'Z')
            return (byte) (b + 32);
        return b;
    }

    public boolean isEndsWithIndexMatch(final NavigableSet<byte[]> index, final byte[] searchTerm,
            final byte[] primaryKey) {
        for (final byte[] entry : index) {
            if (!matchesPrimaryKey(entry, primaryKey))
                continue;

            int sepIndex = -1;
            for (int i = 0; i < entry.length; i++) {
                if (entry[i] == SEP) {
                    sepIndex = i;
                    break;
                }
            }
            if (sepIndex == -1)
                continue;

            final int fieldLen = sepIndex;
            final int suffixLen = searchTerm.length;
            if (fieldLen < suffixLen)
                continue;

            boolean match = true;
            for (int i = 0; i < suffixLen; i++) {
                final byte b1 = toLower(entry[fieldLen - suffixLen + i]);
                final byte b2 = toLower(searchTerm[i]);
                if (b1 != b2) {
                    match = false;
                    break;
                }
            }

            if (match)
                return true;
        }

        return false;
    }

    public boolean isBetweenIndexMatch(final NavigableSet<byte[]> index,
            final byte[] lowerBound, final byte[] upperBound, final byte[] key) {
        final byte[] lowerKey = createIndexKey(lowerBound, zeroByte);
        final byte[] upperKey = createIndexKey(upperBound, upperBoundByte);

        final NavigableSet<byte[]> range = index.subSet(lowerKey, true, upperKey, true);

        for (final byte[] indexKey : range) {
            if (matchesPrimaryKey(indexKey, key)) {
                return true;
            }
        }
        return false;
    }
}