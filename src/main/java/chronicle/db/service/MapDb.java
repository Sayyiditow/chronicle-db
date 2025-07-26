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
    private static final ConcurrentMap<String, SharedKeyMap> mapCache = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, SharedIndexMap> treeCache = new ConcurrentHashMap<>();
    private static final byte indexSep = 0x1F;
    private static final byte upperByte = (byte) 0xFF;

    public static class SharedKeyMap implements AutoCloseable {
        public final HTreeMap<String, String> map;
        private final AtomicInteger refCount;
        private final String filePath; // Track file path for cleanup

        SharedKeyMap(final HTreeMap<String, String> map, final String filePath) {
            this.map = map;
            this.filePath = filePath;
            this.refCount = new AtomicInteger(1);
        }

        // Increment reference count when sharing this entry
        SharedKeyMap retain() {
            refCount.incrementAndGet();
            return this;
        }

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

    public static class SharedIndexMap implements AutoCloseable {
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

        // Increment reference count when sharing this entry
        SharedIndexMap retain() {
            refCount.incrementAndGet();
            return this;
        }

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
                final HTreeMap<String, String> map = DBMaker.fileDB(filePath)
                        .allocateStartSize(128 * 1024 * 1024) // initial size
                        .allocateIncrement(48 * 1024 * 1024) // Grow by 48 MB
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
            } catch (final DBException.DataCorruption | DBException.GetVoid e) {
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
                        .allocateStartSize(64 * 1024 * 1024)
                        .allocateIncrement(32 * 1024 * 1024)
                        .closeOnJvmShutdown()
                        .fileLockDisable()
                        .fileMmapEnableIfSupported()
                        .fileMmapPreclearDisable()
                        .cleanerHackEnable()
                        .make();
                final var tree = db.treeSet("index")
                        .serializer(Serializer.BYTE_ARRAY)
                        .createOrOpen();
                return new SharedIndexMap(db, tree, filePath);
            } catch (final DBException.DataCorruption | DBException.GetVoid e) {
                CHRONICLE_UTILS.deleteFileIfExists(filePath); // let it reindex
                Logger.error("Reinitializing IndexMap at [{}]", filePath);
                throw new RuntimeException(e);
            } catch (final Exception e) {
                Logger.error("Failed to open IndexMap at [{}]", filePath);
                throw new RuntimeException(e);
            }
        });

        return entry;
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

    public int fastCount(final Iterable<String> result, final int limit) {
        int count = 0;
        for (@SuppressWarnings("unused")
        final var ignored : result) {
            count++;
            if (count == limit) {
                break;
            }
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
        return index.subSet(createLowerBoundKey(fieldBytes), true,
                createUpperBoundKey(fieldBytes), false);
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
            boolean hasNextComputed = false;
            int returned = 0;

            @Override
            public boolean hasNext() {
                if (hasNextComputed)
                    return nextValid != null;

                while (it.hasNext()) {
                    if (limit != -1 && returned >= limit)
                        break;

                    final var indexValueAndKey = extractIndexValueAndKey(it.next());
                    if (!searchTerms.contains(indexValueAndKey[0])) {
                        nextValid = indexValueAndKey[1];
                        hasNextComputed = true;
                        return true;
                    }
                }

                nextValid = null;
                hasNextComputed = true;
                return false;
            }

            @Override
            public String next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                returned++;
                hasNextComputed = false;
                return nextValid;
            }
        };

        return new SearchResult(iterable);
    }

    public SearchResult getNotInIndexSearch(final NavigableSet<byte[]> index, final Set<String> searchTerms,
            final int limit, final Set<String> excludedKeys) {
        final Iterable<String> iterable = () -> new Iterator<>() {
            final Iterator<byte[]> it = index.iterator();
            String nextValid = null;
            boolean hasNextComputed = false;
            int returned = 0;

            @Override
            public boolean hasNext() {
                if (hasNextComputed)
                    return nextValid != null;

                while (it.hasNext()) {
                    if (limit != -1 && returned >= limit)
                        break;

                    final var indexValueAndKey = extractIndexValueAndKey(it.next());
                    if (!searchTerms.contains(indexValueAndKey[0])) {
                        if (!excludedKeys.contains(indexValueAndKey[1])) {
                            nextValid = indexValueAndKey[1];
                            hasNextComputed = true;
                            return true;
                        }
                    }
                }

                nextValid = null;
                hasNextComputed = true;
                return false;
            }

            @Override
            public String next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                returned++;
                hasNextComputed = false;
                return nextValid;
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