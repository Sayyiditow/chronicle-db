package chronicle.db.utils;

import static chronicle.db.service.MapDb.MAP_DB;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.tinylog.Logger;

import com.jsoniter.JsonIterator;
import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.TypeLiteral;

import chronicle.db.dao.ChronicleDao;
import chronicle.db.entity.Search;
import chronicle.db.entity.Search.SearchType;
import chronicle.db.service.ChronicleDb.SharedChronicleMap;
import chronicle.db.service.MapDb.SharedIndexSet;
import net.openhft.chronicle.algo.hashing.LongHashFunction;
import net.openhft.chronicle.hash.locks.InterProcessDeadLockException;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.MapEntry;

/**
 * Utility class providing core functionality for ChronicleDao operations.
 * <p>
 * This singleton contains helper methods for:
 * <ul>
 * <li><b>Reflection & Field Access:</b> High-performance field access using
 * VarHandles and MethodHandles</li>
 * <li><b>Search Operations:</b> Manual filtering logic for non-indexed
 * searches</li>
 * <li><b>Index Management:</b> Building, updating, and removing secondary
 * indexes</li>
 * <li><b>CSV Operations:</b> Converting entities to CSV rows and headers</li>
 * <li><b>Data Migration:</b> Moving records between different entity
 * versions</li>
 * <li><b>File Operations:</b> File management utilities</li>
 * <li><b>Type Conversion:</b> Converting between types (enums, numbers,
 * strings)</li>
 * </ul>
 * </p>
 * 
 * <p>
 * <b>Performance Optimizations:</b>
 * <ul>
 * <li>Uses VarHandles for fast field access (faster than reflection)</li>
 * <li>Caches MethodHandles and field metadata per class</li>
 * <li>Parallel processing for batch operations</li>
 * <li>Thread-local StringBuilder for string concatenation</li>
 * </ul>
 * </p>
 * 
 * <p>
 * <b>Internal Use:</b> This class is primarily used internally by ChronicleDao.
 * Most applications should not need to interact with it directly.
 * </p>
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public final class ChronicleUtils {
    public static final ChronicleUtils CHRONICLE_UTILS = new ChronicleUtils();
    private static final int processors = Runtime.getRuntime().availableProcessors();
    private static final ExecutorService sharedExecutor = Executors.newFixedThreadPool(
            Integer.getInteger("chronicle.shared.pool.size", Math.min(processors, 32)));
    private static final ExecutorService iterableExecutor = Executors.newFixedThreadPool(
            Integer.getInteger("chronicle.iterable.pool.size", Math.min(processors, 32)));
    private static final String logDateFormat = "yyyy-MM-dd HH:mm:ss";
    private static final DateTimeFormatter logDateTimeFormatter = DateTimeFormatter.ofPattern(logDateFormat);
    private static final int flushSize = 5_242_880;
    private static final int indexChunkSize = Integer.getInteger("chronicle.indexes.chunkSize", 10000);
    // CRITICAL: Changing these seeds will invalidate all existing KeyMaps and
    // Indexes! These values must be permanent for the life of the database.
    private static final long hashSeed1 = 0L;
    private static final long hashSeed2 = 1L;

    public static class FieldData {
        final Field field;
        final VarHandle varHandle;

        FieldData(final Field field, final VarHandle varHandle) {
            this.field = field;
            this.varHandle = varHandle;
        }
    }

    /**
     * Represents a computed expression like {field1*field2}.
     * Supports operators: * (multiply), / (divide), - (subtract), + (add)
     */
    public static class ComputedExpression {
        final FieldData leftField;
        final FieldData rightField;
        final char operator;

        ComputedExpression(final FieldData leftField, final FieldData rightField, final char operator) {
            this.leftField = leftField;
            this.rightField = rightField;
            this.operator = operator;
        }

        BigDecimal evaluate(final Object value) {
            final Object leftVal = leftField.varHandle.get(value);
            final Object rightVal = rightField.varHandle.get(value);

            if (leftVal == null || rightVal == null) {
                return null;
            }

            final BigDecimal left = toBigDecimal(leftVal);
            final BigDecimal right = toBigDecimal(rightVal);

            return switch (operator) {
                case '*' -> left.multiply(right);
                case '/' -> right.signum() != 0
                        ? left.divide(right, 10, RoundingMode.HALF_UP)
                        : null;
                case '+' -> left.add(right);
                case '-' -> left.subtract(right);
                default -> null;
            };
        }

        private BigDecimal toBigDecimal(final Object val) {
            if (val instanceof final BigDecimal bd) {
                return bd;
            }
            if (val instanceof final Number n) {
                return BigDecimal.valueOf(n.doubleValue());
            }
            try {
                return new BigDecimal(String.valueOf(val));
            } catch (final NumberFormatException e) {
                return BigDecimal.ZERO;
            }
        }
    }

    public static class ClassData {
        final Map<String, FieldData> fields = new ConcurrentHashMap<>();
        final MethodHandle headerHandle;
        final MethodHandle rowHandle;
        final MethodHandle subsetHandle;
        final MethodHandle subsetRowHandle;

        ClassData(final Class<?> clazz) {
            try {
                final MethodHandles.Lookup lookup = MethodHandles.lookup();
                this.headerHandle = lookup.findVirtual(clazz, "header", MethodType.methodType(String[].class));
                this.rowHandle = lookup.findVirtual(clazz, "row", MethodType.methodType(Object[].class, String.class));
                this.subsetHandle = lookup.findVirtual(clazz, "subset",
                        MethodType.methodType(Map.class, String[].class));
                this.subsetRowHandle = lookup.findVirtual(clazz, "subsetRow",
                        MethodType.methodType(Object[].class, String.class, String[].class));
            } catch (NoSuchMethodException | IllegalAccessException e) {
                throw new RuntimeException("Failed to initialize MethodHandles for " + clazz.getSimpleName(), e);
            }
        }
    }

    private static final Map<Class<?>, ClassData> CLASS_DATA_CACHE = new ConcurrentHashMap<>();

    /**
     * Retrieves or creates cached ClassData for the given class.
     * ClassData contains MethodHandles for optimized reflection.
     *
     * @param clazz The class to retrieve data for.
     * @return The cached ClassData instance.
     */
    public ClassData getClassData(final Class<?> clazz) {
        return CLASS_DATA_CACHE.computeIfAbsent(clazz, ClassData::new);
    }

    /**
     * Retrieves or creates cached FieldData for a specific field in a class.
     * FieldData contains the Field object and its VarHandle.
     *
     * @param clazz     The class containing the field.
     * @param fieldName The name of the field.
     * @return The FieldData instance, or null if the field does not exist.
     */
    private FieldData getFieldData(final Class<?> clazz, final String fieldName) {
        final ClassData classData = getClassData(clazz);
        return classData.fields.computeIfAbsent(fieldName, f -> {
            try {
                final Field field = clazz.getField(f);
                final VarHandle varHandle = MethodHandles.lookup().unreflectVarHandle(field);
                return new FieldData(field, varHandle);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                // Don't warn for composite index (+) or OR search (|) fields
                if (!f.contains("+") && !f.contains("|")) {
                    Logger.warn("No such field [{}] in class [{}].", f, clazz.getSimpleName());
                }
                return null;
            }
        });
    }

    /**
     * Retrieve a list of files in a dirPath and throw an exception is dirPath is
     * null
     *
     * @param dirPath dirPath to retrieve files from
     * @return a list of files
     */
    public List<String> getFileList(final String dirPath) throws IOException {
        try (final var stream = Files.list(Path.of(dirPath))) {
            return stream.map(Path::getFileName).map(Path::toString).collect(Collectors.toList());
        }
    }

    public String toStringOptimized(final Object value) {
        return value instanceof final String s ? s : String.valueOf(value);
    }

    /**
     * Compares two objects. Handles Numbers specifically as Doubles, otherwise uses
     * String comparison.
     *
     * @param obj1 The first object.
     * @param obj2 The second object.
     * @return A negative integer, zero, or a positive integer as the first argument
     *         is less than, equal to, or greater than the second.
     */
    public int compare(final Object obj1, final Object obj2) {
        if (obj1 instanceof final Number n1 && obj2 instanceof final Number n2) {
            return Double.compare(n1.doubleValue(), n2.doubleValue());
        }
        return toStringOptimized(obj1).compareTo(toStringOptimized(obj2));
    }

    /**
     * Checks if a string representation of an object contains a search term,
     * ignoring case.
     *
     * @param str        The object to search within.
     * @param searchTerm The term to search for.
     * @return true if the string contains the search term, false otherwise.
     */
    public boolean containsIgnoreCase(final Object str, final Object searchTerm) {
        final String strValue = toStringOptimized(str);
        final String termValue = toStringOptimized(searchTerm);
        final int termLen = termValue.length();

        if (termLen == 0)
            return true;

        for (int i = 0; i <= strValue.length() - termLen; i++) {
            if (strValue.regionMatches(true, i, termValue, 0, termLen)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if a string representation of an object starts with a search term,
     * ignoring case.
     *
     * @param value      The object to check.
     * @param searchTerm The prefix to search for.
     * @return true if the string starts with the search term, false otherwise.
     */
    public boolean startsWithIgnoreCase(final Object value, final Object searchTerm) {
        final String str = toStringOptimized(value);
        final String term = toStringOptimized(searchTerm);
        return str.regionMatches(true, 0, term, 0, term.length());
    }

    /**
     * Checks if a string representation of an object ends with a search term,
     * ignoring case.
     *
     * @param value      The object to check.
     * @param searchTerm The suffix to search for.
     * @return true if the string ends with the search term, false otherwise.
     */
    public boolean endsWithIgnoreCase(final Object value, final Object searchTerm) {
        final String str = toStringOptimized(value);
        final String term = toStringOptimized(searchTerm);
        return str.regionMatches(true, str.length() - term.length(), term, 0, term.length());
    }

    /**
     * Converts an object value to a specific Enum type.
     *
     * @param enumClass The Enum class.
     * @param value     The value to convert (usually a String).
     * @return The Enum constant, or null if conversion fails.
     */
    public Enum toEnum(final Class<?> enumClass, final Object value) {
        try {
            return Enum.valueOf((Class<Enum>) enumClass, toStringOptimized(value));
        } catch (final Exception e) {
            return null;
        }
    }

    /**
     * Prepares a set of search terms for non-indexed search, converting types if
     * necessary.
     *
     * @param searchTerms The list of raw search terms.
     * @param fieldClass  The target field type.
     * @return A Set of converted search terms.
     */
    private Set<Object> setSearchTermNonIndexed(final Collection<Object> searchTerms, final Class<?> fieldClass) {
        final Set<Object> searchTermSet = new HashSet<>(1000);
        searchTermSet.clear();
        for (final Object searchTerm : searchTerms) {
            if (fieldClass.isEnum() && searchTerm instanceof String) {
                searchTermSet.add(toEnum(fieldClass, searchTerm));
            } else if (fieldClass == long.class
                    && (searchTerm instanceof String || searchTerm instanceof Integer)) {
                searchTermSet.add(Long.parseLong(toStringOptimized(searchTerm)));
            } else if (fieldClass == BigDecimal.class) {
                searchTermSet.add(toBigDecimal(searchTerm));
            } else {
                searchTermSet.add(searchTerm);
            }
        }
        return searchTermSet;
    }

    /**
     * Prepares a single search term for non-indexed search, converting types if
     * necessary.
     *
     * @param searchTerm The raw search term.
     * @param fieldClass The target field type.
     * @return The converted search term.
     */
    private Object setSearchTermNonIndexed(final Object searchTerm, final Class<?> fieldClass) {
        if (fieldClass.isEnum() && (searchTerm instanceof String)) {
            return toEnum(fieldClass, searchTerm);
        } else if (fieldClass == long.class && (searchTerm instanceof String || searchTerm instanceof Integer)) {
            return Long.parseLong(toStringOptimized(searchTerm));
        } else if (fieldClass == BigDecimal.class) {
            return toBigDecimal(searchTerm);
        }

        return searchTerm;
    }

    private BigDecimal toBigDecimal(final Object val) {
        if (val instanceof final BigDecimal bd) {
            return bd;
        }
        if (val instanceof final Number n) {
            return BigDecimal.valueOf(n.doubleValue());
        }
        try {
            return new BigDecimal(String.valueOf(val));
        } catch (final NumberFormatException e) {
            return BigDecimal.ZERO;
        }
    }

    /**
     * Checks if an array contains any element from a search set.
     *
     * @param array     The array to check.
     * @param searchSet The set of elements to look for.
     * @return true if any element is found, false otherwise.
     */
    private boolean containsAny(final Object[] array, final Set<Object> searchSet) {
        for (final Object obj : array) {
            if (searchSet.contains(obj)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if an array contains none of the elements from a search set.
     *
     * @param array     The array to check.
     * @param searchSet The set of elements to avoid.
     * @return true if none of the elements are found, false otherwise.
     */
    private boolean containsNone(final Object[] array, final Set<Object> searchSet) {
        for (final Object obj : array) {
            if (searchSet.contains(obj)) {
                return false; // Found one, so NOT "none"
            }
        }
        return true;
    }

    /**
     * Evaluates if a value matches a search criteria based on the search type.
     *
     * @param currentValue      The value to check.
     * @param searchTerm        The single search term (for EQUAL, LIKE, etc.).
     * @param searchTermSet     The set of search terms (for IN, CONTAINS, etc.).
     * @param searchTermBetween The range of search terms (for BETWEEN).
     * @param searchType        The type of search operation.
     * @return true if the value matches the criteria.
     */
    private boolean matchesSearch(final Object currentValue, final Object searchTerm,
            final Set<Object> searchTermSet, final List<Object> searchTermBetween,
            final SearchType searchType) {
        return switch (searchType) {
            case EQUAL -> Objects.equals(currentValue, searchTerm);
            case NOT_EQUAL -> !Objects.equals(currentValue, searchTerm);
            case LESS -> compare(currentValue, searchTerm) < 0;
            case GREATER -> compare(currentValue, searchTerm) > 0;
            case LESS_OR_EQUAL -> compare(currentValue, searchTerm) <= 0;
            case GREATER_OR_EQUAL -> compare(currentValue, searchTerm) >= 0;
            case LIKE -> containsIgnoreCase(currentValue, searchTerm);
            case NOT_LIKE -> !containsIgnoreCase(currentValue, searchTerm);
            case CONTAINS -> containsAny((Object[]) currentValue, searchTermSet);
            case NOT_CONTAINS -> containsNone((Object[]) currentValue, searchTermSet);
            case STARTS_WITH -> startsWithIgnoreCase(currentValue, searchTerm);
            case ENDS_WITH -> endsWithIgnoreCase(currentValue, searchTerm);
            case IN -> searchTermSet.contains(currentValue);
            case NOT_IN -> !searchTermSet.contains(currentValue);
            case BETWEEN -> compare(currentValue, searchTermBetween.get(0)) >= 0
                    && compare(currentValue, searchTermBetween.get(1)) <= 0;
            default -> false;
        };
    }

    private void appendValue(final StringBuilder sb, final Object objVal) {
        if (objVal instanceof final String s) {
            sb.append(s);
        } else if (objVal instanceof final Integer i) {
            sb.append(i.intValue());
        } else if (objVal instanceof final Long l) {
            sb.append(l.longValue());
        } else if (objVal instanceof final Double d) {
            sb.append(d.doubleValue());
        } else if (objVal instanceof final Boolean b) {
            sb.append(b.booleanValue());
        } else {
            sb.append(objVal);
        }
    }

    public static class PreparedSearch {
        final List<FieldData> fields;
        final SearchType searchType;
        final Object searchTerm;
        final Set<Object> searchTermSet;
        final List<Object> searchTermBetween;
        final ComputedExpression computedExpression;

        PreparedSearch(final List<FieldData> fields, final SearchType searchType, final Object searchTerm,
                final Set<Object> searchTermSet, final List<Object> searchTermBetween) {
            this(fields, searchType, searchTerm, searchTermSet, searchTermBetween, null);
        }

        PreparedSearch(final List<FieldData> fields, final SearchType searchType, final Object searchTerm,
                final Set<Object> searchTermSet, final List<Object> searchTermBetween,
                final ComputedExpression computedExpression) {
            this.fields = fields;
            this.searchType = searchType;
            this.searchTerm = searchTerm;
            this.searchTermSet = searchTermSet;
            this.searchTermBetween = searchTermBetween;
            this.computedExpression = computedExpression;
        }
    }

    /**
     * Parses a computed expression like {field1*field2}.
     * Supports operators: * / + -
     *
     * @param expression The expression without braces (e.g., "field1*field2")
     * @param valueClass The class containing the fields
     * @return ComputedExpression or null if parsing fails
     */
    private ComputedExpression parseComputedExpression(final String expression, final Class<?> valueClass) {
        final char[] operators = { '*', '/', '+', '-' };
        for (final char op : operators) {
            final int idx = expression.indexOf(op);
            if (idx > 0 && idx < expression.length() - 1) {
                final String leftFieldName = expression.substring(0, idx).trim();
                final String rightFieldName = expression.substring(idx + 1).trim();
                final FieldData leftField = getFieldData(valueClass, leftFieldName);
                final FieldData rightField = getFieldData(valueClass, rightFieldName);
                if (leftField != null && rightField != null) {
                    return new ComputedExpression(leftField, rightField, op);
                }
            }
        }
        return null;
    }

    public PreparedSearch prepareSearch(final Search search, final Class<?> valueClass) {
        final String fieldSpec = search.field();
        ComputedExpression computedExpression = null;

        // Check for computed expression syntax: {field1*field2}
        if (fieldSpec.startsWith("{") && fieldSpec.endsWith("}")) {
            final String expression = fieldSpec.substring(1, fieldSpec.length() - 1);
            computedExpression = parseComputedExpression(expression, valueClass);
            if (computedExpression != null) {
                final SearchType searchType = search.searchType();
                Object searchTerm = null;
                Set<Object> searchTermSet = null;
                List<Object> searchTermBetween = null;

                // For computed expressions, always use BigDecimal for comparison
                if (searchType == SearchType.IN || searchType == SearchType.NOT_IN) {
                    searchTermSet = setSearchTermNonIndexed((Collection<Object>) search.searchTerm(), BigDecimal.class);
                } else if (searchType == SearchType.BETWEEN) {
                    searchTermBetween = (List<Object>) search.searchTerm();
                } else {
                    searchTerm = setSearchTermNonIndexed(search.searchTerm(), BigDecimal.class);
                }

                return new PreparedSearch(Collections.emptyList(), searchType, searchTerm, searchTermSet,
                        searchTermBetween, computedExpression);
            }
        }

        // Standard field processing
        final String[] fieldNames = fieldSpec.split("\\|");
        final List<FieldData> fields = new ArrayList<>(fieldNames.length);
        Class<?> fieldType = String.class;

        for (final String fieldName : fieldNames) {
            final FieldData fd = getFieldData(valueClass, fieldName);
            if (fd != null) {
                fields.add(fd);
                fieldType = fd.field.getType();
            }
        }

        final SearchType searchType = search.searchType();
        Object searchTerm = null;
        Set<Object> searchTermSet = null;
        List<Object> searchTermBetween = null;

        if (searchType == SearchType.IN || searchType == SearchType.NOT_IN || searchType == SearchType.CONTAINS
                || searchType == SearchType.NOT_CONTAINS) {
            searchTermSet = setSearchTermNonIndexed((Collection<Object>) search.searchTerm(), fieldType);
        } else if (searchType == SearchType.BETWEEN) {
            searchTermBetween = (List<Object>) search.searchTerm();
        } else {
            searchTerm = setSearchTermNonIndexed(search.searchTerm(), fieldType);
        }

        return new PreparedSearch(fields, searchType, searchTerm, searchTermSet, searchTermBetween);
    }

    public <V> boolean search(final PreparedSearch search, final String key, final V value) {
        // Handle computed expression
        if (search.computedExpression != null) {
            final BigDecimal computedValue = search.computedExpression.evaluate(value);
            if (computedValue == null) {
                return false;
            }
            return matchesSearch(computedValue, search.searchTerm, search.searchTermSet, search.searchTermBetween,
                    search.searchType);
        }

        // Standard field search
        for (final FieldData fieldData : search.fields) {
            final Object currentValue = fieldData.varHandle.get(value);
            if (matchesSearch(currentValue, search.searchTerm, search.searchTermSet, search.searchTermBetween,
                    search.searchType)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Pre-calculates 128-bit hashes for a collection of keys.
     * This should be called BEFORE acquiring locks to reduce lock hold time.
     *
     * @param keys The collection of primary keys to hash
     * @return Map of primary key -> 16-byte hash
     */
    public Map<String, byte[]> preCalculateKeyHashes(final Iterable<String> keys) {
        if (keys == null || !keys.iterator().hasNext())
            return Collections.emptyMap();
        final var map = new ConcurrentHashMap<String, byte[]>(1000);
        try {
            parallelIterable(keys, Integer.MAX_VALUE, key -> {
                map.put(key, to128BitHash(key));
            });
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("InterruptedException while calculating key hashes.");
        }
        return map;
    }

    /**
     * Pre-calculates 128-bit hashes for keys, excluding specified keys.
     * Filters and hashes in one pass for efficiency.
     *
     * @param keys         The collection of primary keys to hash
     * @param excludedKeys Keys to skip (not hashed)
     * @return Map of primary key -> 16-byte hash (excluding excluded keys)
     */
    public Map<String, byte[]> preCalculateKeyHashes(final Iterable<String> keys, final Set<String> excludedKeys) {
        if (keys == null || !keys.iterator().hasNext())
            return Collections.emptyMap();
        if (excludedKeys == null || excludedKeys.isEmpty())
            return preCalculateKeyHashes(keys);
        final var map = new ConcurrentHashMap<String, byte[]>(1000);
        try {
            parallelIterable(keys, Integer.MAX_VALUE, key -> {
                if (!excludedKeys.contains(key)) {
                    map.put(key, to128BitHash(key));
                }
            });
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("InterruptedException while calculating key hashes.");
        }
        return map;
    }

    /**
     * Index the db so that joins for 1 to many are efficient.
     *
     * @param db              the db object being indexed
     * @param dbName          the db name
     * @param fields          the fields to index
     * @param dataPath        the data path
     * @param valueClass      the value class
     * @param exclusions      field exclusions
     * @param expectedEntries expected number of entries for allocation
     *
     */
    public <V> void index(final SharedChronicleMap<String, V> db, final String dbName, final Set<String> fields,
            final String dataPath, final Class<?> valueClass, final Map<String, Set<String>> exclusions,
            final long expectedEntries) {
        final var indexDirPath = dataPath + ChronicleDao.INDEX_DIR;

        final Map<String, List<FieldData>> indexFieldMap = new HashMap<>();
        for (final String rawField : fields) {
            final String[] parts = rawField.split("\\+");
            final List<FieldData> getters = new ArrayList<>();
            for (final String part : parts) {
                getters.add(getFieldData(valueClass, part));
            }
            if (!getters.isEmpty()) {
                indexFieldMap.put(rawField, getters);
            }
        }

        final Map<String, SharedIndexSet> openIndexes = new ConcurrentHashMap<>();
        for (final String field : indexFieldMap.keySet()) {
            final String indexPath = indexDirPath + field;
            try {
                openIndexes.put(indexPath, MAP_DB.openIndex(indexPath, expectedEntries));
            } catch (final RuntimeException e) {
                Logger.warn("Skipping indexing for [{}]: {}", indexPath, e.getMessage());
            }
        }

        if (db.map.isEmpty()) {
            Logger.info("DB is empty. Index files created at [{}].", indexDirPath);
            openIndexes.forEach((indexPath, sharedIndexSet) -> {
                sharedIndexSet.close();
            });
            return;
        }

        final var failed = new AtomicBoolean(false);
        try {
            final Map<String, List<byte[]>> fieldBatches = new ConcurrentHashMap<>();
            final AtomicInteger recordCount = new AtomicInteger(0);

            for (final String field : indexFieldMap.keySet()) {
                final String indexPath = indexDirPath + field;
                if (openIndexes.containsKey(indexPath)) {
                    fieldBatches.put(field, new ArrayList<>(indexChunkSize));
                }
            }

            final StringBuilder sb = new StringBuilder();
            try {
                safeForEachEntry(db, entry -> {
                    final var key = entry.key().get();
                    final V value = entry.value().get();
                    final byte[] keyHash = to128BitHash(key);

                    try {
                        for (final var fieldEntry : indexFieldMap.entrySet()) {
                            final String compoundField = fieldEntry.getKey();
                            final List<byte[]> batch = fieldBatches.get(compoundField);
                            if (batch == null)
                                continue;

                            final Set<String> excluded = exclusions.getOrDefault(compoundField, Collections.emptySet());
                            final List<FieldData> fieldDataList = fieldEntry.getValue();
                            sb.setLength(0);
                            boolean shouldSkip = false;

                            for (final FieldData fd : fieldDataList) {
                                final Object objVal = fd.varHandle.get(value);
                                if (excluded.isEmpty()) {
                                    appendValue(sb, objVal);
                                } else {
                                    final var val = toStringOptimized(objVal);
                                    if (excluded.contains(val)) {
                                        shouldSkip = true;
                                        break;
                                    }
                                    sb.append(val);
                                }
                            }

                            if (!shouldSkip) {
                                final byte[] indexKey = MAP_DB.createIndexKey(sb.toString(), keyHash);
                                batch.add(indexKey);
                            }
                        }

                        if (recordCount.incrementAndGet() % indexChunkSize == 0) {
                            parallelIterable(fieldBatches.entrySet(), Integer.MAX_VALUE, batchEntry -> {
                                final String field = batchEntry.getKey();
                                final List<byte[]> batch = batchEntry.getValue();
                                if (!batch.isEmpty()) {
                                    final var indexPath = indexDirPath + field;
                                    final var sharedIndexSet = openIndexes.get(indexPath);
                                    // MUST use sequential, parallel writes are an issue.
                                    batch.forEach(sharedIndexSet.index::add);
                                    batch.clear();
                                }
                            });
                        }
                    } catch (final Throwable e) {
                        Logger.error("Error processing key [{}] for fields {}", key, fields);
                        Logger.error(e);
                    }
                });
            } catch (final Exception e) {
                // Close first to release file handles, then delete for rebuild
                Logger.error("Failed to index [{}]. Deleting indexes for rebuild.", dataPath);
                failed.set(true);
                throw e;
            }

            // Flush remaining
            try {
                parallelIterable(fieldBatches.entrySet(), Integer.MAX_VALUE, batchEntry -> {
                    final String field = batchEntry.getKey();
                    final List<byte[]> batch = batchEntry.getValue();
                    if (!batch.isEmpty()) {
                        final var indexPath = indexDirPath + field;
                        final var sharedIndexSet = openIndexes.get(indexPath);
                        // MUST use sequential, parallel writes are an issue.
                        batch.forEach(sharedIndexSet.index::add);
                    }
                });
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                Logger.error("InterruptedException while indexing [{}]", dataPath);
            }
            Logger.info("Indexed [{}] records for fields: {} at [{}]", recordCount.get(), indexFieldMap.keySet(),
                    dataPath);
        } finally {
            openIndexes.forEach((indexPath, sharedIndexSet) -> {
                sharedIndexSet.close();
                if (failed.get()) {
                    deleteFileIfExists(indexPath);
                }
            });
        }
    }

    private <V> void processRemoveChunk(final List<Map.Entry<String, V>> chunk, final List<FieldData> fieldGetters,
            final Map<String, byte[]> keyHashMap, final NavigableSet<byte[]> index) {
        final List<byte[]> batchToRemove = new ArrayList<>(chunk.size());
        final StringBuilder sb = new StringBuilder();
        for (final var e : chunk) {
            final var key = e.getKey();
            final V value = e.getValue();
            sb.setLength(0);
            for (final FieldData fd : fieldGetters) {
                appendValue(sb, fd.varHandle.get(value));
            }
            batchToRemove.add(MAP_DB.createIndexKey(sb.toString(), keyHashMap.get(key)));
        }
        // MUST use sequential, parallel writes are an issue.
        batchToRemove.forEach(index::remove);
    }

    /**
     * Removes entries from secondary indexes.
     *
     * @param <V>            The type of the value object.
     * @param dbName         The name of the database.
     * @param dataPath       The path to the data directory.
     * @param indexFileNames The set of index file names to update.
     * @param values         The map of key-value pairs to remove from the index.
     * @param valueClass     The class of the value object.
     * @param keyHashMap     Pre-calculated map of primary key -> 16-byte hash
     *                       (passed from caller)
     */
    public <V> void removeFromIndex(final String dbName, final String dataPath, final Set<String> indexFileNames,
            final Map<String, V> values, final Class<?> valueClass, final Map<String, byte[]> keyHashMap) {
        if (values.isEmpty() || indexFileNames.isEmpty()) {
            return;
        }

        final Map<String, SharedIndexSet> openIndexes = new ConcurrentHashMap<>();
        try {
            // Step 1: Parse all field getters (supporting compound fields)
            final Map<String, List<FieldData>> indexFieldMap = new HashMap<>();
            for (final String indexName : indexFileNames) {
                final String[] parts = indexName.split("\\+");
                final List<FieldData> getters = new ArrayList<>();

                for (final String part : parts) {
                    getters.add(getFieldData(valueClass, part));
                }
                indexFieldMap.put(indexName, getters);
                final String indexPath = dataPath + ChronicleDao.INDEX_DIR + indexName;
                try {
                    openIndexes.put(indexPath, MAP_DB.openIndex(indexPath));
                } catch (final RuntimeException e) {
                    Logger.warn("Skipping index removal for [{}]: {}", indexPath, e.getMessage());
                }
            }

            // Step 2: Remove from each index (no chunking - process all at once)
            final List<Map.Entry<String, V>> recordList = new ArrayList<>(values.entrySet());
            final int totalRecords = recordList.size();

            indexFieldMap.forEach((indexName, fieldGetters) -> {
                final String indexPath = dataPath + ChronicleDao.INDEX_DIR + indexName;
                final var sharedIndexSet = openIndexes.get(indexPath);
                if (sharedIndexSet == null)
                    return;

                processRemoveChunk(recordList, fieldGetters, keyHashMap, sharedIndexSet.index);
                Logger.debug("Deleted [{}] indexes at [{}]", totalRecords, indexPath);
            });
        } finally {
            openIndexes.forEach((path, sharedIndexSet) -> {
                sharedIndexSet.close();
            });
        }
    }

    /**
     * Prepares the index keys to be added. Run this OUTSIDE the lock.
     *
     * @param indexFileNames The index file names.
     * @param values         The map of values to add (inserted or updated).
     * @param valueClass     The value class.
     * @param exclusions     A map of values to exclude from indexing for specific
     *                       fields.
     * @param keyHashMap     Pre-calculated map of primary key -> 16-byte hash
     *                       (passed from caller)
     * @return A map of IndexFileName -> Map of (RecordKey -> IndexKeyBytes) to add.
     */
    public <V> Map<String, Map<String, byte[]>> prepareIndexAdditions(final Set<String> indexFileNames,
            final Map<String, V> values, final Class<?> valueClass, final Map<String, Set<String>> exclusions,
            final Map<String, byte[]> keyHashMap) {
        if (values.isEmpty() || indexFileNames.isEmpty()) {
            return Collections.emptyMap();
        }

        final var indexAdditions = new ConcurrentHashMap<String, Map<String, byte[]>>();
        final var recordList = new ArrayList<>(values.entrySet());

        // Parse fields once
        final Map<String, List<FieldData>> indexFieldMap = new HashMap<>();
        for (final String indexName : indexFileNames) {
            final String[] parts = indexName.split("\\+");
            final List<FieldData> getters = new ArrayList<>();
            for (final String part : parts) {
                getters.add(getFieldData(valueClass, part));
            }
            indexFieldMap.put(indexName, getters);
        }

        // Process each index field in parallel (different indexes are independent)
        processInParallel(indexFieldMap.entrySet(), entry -> {
            final String indexName = entry.getKey();
            final List<FieldData> fieldGetters = entry.getValue();
            final Set<String> excluded = exclusions.getOrDefault(indexName, Collections.emptySet());
            final Map<String, byte[]> additions = new HashMap<>(values.size());
            final StringBuilder sb = new StringBuilder();

            // No chunking needed - just iterate through all records
            for (final var e : recordList) {
                sb.setLength(0);
                boolean skipAdd = false;
                try {
                    for (final FieldData fd : fieldGetters) {
                        final Object objVal = fd.varHandle.get(e.getValue());
                        if (excluded.isEmpty()) {
                            appendValue(sb, objVal);
                        } else {
                            final var valStr = toStringOptimized(objVal);
                            if (excluded.contains(valStr))
                                skipAdd = true;
                            sb.append(valStr);
                        }
                    }
                    if (!skipAdd) {
                        additions.put(e.getKey(), MAP_DB.createIndexKey(sb.toString(), keyHashMap.get(e.getKey())));
                    }
                } catch (final Throwable t) {
                    Logger.error(t);
                }
            }
            indexAdditions.put(indexName, additions);
        });

        return indexAdditions;
    }

    /**
     * Applies the index additions to MapDB. Run this INSIDE the lock.
     * 
     * @param dataPath       The data path.
     * @param indexAdditions The pre-calculated additions map (IndexName ->
     *                       RecordKey -> IndexBytes).
     */
    public void applyIndexAdditions(final String dataPath, final Map<String, Map<String, byte[]>> indexAdditions) {
        if (indexAdditions.isEmpty())
            return;

        final var indexDirPath = dataPath + ChronicleDao.INDEX_DIR;
        try {
            parallelIterable(indexAdditions.entrySet(), Integer.MAX_VALUE,
                    (Consumer<Map.Entry<String, Map<String, byte[]>>>) entry -> {
                        final String indexName = entry.getKey();
                        final Map<String, byte[]> keysToAddMap = entry.getValue();
                        final String indexPath = indexDirPath + indexName;

                        if (keysToAddMap.isEmpty())
                            return;

                        try (final var sharedIndex = MAP_DB.openIndex(indexPath)) {
                            // MUST use sequential, parallel writes are an issue.
                            keysToAddMap.values().forEach(sharedIndex.index::add);
                            Logger.debug("Inserted [{}] indexes at [{}]", keysToAddMap.size(), indexPath);
                        }
                    });
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Applies the index updates to MapDB using pre-calculated additions.
     * Run this INSIDE the lock.
     *
     * @param dataPath       The data path.
     * @param indexFileNames The set of index file names to update.
     * @param preparedAdds   The pre-calculated new index keys (from
     *                       prepareIndexAdditions).
     * @param previousValues The map of previous values (before the update).
     * @param valueClass     The value class.
     * @param exclusions     A map of values to exclude from indexing.
     * @param keyHashMap     Pre-calculated map of primary key -> 16-byte hash
     *                       (passed from caller)
     */
    public <V> void applyIndexUpdates(final String dataPath, final Set<String> indexFileNames,
            final Map<String, Map<String, byte[]>> preparedAdds, final Map<String, V> previousValues,
            final Class<?> valueClass, final Map<String, Set<String>> exclusions,
            final Map<String, byte[]> keyHashMap) {
        if (previousValues.isEmpty() || indexFileNames.isEmpty()) {
            return;
        }

        final var indexDirPath = dataPath + ChronicleDao.INDEX_DIR;
        final var prevEntries = new ArrayList<>(previousValues.entrySet());
        final int size = prevEntries.size();

        // Process each index in parallel using ITERABLE_EXECUTOR
        try {
            parallelIterable(indexFileNames, Integer.MAX_VALUE, (Consumer<String>) indexName -> {
                final String indexPath = indexDirPath + indexName;
                final Map<String, byte[]> newKeysMap = preparedAdds.getOrDefault(indexName, Collections.emptyMap());
                final List<FieldData> fieldGetters = new ArrayList<>();
                for (final String part : indexName.split("\\+")) {
                    fieldGetters.add(getFieldData(valueClass, part));
                }
                final Set<String> excluded = exclusions.getOrDefault(indexName, Collections.emptySet());

                try (final var sharedIndex = MAP_DB.openIndex(indexPath)) {
                    final List<byte[]> toRemove = new ArrayList<>();
                    final List<byte[]> toAdd = new ArrayList<>();
                    final StringBuilder sb = new StringBuilder();

                    // Process in chunks to avoid huge lists
                    for (int i = 0; i < size; i += indexChunkSize) {
                        final var chunk = prevEntries.subList(i, Math.min(i + indexChunkSize, size));
                        toRemove.clear();
                        toAdd.clear();

                        for (final var entry : chunk) {
                            final String key = entry.getKey();
                            final V prevVal = entry.getValue();
                            final byte[] keyHash = keyHashMap.get(key);

                            // New Key (Fast Lookup)
                            final byte[] newIndexKey = newKeysMap.get(key);

                            // Old Key (Must Calculate)
                            byte[] oldIndexKey = null;
                            sb.setLength(0);
                            boolean skipOld = false;
                            try {
                                for (final FieldData fd : fieldGetters) {
                                    final Object objVal = fd.varHandle.get(prevVal);
                                    if (excluded.isEmpty()) {
                                        appendValue(sb, objVal);
                                    } else {
                                        final var valStr = toStringOptimized(objVal);
                                        if (excluded.contains(valStr))
                                            skipOld = true;
                                        sb.append(valStr);
                                    }
                                }
                                if (!skipOld) {
                                    oldIndexKey = MAP_DB.createIndexKey(sb.toString(), keyHash);
                                }
                            } catch (final Throwable t) {
                                Logger.error(t);
                            }

                            // Compare and Update
                            if (!Arrays.equals(oldIndexKey, newIndexKey)) {
                                if (oldIndexKey != null)
                                    toRemove.add(oldIndexKey);
                                if (newIndexKey != null)
                                    toAdd.add(newIndexKey);
                            }
                        }

                        // Flush Chunk
                        if (!toRemove.isEmpty() || !toAdd.isEmpty()) {
                            // MUST use sequential, parallel writes are an issue.
                            if (!toRemove.isEmpty()) {
                                toRemove.forEach(sharedIndex.index::remove);
                                Logger.debug("Deleted [{}] indexes at [{}]", toRemove.size(), indexPath);
                            }
                            if (!toAdd.isEmpty()) {
                                toAdd.forEach(sharedIndex.index::add);
                                Logger.debug("Inserted [{}] indexes at [{}]", toAdd.size(), indexPath);
                            }
                        }
                    }
                }
            });
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Retrieves CSV headers from an object using reflection (via MethodHandle).
     *
     * @param <V>         The type of the value object.
     * @param classData   The cached ClassData for the object type.
     * @param sampleValue A sample object instance.
     * @return An array of header strings.
     */
    public <V> String[] getHeadersFromObject(final ClassData classData, final V sampleValue) {
        try {
            final MethodHandle headersMethod = classData.headerHandle;
            return (String[]) headersMethod.invoke(sampleValue);
        } catch (final Throwable e) {
            // should not happen
            Logger.error("Error when getting headers from object.");
            Logger.error(e);
            return new String[0];
        }
    }

    /**
     * Retrieves a CSV row from an object using reflection (via MethodHandle).
     *
     * @param <V>         The type of the value object.
     * @param classData   The cached ClassData for the object type.
     * @param key         The key associated with the object.
     * @param sampleValue The object instance.
     * @return An array of objects representing the row data.
     */
    public <V> Object[] getRowFromObject(final ClassData classData, final String key, final V sampleValue) {
        try {
            final MethodHandle rowMethod = classData.rowHandle;
            return (Object[]) rowMethod.invoke(sampleValue, key);
        } catch (final Throwable e) {
            // should not happen
            Logger.error("Error when getting row from object.");
            Logger.error(e);
            return new Object[0];
        }
    }

    /**
     * Retrieves a subset of fields from an object as a Map.
     *
     * @param <V>         The type of the value object.
     * @param classData   The cached ClassData for the object type.
     * @param fields      The list of fields to retrieve.
     * @param sampleValue The object instance.
     * @return A Map containing the requested fields.
     */
    public <V> Map<String, Object> getSubsetFromObject(final ClassData classData, final String[] fields,
            final V sampleValue) {
        try {
            final MethodHandle rowMethod = classData.subsetHandle;
            return (Map<String, Object>) rowMethod.invoke(sampleValue, fields);
        } catch (final Throwable e) {
            // should not happen
            Logger.error("Error when getting subset from object.");
            Logger.error(e);
            return Collections.emptyMap();
        }
    }

    /**
     * Retrieves a subset of fields from an object as a row array.
     *
     * @param <V>         The type of the value object.
     * @param classData   The cached ClassData for the object type.
     * @param key         The key associated with the object.
     * @param fields      The list of fields to retrieve.
     * @param sampleValue The object instance.
     * @return An array of objects representing the subset row data.
     */
    public <V> Object[] getSubsetRowFromObject(final ClassData classData, final String key, final String[] fields,
            final V sampleValue) {
        try {
            final MethodHandle rowMethod = classData.subsetRowHandle;
            return (Object[]) rowMethod.invoke(sampleValue, key, fields);
        } catch (final Throwable e) {
            // should not happen
            Logger.error("Error when getting subset row from object [{}].", sampleValue.getClass().getSimpleName());
            Logger.error(e);
            return new Object[0];
        }
    }

    /**
     * Generates CSV headers including an "ID" column.
     *
     * @param fields The list of field names.
     * @return An array of header strings starting with "ID".
     */
    public String[] getCsvHeaders(final String[] fields) {
        final String[] headers = new String[fields.length + 1];
        headers[0] = "ID";
        System.arraycopy(fields, 0, headers, 1, fields.length);
        return headers;
    }

    /**
     * Sets a non-enum value on an object field using VarHandle.
     *
     * @param <V>        The type of the object.
     * @param object     The object instance.
     * @param fieldName  The name of the field to set.
     * @param fieldValue The value to set.
     * @throws Throwable If the field access fails.
     */
    public <V> void setNonEnumValue(final V object, final String fieldName, final Object fieldValue)
            throws Throwable {
        final var fieldData = getFieldData(object.getClass(), fieldName);
        if (fieldData != null)
            fieldData.varHandle.set(object, fieldValue);
    }

    /**
     * Sets a value on an object field, handling Enum conversion if necessary.
     *
     * @param <V>        The type of the object.
     * @param object     The object instance.
     * @param fieldName  The name of the field to set.
     * @param fieldValue The value to set.
     * @throws Throwable If the field access fails.
     */
    public <V> void setObjectValue(final V object, final String fieldName, final Object fieldValue)
            throws Throwable {
        final var fieldData = getFieldData(object.getClass(), fieldName);

        if (fieldData != null) {
            final var type = fieldData.field.getType();
            if (type.isEnum())
                fieldData.varHandle.set(object, toEnum(type, fieldValue));
            else
                fieldData.varHandle.set(object, fieldValue);
        }
    }

    /**
     * Appends a string value to an existing string field on an object.
     *
     * @param <V>        The type of the object.
     * @param object     The object instance.
     * @param fieldName  The name of the field to update.
     * @param fieldValue The string value to append.
     * @throws Throwable If the field access fails.
     */
    public <V> void concatenateObjectValue(final V object, final String fieldName, final String fieldValue)
            throws Throwable {
        final var fieldData = getFieldData(object.getClass(), fieldName);
        if (fieldData != null) {
            final var value = (String) fieldData.varHandle.get(object);
            fieldData.varHandle.set(object, value + fieldValue);
        }
    }

    /**
     * Replaces a substring within a string field on an object.
     *
     * @param <V>        The type of the object.
     * @param object     The object instance.
     * @param fieldName  The name of the field to update.
     * @param fieldValue The replacement string.
     * @param toReplace  The substring to be replaced.
     * @throws Throwable If the field access fails.
     */
    public <V> void replaceObjectValue(final V object, final String fieldName, final String fieldValue,
            final String toReplace) throws Throwable {
        final var fieldData = getFieldData(object.getClass(), fieldName);
        if (fieldData != null) {
            final var value = ((String) fieldData.varHandle.get(object)).replace(toReplace, fieldValue);
            fieldData.varHandle.set(object, value);
        }
    }

    /**
     * Deletes a file if it exists, logging a message if it does not.
     *
     * @param filePath The path to the file.
     */
    public void deleteFileIfExists(final String filePath) {
        try {
            Files.delete(Path.of(filePath));
        } catch (final IOException e) {
            Logger.info("File for deletion does not exist [{}].", filePath);
        }
    }

    /**
     * Moves a file from source to destination, replacing existing files.
     *
     * @param source The source path.
     * @param dest   The destination path.
     */
    public void move(final Path source, final Path dest) {
        try {
            Files.move(source, dest, REPLACE_EXISTING);
        } catch (final IOException e) {
            Logger.error("Error moving from [{}]  to [{}]. {}", source, dest, e);
        }
    }

    /**
     * Moves contents of a directory that start with a specific prefix to a
     * destination directory.
     *
     * @param src  The source directory.
     * @param dest The destination directory.
     * @throws IOException If an I/O error occurs.
     */
    public void moveDirContents(final Path src, final Path dest)
            throws IOException {
        Files.walk(src).filter(path -> !path.equals(src))
                .forEach(source -> move(source, dest.resolve(src.relativize(source))));
    }

    /**
     * Moves contents of a directory that start with a specific prefix to a
     * destination directory.
     *
     * @param src        The source directory.
     * @param dest       The destination directory.
     * @param filePrefix The file name prefix to filter by.
     * @throws IOException If an I/O error occurs.
     */
    public void moveDirContentsStartsWith(final Path src, final Path dest, final String filePrefix)
            throws IOException {
        Files.walk(src).filter(path -> !path.equals(src))
                .filter(path -> path.getFileName().toString().startsWith(filePrefix))
                .forEach(source -> move(source, dest.resolve(src.relativize(source))));
    }

    /**
     * Migrate records from one object version to another
     * Usefule when adding/removing fields
     * 
     * @throws InvocationTargetException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws SecurityException
     * @throws NoSuchMethodException
     * @throws ClassNotFoundException
     */
    public <V> Map<String, Object> moveRecords(final ChronicleMap<String, V> currentValues,
            final String fromObjectClass, final String toObjectClass, final Map<String, String> move,
            final Map<String, Object> def)
            throws SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, ClassNotFoundException {
        if (currentValues.isEmpty())
            return new HashMap<>(); // Early exit

        final Map<String, Object> map = new HashMap<>(currentValues.size()); // Pre-size map
        final Class<?> sourceCls = Class.forName(fromObjectClass);
        final Class<?> cls = Class.forName(toObjectClass);
        final Constructor<?> constructor = cls.getConstructor();
        final Field[] fields = sourceCls.getDeclaredFields();
        final Field[] newFields = cls.getDeclaredFields();
        final Set<Field> newFieldsSet = new HashSet<>(Arrays.asList(newFields)); // Faster lookup
        newFieldsSet.removeAll(Arrays.asList(fields));

        // --- Optimization: Pre-compute field mappings with VarHandles ---
        class FieldTransfer {
            final VarHandle srcHandle;
            final VarHandle destHandle;
            final Class<?> destType;
            final Object defValue;
            final boolean isDestEnum;

            FieldTransfer(final Field src, final Field dest, final Object defValue) throws IllegalAccessException {
                this.srcHandle = MethodHandles.lookup().unreflectVarHandle(src);
                this.destHandle = MethodHandles.lookup().unreflectVarHandle(dest);
                this.destType = dest.getType();
                this.defValue = defValue;
                this.isDestEnum = destType.isEnum();
            }
        }

        class DefaultTransfer {
            final VarHandle destHandle;
            final Class<?> destType;
            final Object value;

            DefaultTransfer(final Field dest, final Object value) throws IllegalAccessException {
                this.destHandle = MethodHandles.lookup().unreflectVarHandle(dest);
                this.destType = dest.getType();
                this.value = destType.isEnum() ? toEnum(destType, value) : value;
            }
        }

        final List<FieldTransfer> transfers = new ArrayList<>(fields.length);
        for (final Field field : fields) {
            final String fieldName = field.getName();
            final String destFieldName = move.getOrDefault(fieldName, fieldName);
            final Object defValue = def.get(fieldName);

            try {
                final Field destField = cls.getDeclaredField(destFieldName);
                transfers.add(new FieldTransfer(field, destField, defValue));
            } catch (final NoSuchFieldException e) {
                // Destination field doesn't exist, skip
            }
        }

        final List<DefaultTransfer> defaults = new ArrayList<>();
        for (final Field field : newFieldsSet) {
            final Object defValue = def.get(field.getName());
            if (defValue != null) {
                defaults.add(new DefaultTransfer(field, defValue));
            }
        }
        // ------------------------------------------------

        currentValues.forEachEntry(entry -> {
            try {
                final var key = entry.key().get();
                final V currentVal = entry.value().get();
                final Object newObj = constructor.newInstance();

                // Apply transfers
                for (final FieldTransfer ft : transfers) {
                    final Object fieldVal = ft.srcHandle.get(currentVal);
                    final Object value = ft.defValue != null
                            ? (ft.isDestEnum ? toEnum(ft.destType, ft.defValue) : ft.defValue)
                            : (ft.isDestEnum && fieldVal != null ? toEnum(ft.destType, fieldVal) : fieldVal);
                    ft.destHandle.set(newObj, value);
                }

                // Apply defaults
                for (final DefaultTransfer dt : defaults) {
                    dt.destHandle.set(newObj, dt.value);
                }

                map.put(key, newObj);
            } catch (final IllegalArgumentException | IllegalAccessException
                    | InstantiationException | InvocationTargetException e) {
                Logger.error("Error during migration for [{}]", toObjectClass);
                Logger.error(e);
            }
        });

        return map;
    }

    /**
     * Limits a Map to a specified number of entries.
     *
     * @param <K>        The type of keys.
     * @param <V>        The type of values.
     * @param sourceData The source map.
     * @param limit      The maximum number of entries.
     * @return A new Map containing at most 'limit' entries.
     */
    public <K, V> Map<K, V> limitMapValues(final Map<K, V> sourceData, final int limit) {
        if (sourceData.size() <= limit) {
            return sourceData;
        }

        final var limitedMap = new HashMap<K, V>(limit);
        int count = 0;
        for (final var entry : sourceData.entrySet()) {
            if (count >= limit) {
                break;
            }
            limitedMap.put(entry.getKey(), entry.getValue());
            count++;
        }
        return limitedMap;
    }

    /**
     * Limits a Set to a specified number of entries.
     *
     * @param <K>        The type of elements.
     * @param sourceData The source set.
     * @param limit      The maximum number of entries.
     * @return A new Set containing at most 'limit' entries.
     */
    public <K> Set<K> limitSetValues(final Set<K> sourceData, final int limit) {
        if (sourceData.size() <= limit) {
            return sourceData;
        }

        final var limitedSet = new HashSet<K>(limit);
        int count = 0;
        for (final var key : sourceData) {
            if (count >= limit) {
                break;
            }
            limitedSet.add(key);
            count++;
        }
        return limitedSet;
    }

    /**
     * Limits a List to a specified number of entries.
     *
     * @param <K>        The type of elements.
     * @param sourceData The source list.
     * @param limit      The maximum number of entries.
     * @return A new List containing at most 'limit' entries.
     */
    public <K> List<K> limitListValues(final List<K> sourceData, final int limit) {
        if (sourceData.size() <= limit) {
            return sourceData;
        }

        final var limitedList = new ArrayList<K>(limit);
        int count = 0;
        for (final var key : sourceData) {
            if (count >= limit) {
                break;
            }
            limitedList.add(key);
            count++;
        }
        return limitedList;
    }

    /**
     * Processes an Iterable in parallel using a shared executor service.
     *
     * @param <T>          The type of elements.
     * @param iterable     The iterable to process.
     * @param limit        The maximum number of items to process.
     * @param matchCounter An atomic counter to track processed items.
     * @param action       The predicate action to perform on each item.
     * @throws InterruptedException If the thread is interrupted.
     */
    public <T> void parallelIterable(final Iterable<T> iterable, final int limit, final AtomicInteger matchCounter,
            final Predicate<T> action) throws InterruptedException {
        if (limit <= 0 || iterable == null) {
            return;
        }

        final var iterator = iterable.iterator();
        final var iteratorLock = new ReentrantLock();
        final var batchSize = 100;
        // Use shared executor instead of creating new one
        final int consumerThreads = processors;
        final var futures = new ArrayList<Future<?>>(consumerThreads);

        for (int i = 0; i < consumerThreads; i++) {
            futures.add(iterableExecutor.submit(() -> {
                final var batch = new ArrayList<T>(batchSize);

                while (matchCounter.get() < limit && !Thread.currentThread().isInterrupted()) {
                    // Fetch batch
                    batch.clear();
                    iteratorLock.lock();
                    try {
                        if (!iterator.hasNext()) {
                            return; // No more data
                        }
                        for (int j = 0; j < batchSize && iterator.hasNext(); j++) {
                            batch.add(iterator.next());
                        }
                    } finally {
                        iteratorLock.unlock();
                    }

                    // Process batch
                    for (final T item : batch) {
                        if (matchCounter.get() >= limit) {
                            return; // Early exit
                        }

                        try {
                            if (action.test(item)) {
                                matchCounter.incrementAndGet();
                                // Note: May slightly exceed limit, acceptable for performance
                            }
                        } catch (final Exception e) {
                            Logger.error("[Parallel Iterable] - Error processing item");
                            Logger.error(e);
                        }
                    }
                }
            }));
        }

        // Wait for all tasks to complete
        for (final Future<?> future : futures) {
            try {
                future.get();
            } catch (final ExecutionException e) {
                Logger.error("[Parallel Iterable] - Consumer thread failed");
                Logger.error(e);
            } catch (final CancellationException e) {
                // Expected if cancelled
            }
        }
    }

    /**
     * Processes an Iterable in parallel using a shared executor service.
     *
     * @param iterable The iterable to process.
     * @param limit    The maximum number of items to process.
     * @param action   The predicate action to perform on each item.
     * @throws InterruptedException If the thread is interrupted.
     */
    public <T> void parallelIterable(final Iterable<T> iterable, final int limit, final Predicate<T> action)
            throws InterruptedException {
        final AtomicInteger matchCounter = new AtomicInteger(0);
        parallelIterable(iterable, limit, matchCounter, action);
    }

    /**
     * Processes an Iterable in parallel using a shared executor service.
     *
     * @param <T>          The type of elements.
     * @param iterable     The iterable to process.
     * @param limit        The maximum number of items to process.
     * @param matchCounter An atomic counter to track processed items.
     * @param action       The consumer action to perform on each item.
     * @throws InterruptedException If the thread is interrupted.
     */
    public <T> void parallelIterable(final Iterable<T> iterable, final int limit, final AtomicInteger matchCounter,
            final Consumer<T> action)
            throws InterruptedException {
        if (limit <= 0 || iterable == null) {
            return;
        }

        final var iterator = iterable.iterator();
        final var iteratorLock = new ReentrantLock();
        final var batchSize = 100;
        // Use shared executor instead of creating new one
        final int consumerThreads = processors;
        final var futures = new ArrayList<Future<?>>(consumerThreads);

        for (int i = 0; i < consumerThreads; i++) {
            futures.add(iterableExecutor.submit(() -> {
                final var batch = new ArrayList<T>(batchSize);
                while (matchCounter.get() < limit && !Thread.currentThread().isInterrupted()) {

                    // 1. Fetch Batch (Inside Lock)
                    batch.clear();
                    iteratorLock.lock();
                    try {
                        if (!iterator.hasNext())
                            return;
                        for (int j = 0; j < batchSize && iterator.hasNext(); j++) {
                            batch.add(iterator.next());
                        }
                    } finally {
                        iteratorLock.unlock();
                    }

                    // 2. Process Batch (Outside Lock)
                    for (final T item : batch) {
                        if (matchCounter.get() >= limit)
                            return;

                        try {
                            action.accept(item); // The "Runnable-style" call
                            matchCounter.incrementAndGet();
                        } catch (final Exception e) {
                            Logger.error("Error processing item");
                            Logger.error(e);
                        }
                    }
                }
            }));
        }

        // Wait for all tasks to complete
        for (final Future<?> future : futures) {
            try {
                future.get();
            } catch (final ExecutionException e) {
                Logger.error("[Parallel Iterable] - Consumer thread failed");
                Logger.error(e);
            } catch (final CancellationException e) {
                // Expected if cancelled
            }
        }
    }

    /**
     * Processes an Iterable in parallel using a shared executor service.
     *
     * @param iterable The iterable to process.
     * @param limit    The maximum number of items to process.
     * @param action   The predicate action to perform on each item.
     * @throws InterruptedException If the thread is interrupted.
     */
    public <T> void parallelIterable(final Iterable<T> iterable, final int limit, final Consumer<T> action)
            throws InterruptedException {
        final AtomicInteger matchCounter = new AtomicInteger(0);
        parallelIterable(iterable, limit, matchCounter, action);
    }

    /**
     * Concatenates two Iterables into a single Iterable, limited by a maximum
     * count.
     *
     * @param <T>    The type of elements.
     * @param first  The first iterable.
     * @param second The second iterable.
     * @param limit  The maximum number of elements to return.
     * @return A concatenated Iterable.
     */
    public <T> Iterable<T> concatIterable(final Iterable<T> first, final Iterable<T> second, final int limit) {
        return () -> new Iterator<>() {
            private final Iterator<T> firstIterator = first.iterator();
            private final Iterator<T> secondIterator = second.iterator();
            private int remaining = limit != -1 ? limit : Integer.MAX_VALUE;

            @Override
            public boolean hasNext() {
                return remaining > 0 && (firstIterator.hasNext() || secondIterator.hasNext());
            }

            @Override
            public T next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                remaining--;
                return firstIterator.hasNext() ? firstIterator.next() : secondIterator.next();
            }
        };
    }

    /**
     * Executes a list of Runnable tasks in parallel using Virtual Threads.
     *
     * @param tasks The list of tasks to execute.
     */
    public void processInParallel(final Collection<Runnable> tasks) {
        if (tasks == null || tasks.isEmpty()) {
            return;
        }
        final List<Future<?>> futures = new ArrayList<>(tasks.size());
        for (final Runnable task : tasks) {
            if (task != null) {
                futures.add(sharedExecutor.submit(task));
            }
        }
        for (final Future<?> future : futures) {
            try {
                future.get();
            } catch (final Exception e) {
                Logger.error(e);
            }
        }
    }

    public <T> void processInParallel(final Collection<T> items, final Consumer<T> action) {
        if (items == null || items.isEmpty())
            return;
        final List<Future<?>> futures = new ArrayList<>(items.size());
        for (final T item : items) {
            futures.add(sharedExecutor.submit(() -> action.accept(item)));
        }
        for (final Future<?> future : futures) {
            try {
                future.get();
            } catch (final Exception e) {
                Logger.error(e);
            }
        }
    }

    public void doWithLock(final ReentrantLock lock, final Runnable action) {
        lock.lock();
        try {
            action.run();
        } finally {
            lock.unlock();
        }
    }

    public <V> V doWithLock(final ReentrantLock lock, final Supplier<V> action) {
        lock.lock();
        try {
            return action.get();
        } finally {
            lock.unlock();
        }
    }

    public <K> void doWithLock(final ConcurrentMap<K, ReentrantLock> LOCKS, final K lockKey,
            final Runnable action) {
        final var lock = LOCKS.computeIfAbsent(lockKey, k -> new ReentrantLock(true));
        lock.lock();
        try {
            action.run();
        } finally {
            lock.unlock();
        }
    }

    public <K, V> V doWithLock(final ConcurrentMap<K, ReentrantLock> LOCKS, final K lockKey,
            final Supplier<V> action) {
        final var lock = LOCKS.computeIfAbsent(lockKey, k -> new ReentrantLock(true));
        lock.lock();
        try {
            return action.get();
        } finally {
            lock.unlock();
        }
    }

    public void doWithTryLock(final ReentrantLock lock, final Runnable action, final String ifLockedMsg) {
        if (!lock.tryLock()) {
            Logger.info(ifLockedMsg);
            return;
        }
        try {
            action.run();
        } finally {
            lock.unlock();
        }
    }

    public <V> V doWithTryLock(final ReentrantLock lock, final Supplier<V> action, final String ifLockedMsg,
            final V ifLockedReturn) {
        if (!lock.tryLock()) {
            Logger.info(ifLockedMsg);
            return ifLockedReturn;
        }
        try {
            return action.get();
        } finally {
            lock.unlock();
        }
    }

    public void logProcessId(final String fileName) throws IOException {
        final var pid = ProcessHandle.current().pid();
        Logger.info("Process ID for this task: {}", pid);
        Files.writeString(Path.of(fileName), String.valueOf(pid));
    }

    public String getCurrentDate() {
        return LocalDateTime.ofInstant(new Date().toInstant(), ZoneId.systemDefault()).format(logDateTimeFormatter);
    }

    /**
     * Reads a JSON file and converts it to a specific Java object type.
     * 
     * @param path        The path to the JSON file
     * @param typeLiteral The Jsoniter TypeLiteral representing the target type
     * @param <T>         The type of the target object
     * @return The deserialized object
     * @throws IOException If an I/O error occurs
     */
    public <T> T fromJsonFileToObj(final String path, final TypeLiteral<T> typeLiteral) throws IOException {
        return JsonIterator.deserialize(Files.readAllBytes(Path.of(path)), typeLiteral);
    }

    /**
     * Saves a Java object to a file as JSON.
     * 
     * @param path The full file path
     * @param prop The Java object to save
     * @param <T>  The type of the object
     * @throws IOException If an I/O error occurs
     */
    public <T> void toJsonFileFromObj(final String path, final T prop) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(path); JsonStream stream = new JsonStream(fos, flushSize)) {
            stream.writeVal(prop);
            stream.flush();
        }
    }

    public String humanReadableByteCount(final long bytes) {
        if (bytes < 1024)
            return bytes + "B";
        final int exp = (int) (Math.log(bytes) / Math.log(1024));
        final String pre = "KMGTPE".charAt(exp - 1) + "";
        return String.format("%.1f%sB", bytes / Math.pow(1024, exp), pre);
    }

    public byte[] to128BitHash(final String key) {
        if (key == null)
            return new byte[16];

        // 1. Force conversion to a stable byte array (UTF-8)
        // This bypasses Java's internal String memory optimization (Compact Strings)
        final byte[] bytes = key.getBytes(StandardCharsets.UTF_8);

        // 2. Hash the BYTES, not the CHARS
        final long h1 = LongHashFunction.xx_r39(hashSeed1).hashBytes(bytes);
        final long h2 = LongHashFunction.xx_r39(hashSeed2).hashBytes(bytes);

        final byte[] result = new byte[16];

        // Unroll the loop manually for maximum speed
        // High 64 bits (h1)
        result[0] = (byte) (h1 >>> 56);
        result[1] = (byte) (h1 >>> 48);
        result[2] = (byte) (h1 >>> 40);
        result[3] = (byte) (h1 >>> 32);
        result[4] = (byte) (h1 >>> 24);
        result[5] = (byte) (h1 >>> 16);
        result[6] = (byte) (h1 >>> 8);
        result[7] = (byte) h1;

        // Low 64 bits (h2)
        result[8] = (byte) (h2 >>> 56);
        result[9] = (byte) (h2 >>> 48);
        result[10] = (byte) (h2 >>> 40);
        result[11] = (byte) (h2 >>> 32);
        result[12] = (byte) (h2 >>> 24);
        result[13] = (byte) (h2 >>> 16);
        result[14] = (byte) (h2 >>> 8);
        result[15] = (byte) h2;

        return result;
    }

    /**
     * Safely iterates over map entries, marking for recovery if deadlock is
     * detected.
     * Recovery happens automatically when all references are closed.
     */
    public <V> void safeForEachEntry(final SharedChronicleMap<String, V> sharedChronicleMap,
            final Consumer<MapEntry<String, V>> action) {
        try {
            sharedChronicleMap.map.forEachEntry(action);
        } catch (final InterProcessDeadLockException e) {
            Logger.error("Deadlock detected on [{}]. Marked for recovery on close.", sharedChronicleMap.getFilePath());
            sharedChronicleMap.markForRecovery();
            throw e; // Let caller know operation failed
        }
    }
}
