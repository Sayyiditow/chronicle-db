package chronicle.db.service;

import static chronicle.db.service.ChronicleDb.CHRONICLE_DB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import chronicle.db.dao.ChronicleDao;
import chronicle.db.entity.PutStatus;
import chronicle.db.entity.Search;
import chronicle.db.entity.Search.SearchType;

/**
 * Comprehensive tests for ChronicleDao methods.
 * Tests both single-file (no keymap) and multi-file (with keymap) scenarios.
 */
@SuppressWarnings("unchecked")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ChronicleDaoTest {
    private static final String DAO = "chronicle.db.service.LeadDao";
    private static final String SINGLE_FILE_PATH = "src/test/.data/single/";
    private static final String MULTI_FILE_PATH = "src/test/.data/multi/";

    private static ChronicleDao<Lead> singleFileDao;
    private static ChronicleDao<Lead> multiFileDao;

    @BeforeAll
    static void setup() throws Throwable {
        singleFileDao = CHRONICLE_DB.getChronicleDao(DAO, SINGLE_FILE_PATH);
        multiFileDao = CHRONICLE_DB.getChronicleDao(DAO, MULTI_FILE_PATH);
    }

    private static Lead createLead(final String name, final String email) {
        return new Lead(name, "linkedin-" + name, "fb-" + name, "twitter-" + name,
                email, "012-" + name, "Engineer", "Location-" + name, new ArrayList<>());
    }

    // ==================== SINGLE FILE DAO TESTS ====================

    @Test
    @Order(1)
    void singleFile_insertAndGet() {
        final String key = "single-test-1";
        final Lead lead = createLead("SingleTest1", "single1@test.com");

        singleFileDao.put(key, lead);

        final Lead retrieved = singleFileDao.get(key);
        assertNotNull(retrieved);
        assertEquals("SingleTest1", retrieved.fullName);
        assertEquals("single1@test.com", retrieved.email);
    }

    @Test
    @Order(2)
    void singleFile_size() {
        // Ensure idempotent across runs: drop the key if it was inserted by a
        // prior run, otherwise put() upserts and size stays unchanged.
        singleFileDao.delete("single-size-test");
        final int initialSize = singleFileDao.size();

        singleFileDao.put("single-size-test", createLead("SizeTest", "size@test.com"));

        assertEquals(initialSize + 1, singleFileDao.size());
    }

    @Test
    @Order(3)
    void singleFile_exists() {
        final String existingKey = "single-exists-test";
        singleFileDao.put(existingKey, createLead("ExistsTest", "exists@test.com"));

        assertTrue(singleFileDao.exists(existingKey));
        assertFalse(singleFileDao.exists("non-existent-key-12345"));
    }

    @Test
    @Order(4)
    void singleFile_existsCollection() {
        final List<String> insertedKeys = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final String key = "single-exists-coll-" + i;
            singleFileDao.put(key, createLead("ExistsColl" + i, "ec" + i + "@test.com"));
            insertedKeys.add(key);
        }

        final List<String> testKeys = new ArrayList<>(insertedKeys);
        testKeys.add("non-existent-1");
        testKeys.add("non-existent-2");

        final Map<String, Boolean> result = singleFileDao.exists(testKeys);

        assertEquals(testKeys.size(), result.size());
        for (final String key : insertedKeys) {
            assertTrue(result.get(key), "Key should exist: " + key);
        }
        assertFalse(result.get("non-existent-1"));
        assertFalse(result.get("non-existent-2"));
    }

    @Test
    @Order(5)
    void singleFile_existsList() {
        final List<String> insertedKeys = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            final String key = "single-existslist-" + i;
            singleFileDao.put(key, createLead("ExistsList" + i, "el" + i + "@test.com"));
            insertedKeys.add(key);
        }

        final List<String> testKeys = new ArrayList<>(insertedKeys);
        testKeys.add("fake-key-1");
        testKeys.add("fake-key-2");

        final List<String> existingKeys = singleFileDao.existsList(testKeys);

        assertEquals(insertedKeys.size(), existingKeys.size());
        assertTrue(existingKeys.containsAll(insertedKeys));
    }

    @Test
    @Order(6)
    void singleFile_notExists() {
        final List<String> insertedKeys = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            final String key = "single-notexists-" + i;
            singleFileDao.put(key, createLead("NotExists" + i, "ne" + i + "@test.com"));
            insertedKeys.add(key);
        }

        final List<String> nonExistentKeys = List.of("missing-1", "missing-2", "missing-3");
        final List<String> testKeys = new ArrayList<>(insertedKeys);
        testKeys.addAll(nonExistentKeys);

        final Set<String> notExisting = singleFileDao.notExists(testKeys);

        assertEquals(nonExistentKeys.size(), notExisting.size());
        assertTrue(notExisting.containsAll(nonExistentKeys));
        for (final String key : insertedKeys) {
            assertFalse(notExisting.contains(key));
        }
    }

    @Test
    @Order(7)
    void singleFile_notExistsList() {
        final List<String> insertedKeys = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            final String key = "single-notexistslist-" + i;
            singleFileDao.put(key, createLead("NotExistsList" + i, "nel" + i + "@test.com"));
            insertedKeys.add(key);
        }

        final List<String> nonExistentKeys = List.of("absent-1", "absent-2");
        final List<String> testKeys = new ArrayList<>(insertedKeys);
        testKeys.addAll(nonExistentKeys);

        final List<String> notExisting = singleFileDao.notExistsList(testKeys);

        assertEquals(nonExistentKeys.size(), notExisting.size());
        assertTrue(notExisting.containsAll(nonExistentKeys));
    }

    @Test
    @Order(8)
    void singleFile_delete() {
        final String key = "single-delete-test";
        singleFileDao.put(key, createLead("DeleteTest", "delete@test.com"));

        assertTrue(singleFileDao.exists(key));

        final boolean deleted = singleFileDao.delete(key);

        assertTrue(deleted);
        assertFalse(singleFileDao.exists(key));
        assertNull(singleFileDao.get(key));
    }

    @Test
    @Order(9)
    void singleFile_batchGet() throws InterruptedException {
        final List<String> keys = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final String key = "single-batch-get-" + i;
            singleFileDao.put(key, createLead("BatchGet" + i, "bg" + i + "@test.com"));
            keys.add(key);
        }

        final Map<String, Lead> results = singleFileDao.get(keys);

        assertEquals(keys.size(), results.size());
        for (final String key : keys) {
            assertNotNull(results.get(key));
        }
    }

    @Test
    @Order(10)
    void singleFile_batchGetWithLimit() throws InterruptedException {
        final List<String> keys = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final String key = "single-batch-limit-" + i;
            singleFileDao.put(key, createLead("BatchLimit" + i, "bl" + i + "@test.com"));
            keys.add(key);
        }

        final Map<String, Lead> results = singleFileDao.get(keys, 5);

        assertEquals(5, results.size());
    }

    @Test
    @Order(11)
    void singleFile_search() throws InterruptedException {
        for (int i = 0; i < 5; i++) {
            singleFileDao.put("single-search-" + i, createLead("SearchableLead" + i, "search" + i + "@test.com"));
        }

        final Search search = new Search("fullName", SearchType.STARTS_WITH, "SearchableLead");
        final Map<String, Lead> results = singleFileDao.search(search);

        assertTrue(results.size() >= 5);
        for (final Lead lead : results.values()) {
            assertTrue(lead.fullName.startsWith("SearchableLead"));
        }
    }

    @Test
    @Order(12)
    void singleFile_searchKeys() throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            singleFileDao.put("single-searchkeys-" + i, createLead("KeySearchTest" + i, "ks" + i + "@test.com"));
        }

        final Search search = new Search("fullName", SearchType.STARTS_WITH, "KeySearchTest");
        final Set<String> keys = singleFileDao.searchKeys(List.of(search));

        assertTrue(keys.size() >= 3);
    }

    // ==================== MULTI FILE DAO TESTS ====================

    @Test
    @Order(20)
    void multiFile_insertMany() throws InterruptedException {
        final Map<String, Lead> batch = new HashMap<>();
        for (int i = 0; i < 500; i++) {
            batch.put("multi-insert-" + i, createLead("MultiInsert" + i, "mi" + i + "@test.com"));
        }

        multiFileDao.put(batch);

        assertTrue(multiFileDao.size() >= 500);
    }

    @Test
    @Order(21)
    void multiFile_getAcrossRecords() throws InterruptedException {
        final List<String> keys = IntStream.range(0, 100)
                .mapToObj(i -> "multi-insert-" + i)
                .collect(Collectors.toList());

        final Map<String, Lead> results = multiFileDao.get(keys);

        assertEquals(100, results.size());
    }

    @Test
    @Order(22)
    void multiFile_exists() {
        assertTrue(multiFileDao.exists("multi-insert-0"));
        assertTrue(multiFileDao.exists("multi-insert-50"));
        assertTrue(multiFileDao.exists("multi-insert-200"));
        assertFalse(multiFileDao.exists("non-existent-multi"));
    }

    @Test
    @Order(23)
    void multiFile_existsCollection() {
        final List<String> existingKeys = List.of("multi-insert-0", "multi-insert-50", "multi-insert-150");
        final List<String> nonExistingKeys = List.of("fake-multi-1", "fake-multi-2");

        final List<String> allKeys = new ArrayList<>(existingKeys);
        allKeys.addAll(nonExistingKeys);

        final Map<String, Boolean> result = multiFileDao.exists(allKeys);

        for (final String key : existingKeys) {
            assertTrue(result.get(key), "Key should exist: " + key);
        }
        for (final String key : nonExistingKeys) {
            assertFalse(result.get(key), "Key should not exist: " + key);
        }
    }

    @Test
    @Order(24)
    void multiFile_notExists() {
        final List<String> existingKeys = List.of("multi-insert-10", "multi-insert-20");
        final List<String> nonExistingKeys = List.of("ghost-1", "ghost-2", "ghost-3");

        final List<String> allKeys = new ArrayList<>(existingKeys);
        allKeys.addAll(nonExistingKeys);

        final Set<String> notExisting = multiFileDao.notExists(allKeys);

        assertEquals(nonExistingKeys.size(), notExisting.size());
        assertTrue(notExisting.containsAll(nonExistingKeys));
    }

    @Test
    @Order(25)
    void multiFile_indexedSearch() {
        final Search search = new Search("fullName", SearchType.STARTS_WITH, "MultiInsert1");
        final Map<String, Lead> results = multiFileDao.indexedSearch(search);

        assertTrue(results.size() > 0);
        for (final Lead lead : results.values()) {
            assertTrue(lead.fullName.startsWith("MultiInsert1"));
        }
    }

    @Test
    @Order(26)
    void multiFile_indexedSearchKeys() {
        final Search search = new Search("fullName", SearchType.EQUAL, "MultiInsert50");
        final Set<String> keys = multiFileDao.indexedSearchKeys(search);

        assertEquals(1, keys.size());
        assertTrue(keys.contains("multi-insert-50"));
    }

    @Test
    @Order(27)
    void multiFile_deleteMultiple() {
        final List<String> keysToDelete = List.of(
                "multi-insert-5",
                "multi-insert-105",
                "multi-insert-205");

        final int sizeBefore = multiFileDao.size();
        final boolean deleted = multiFileDao.delete(keysToDelete);

        assertTrue(deleted);
        assertEquals(sizeBefore - 3, multiFileDao.size());

        for (final String key : keysToDelete) {
            assertFalse(multiFileDao.exists(key));
        }
    }

    @Test
    @Order(28)
    void multiFile_size() {
        // Ensure idempotent across runs (see singleFile_size).
        multiFileDao.delete("multi-size-check");
        final int size = multiFileDao.size();
        assertTrue(size > 0);

        multiFileDao.put("multi-size-check", createLead("SizeCheck", "sizecheck@test.com"));
        assertEquals(size + 1, multiFileDao.size());
    }

    // ==================== EDGE CASES ====================

    @Test
    @Order(40)
    void edgeCase_getNonExistentKey() {
        assertNull(singleFileDao.get("this-key-definitely-does-not-exist"));
    }

    @Test
    @Order(41)
    void edgeCase_deleteNonExistentKey() {
        final boolean result = singleFileDao.delete("another-non-existent-key");
        assertFalse(result);
    }

    @Test
    @Order(42)
    void edgeCase_existsEmptyCollection() {
        final Map<String, Boolean> result = singleFileDao.exists(Collections.emptyList());
        assertTrue(result.isEmpty());
    }

    @Test
    @Order(43)
    void edgeCase_notExistsEmptyCollection() {
        final Set<String> result = singleFileDao.notExists(Collections.emptyList());
        assertTrue(result.isEmpty());
    }

    @Test
    @Order(44)
    void edgeCase_searchWithLimit() throws InterruptedException {
        for (int i = 0; i < 20; i++) {
            singleFileDao.put("limit-test-" + i, createLead("LimitTest" + i, "lt" + i + "@test.com"));
        }

        final Search search = new Search("fullName", SearchType.STARTS_WITH, "LimitTest", 5);
        final Map<String, Lead> results = singleFileDao.search(search);

        assertEquals(5, results.size());
    }

    @Test
    @Order(45)
    void edgeCase_multiSearchWithMultipleCriteria() throws InterruptedException {
        final String uniquePrefix = "MultiCrit" + System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            singleFileDao.put("multicrit-" + i, createLead(uniquePrefix + i, "mc" + i + "@test.com"));
        }

        final List<Search> searches = List.of(
                new Search("fullName", SearchType.STARTS_WITH, uniquePrefix),
                new Search("email", SearchType.STARTS_WITH, "mc"));

        final Map<String, Lead> results = singleFileDao.multiSearch(searches);

        assertEquals(10, results.size());
    }

    @Test
    @Order(46)
    void edgeCase_getSubset() throws InterruptedException {
        final String key = "subset-test-key";
        singleFileDao.put(key, createLead("SubsetLead", "subset@test.com"));

        final Map<String, Map<String, Object>> result = singleFileDao.getSubset(
                List.of(key),
                new String[] { "fullName", "email" });

        assertEquals(1, result.size());
        final Map<String, Object> fields = result.get(key);
        assertEquals("SubsetLead", fields.get("fullName"));
        assertEquals("subset@test.com", fields.get("email"));
        assertFalse(fields.containsKey("linkedin"));
    }

    @Test
    @Order(47)
    void edgeCase_updateExisting() {
        final String key = "update-test-key";
        singleFileDao.put(key, createLead("OriginalName", "original@test.com"));

        final Lead original = singleFileDao.get(key);
        assertEquals("OriginalName", original.fullName);

        singleFileDao.put(key, createLead("UpdatedName", "updated@test.com"));

        final Lead updated = singleFileDao.get(key);
        assertEquals("UpdatedName", updated.fullName);
        assertEquals("updated@test.com", updated.email);
    }

    @Test
    @Order(48)
    void edgeCase_batchGetWithExclusions() throws InterruptedException {
        final List<String> keys = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final String key = "exclusion-test-" + i;
            singleFileDao.put(key, createLead("ExclusionTest" + i, "ex" + i + "@test.com"));
            keys.add(key);
        }

        final Set<String> excludedKeys = Set.of("exclusion-test-0", "exclusion-test-5", "exclusion-test-9");

        System.out.println("=== Exclusion Test Debug ===");
        System.out.println("Total keys to query: " + keys.size() + " -> " + keys);
        System.out.println("Excluded keys: " + excludedKeys);

        final Map<String, Lead> results = singleFileDao.get(keys, excludedKeys, 100);

        System.out.println("Results count: " + results.size());
        System.out.println("Returned keys: " + results.keySet());
        System.out.println("Missing from results (should be excluded): ");
        for (final String key : keys) {
            if (!results.containsKey(key)) {
                System.out.println("  - " + key + (excludedKeys.contains(key) ? " (EXCLUDED)" : " (UNEXPECTED!)"));
            }
        }
        System.out.println("=== End Debug ===");

        assertEquals(7, results.size());
        for (final String excluded : excludedKeys) {
            assertFalse(results.containsKey(excluded));
        }
    }

    @Test
    @Order(481)
    void edgeCase_multiSearchWithExclusions() throws InterruptedException {
        final String prefix = "MultiExclude" + System.currentTimeMillis();
        final List<String> keys = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final String key = "multi-exclude-" + prefix + "-" + i;
            singleFileDao.put(key, createLead(prefix + i, "me" + i + "@test.com"));
            keys.add(key);
        }

        final Set<String> excludedKeys = Set.of(
                "multi-exclude-" + prefix + "-0",
                "multi-exclude-" + prefix + "-5",
                "multi-exclude-" + prefix + "-9");

        final List<Search> searches = List.of(new Search("fullName", SearchType.STARTS_WITH, prefix));

        System.out.println("=== MultiSearch Exclusion Test Debug ===");
        System.out.println("Total records created: " + keys.size());
        System.out.println("Excluded keys: " + excludedKeys);

        final Map<String, Lead> results = singleFileDao.multiSearch(searches, excludedKeys);

        System.out.println("Results count: " + results.size());
        System.out.println("Returned keys: " + results.keySet());
        System.out.println("Missing from results (should be excluded): ");
        for (final String key : keys) {
            if (!results.containsKey(key)) {
                System.out.println("  - " + key + (excludedKeys.contains(key) ? " (EXCLUDED)" : " (UNEXPECTED!)"));
            }
        }
        System.out.println("=== End Debug ===");

        assertEquals(7, results.size());
        for (final String excluded : excludedKeys) {
            assertFalse(results.containsKey(excluded), "Excluded key should not be in results: " + excluded);
        }
    }

    @Test
    @Order(49)
    void edgeCase_multiSearchCount() throws InterruptedException {
        final String prefix = "CountTest" + System.currentTimeMillis();
        for (int i = 0; i < 15; i++) {
            singleFileDao.put("count-" + prefix + "-" + i, createLead(prefix + i, "ct" + i + "@test.com"));
        }

        final List<Search> searches = List.of(new Search("fullName", SearchType.STARTS_WITH, prefix));
        final long count = singleFileDao.multiSearchCount(searches);

        assertEquals(15, count);
    }

    // ==================== SEARCH TYPE TESTS ====================

    @Test
    @Order(60)
    void searchType_equal() throws InterruptedException {
        final String uniqueName = "EqualSearch" + System.currentTimeMillis();
        singleFileDao.put("equal-search-key", createLead(uniqueName, "equal@test.com"));

        final Search search = new Search("fullName", SearchType.EQUAL, uniqueName);
        final Map<String, Lead> results = singleFileDao.search(search);

        assertEquals(1, results.size());
        assertEquals(uniqueName, results.values().iterator().next().fullName);
    }

    @Test
    @Order(61)
    void searchType_like() throws InterruptedException {
        final String uniquePart = "LikeTest" + System.currentTimeMillis();
        singleFileDao.put("like-key-1", createLead("Prefix" + uniquePart + "Suffix", "like1@test.com"));
        singleFileDao.put("like-key-2", createLead("Another" + uniquePart + "End", "like2@test.com"));

        final Search search = new Search("fullName", SearchType.LIKE, uniquePart);
        final Map<String, Lead> results = singleFileDao.search(search);

        assertEquals(2, results.size());
    }

    @Test
    @Order(62)
    void searchType_endsWith() throws InterruptedException {
        final String uniqueSuffix = "EndsWith" + System.currentTimeMillis();
        singleFileDao.put("ends-key-1", createLead("Lead" + uniqueSuffix, "ends1@test.com"));
        singleFileDao.put("ends-key-2", createLead("Other" + uniqueSuffix, "ends2@test.com"));

        final Search search = new Search("fullName", SearchType.ENDS_WITH, uniqueSuffix);
        final Map<String, Lead> results = singleFileDao.search(search);

        assertEquals(2, results.size());
    }

    @Test
    @Order(63)
    void searchType_in() throws InterruptedException {
        final String prefix = "InSearch" + System.currentTimeMillis();
        final List<String> names = List.of(prefix + "A", prefix + "B", prefix + "C");

        for (int i = 0; i < names.size(); i++) {
            singleFileDao.put("in-search-" + prefix + "-" + i, createLead(names.get(i), "in" + i + "@test.com"));
        }

        final Search search = new Search("fullName", SearchType.IN, names);
        final Map<String, Lead> results = singleFileDao.search(search);

        assertEquals(3, results.size());
    }

    @Test
    @Order(64)
    void searchType_notIn() throws InterruptedException {
        final String prefix = "NotIn" + System.currentTimeMillis();
        for (int i = 0; i < 5; i++) {
            singleFileDao.put("notin-" + prefix + "-" + i, createLead(prefix + i, "notin" + i + "@test.com"));
        }

        final List<String> excluded = List.of(prefix + "0", prefix + "1");

        final List<Search> searches = List.of(
                new Search("fullName", SearchType.STARTS_WITH, prefix),
                new Search("fullName", SearchType.NOT_IN, excluded));

        final Map<String, Lead> results = singleFileDao.multiSearch(searches);

        assertEquals(3, results.size());
    }

    // ==================== CSV OUTPUT TESTS ====================

    @Test
    @Order(70)
    void csvOutput_multiSearchCsv() throws InterruptedException {
        final String prefix = "CsvTest" + System.currentTimeMillis();
        for (int i = 0; i < 5; i++) {
            singleFileDao.put("csv-" + prefix + "-" + i, createLead(prefix + i, "csv" + i + "@test.com"));
        }

        final List<Search> searches = List.of(new Search("fullName", SearchType.STARTS_WITH, prefix));
        final var csvResult = singleFileDao.multiSearchCsv(searches);

        assertNotNull(csvResult.headers());
        assertEquals(5, csvResult.rows().size());
    }

    @Test
    @Order(71)
    void csvOutput_getSubsetCsv() {
        final String prefix = "SubsetCsv" + System.currentTimeMillis();
        final List<String> keys = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            final String key = "subsetcsv-" + prefix + "-" + i;
            singleFileDao.put(key, createLead(prefix + i, "scsv" + i + "@test.com"));
            keys.add(key);
        }

        final var csvResult = singleFileDao.getSubsetCsv(keys, new String[] { "fullName", "email" });

        assertNotNull(csvResult.headers());
        assertEquals(3, csvResult.rows().size());
        assertTrue(csvResult.headers().length <= 3);
    }

    // ==================== INSERT/UPDATE SPECIFIC TESTS ====================

    @Test
    @Order(80)
    void insertUpdate_insertNew() {
        final String key = "insert-new-" + System.currentTimeMillis();

        final var result = singleFileDao.insert(key, createLead("InsertNew", "insertnew@test.com"));

        assertEquals(PutStatus.INSERTED, result);
        assertNotNull(singleFileDao.get(key));
    }

    @Test
    @Order(81)
    void insertUpdate_insertExistingFails() {
        final String key = "insert-existing-" + System.currentTimeMillis();
        singleFileDao.put(key, createLead("First", "first@test.com"));

        final var result = singleFileDao.insert(key, createLead("Second", "second@test.com"));

        assertEquals(PutStatus.FAILED, result);
        assertEquals("First", singleFileDao.get(key).fullName);
    }

    @Test
    @Order(82)
    void insertUpdate_updateExisting() {
        final String key = "update-existing-" + System.currentTimeMillis();
        singleFileDao.put(key, createLead("Original", "original@test.com"));

        final var result = singleFileDao.update(key, createLead("Updated", "updated@test.com"));

        assertEquals(PutStatus.UPDATED, result);
        assertEquals("Updated", singleFileDao.get(key).fullName);
    }

    @Test
    @Order(83)
    void insertUpdate_updateNonExistingFails() {
        final String key = "update-nonexistent-" + System.currentTimeMillis();

        final var result = singleFileDao.update(key, createLead("ShouldFail", "fail@test.com"));

        assertEquals(PutStatus.FAILED, result);
        assertNull(singleFileDao.get(key));
    }

    // ==================== VACUUM ====================

    private static final String VACUUM_DAO = "chronicle.db.service.VacuumTestDao";
    private static final String VACUUM_PATH = "src/test/.data/vacuum/";

    private static void deleteDirRecursive(final Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (final var stream = Files.walk(path)) {
            stream.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (final IOException ignored) {
                }
            });
        }
    }

    /**
     * Vacuum scenario: 5 rotated files, then deletes half of data-2 (sparse),
     * half of data-3 (sparse), and 5% of data-4 (dense). Verifies:
     * <ul>
     * <li>data-2 and data-3 records get repacked into one new file at slot 2
     * (lowest sparse slot, since max-N=5 is dense and untouched)</li>
     * <li>data-3 slot becomes a gap</li>
     * <li>dense files (data, data-4, data-5) keep their original suffixes</li>
     * <li>all surviving records remain retrievable by key</li>
     * <li>deleted records stay deleted</li>
     * <li>indexed search still finds moved records (indexes weren't rebuilt
     * but remain valid because record values didn't change)</li>
     * </ul>
     */
    @Test
    @Order(90)
    void vacuum_partialCompaction() throws Throwable {
        // Fresh data dir so test is fully reproducible.
        deleteDirRecursive(Path.of(VACUUM_PATH));

        final ChronicleDao<Lead> dao = CHRONICLE_DB.getChronicleDao(VACUUM_DAO, VACUUM_PATH);

        // Insert exactly 500 records (entries() = 100 → produces 5 rotated files).
        // Batched so rotation happens during insert, like production traffic.
        for (int batchStart = 0; batchStart < 500; batchStart += 50) {
            final Map<String, Lead> batch = new HashMap<>();
            for (int i = batchStart + 1; i <= batchStart + 50; i++) {
                batch.put("vac-" + i, createLead("Vac" + i, "vac" + i + "@test.com"));
            }
            dao.put(batch);
        }
        assertEquals(500, dao.size());
        final var preFiles = dao.getDataFileState().fileNames();
        assertEquals(5, preFiles.size(), "Expected 5 rotated files; got " + preFiles);
        assertTrue(preFiles.containsAll(Set.of("data", "data-2", "data-3", "data-4", "data-5")));

        // Delete to produce: data unchanged (dense), data-2 50% (sparse),
        // data-3 50% (sparse), data-4 95% (dense), data-5 unchanged (dense).
        // Record-to-file mapping is deterministic because put(Map) inserts
        // sequentially across the rotation boundary: vac-1..100 → data,
        // vac-101..200 → data-2, vac-201..300 → data-3, vac-301..400 → data-4,
        // vac-401..500 → data-5. We delete from each file's key range.
        for (int i = 101; i <= 150; i++) {
            dao.delete("vac-" + i);
        }
        for (int i = 201; i <= 250; i++) {
            dao.delete("vac-" + i);
        }
        for (int i = 301; i <= 305; i++) {
            dao.delete("vac-" + i);
        }
        assertEquals(500 - 50 - 50 - 5, dao.size());

        // Run vacuum.
        dao.vacuum();

        // Post-vacuum: data (dense, untouched), data-2 (new packed of 100
        // sparse survivors), data-4 (dense, untouched), data-5 (dense,
        // untouched). data-3 slot becomes a gap. So 4 files total.
        final var postFiles = dao.getDataFileState().fileNames();
        assertEquals(4, postFiles.size(), "Expected 4 files after vacuum; got " + postFiles);
        assertTrue(postFiles.contains("data"));
        assertTrue(postFiles.contains("data-2"));
        assertFalse(postFiles.contains("data-3"), "data-3 should be a gap");
        assertTrue(postFiles.contains("data-4"));
        assertTrue(postFiles.contains("data-5"));

        // Total size unchanged after vacuum.
        assertEquals(500 - 50 - 50 - 5, dao.size());

        // All surviving records retrievable.
        for (int i = 1; i <= 100; i++) {
            assertNotNull(dao.get("vac-" + i), "vac-" + i + " should be retrievable (was in data)");
        }
        for (int i = 151; i <= 200; i++) {
            assertNotNull(dao.get("vac-" + i), "vac-" + i + " should be retrievable (moved from data-2)");
        }
        for (int i = 251; i <= 300; i++) {
            assertNotNull(dao.get("vac-" + i), "vac-" + i + " should be retrievable (moved from data-3)");
        }
        for (int i = 306; i <= 400; i++) {
            assertNotNull(dao.get("vac-" + i), "vac-" + i + " should be retrievable (was in data-4)");
        }
        for (int i = 401; i <= 500; i++) {
            assertNotNull(dao.get("vac-" + i), "vac-" + i + " should be retrievable (was in data-5)");
        }

        // Deleted records remain deleted.
        for (int i = 101; i <= 150; i++) {
            assertNull(dao.get("vac-" + i), "vac-" + i + " should still be deleted");
        }
        for (int i = 201; i <= 250; i++) {
            assertNull(dao.get("vac-" + i), "vac-" + i + " should still be deleted");
        }
        for (int i = 301; i <= 305; i++) {
            assertNull(dao.get("vac-" + i), "vac-" + i + " should still be deleted");
        }

        // Indexed search still works for a record that was moved across files
        // (validates that the index entries — never rebuilt during vacuum —
        // remain correct because the record value didn't change).
        final var movedKeyResults = dao.multiSearch(
                List.of(new Search("fullName", SearchType.EQUAL, "Vac180")));
        assertTrue(movedKeyResults.containsKey("vac-180"),
                "Indexed search should find moved record 'vac-180'");
    }
}
