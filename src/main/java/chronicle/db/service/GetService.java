package chronicle.db.service;

import static chronicle.db.service.ChronicleDaoService.CHRONICLE_DAO_SERVICE;
import static chronicle.db.utils.ChronicleUtils.CHRONICLE_UTILS;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.tinylog.Logger;

import chronicle.db.Server;
import chronicle.db.dao.ChronicleDao;
import chronicle.db.entity.CsvObject;
import chronicle.db.entity.Search;

@SuppressWarnings({ "unchecked" })
public final class GetService {
    private GetService() {
    }

    public static List<String> getAllDbDirs() throws IOException {
        final var dbFolders = CHRONICLE_UTILS.getFileList(Server.getDbPath());
        dbFolders.remove(Server.getAppdir());
        return dbFolders;
    }

    public static Object get(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);

        return dao.get(params.get("key").toString());
    }

    public static Object getSubset(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        final var key = params.get("key").toString();
        final var value = dao.get(key);

        if (value == null) {
            return new HashMap<>();
        }
        final var map = new HashMap<String, Map<String, Object>>(1);
        final var classData = CHRONICLE_UTILS.getClassData(dao.averageValue().getClass());
        map.put(key, CHRONICLE_UTILS.getSubsetFromObject(classData, (String[]) params.get("subsetFields"), value));
        return map.values().iterator().next();
    }

    public static Map<String, ?> getAll(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);

        return dao.get((Iterable<String>) params.get("keys"));
    }

    public static CsvObject getAllCsv(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);

        return dao.getCsv((Iterable<String>) params.get("keys"));
    }

    public static Map<String, Map<String, Object>> getAllSubset(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        return dao.getSubset((Iterable<String>) params.get("keys"), (String[]) params.get("subsetFields"));
    }

    public static CsvObject getAllSubsetCsv(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        return dao.getSubsetCsv((Iterable<String>) params.get("keys"), (String[]) params.get("subsetFields"));
    }

    public static Map<String, ?> fetch(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        final var limit = params.get("limit");

        if (limit != null) {
            return dao.fetch((int) limit);
        }

        return dao.fetch();
    }

    public static Set<String> fetchKeys(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        final var limit = params.get("limit");
        final var keys = dao.fetchKeys();

        if (limit != null) {
            return CHRONICLE_UTILS.limitSetValues(keys, (int) limit);
        }

        return keys;
    }

    public static List<String> fetchKeysList(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        final var limit = params.get("limit");
        final var keys = dao.fetchKeysList();

        if (limit != null) {
            return CHRONICLE_UTILS.limitListValues(keys, (int) limit);
        }

        return keys;
    }

    public static CsvObject fetchCsv(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        final var limit = params.get("limit");

        if (limit != null) {
            return dao.fetchCsv((int) limit);
        }

        return dao.fetchCsv();
    }

    public static Map<String, Map<String, Object>> fetchSubset(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        final var limit = params.get("limit");

        if (limit != null) {
            return dao.fetchSubset((String[]) params.get("subsetFields"), (int) limit);
        }

        return dao.fetchSubset((String[]) params.get("subsetFields"));
    }

    public static CsvObject fetchSubsetCsv(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        final var limit = params.get("limit");

        if (limit != null) {
            return dao.fetchSubsetCsv((String[]) params.get("subsetFields"), (int) limit);
        }

        return dao.fetchSubsetCsv((String[]) params.get("subsetFields"));
    }

    public static Map<String, Object> searchOneNonIndexed(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        final var search = (Search) params.get("search");
        final var db = params.get("db");

        if (db != null) {
            return dao.search((Map<String, Object>) db, search);
        }

        final var keys = (Collection<String>) params.get("keys");
        if (keys != null && !keys.isEmpty()) {
            return dao.searchMatching(keys, search);
        }

        final var excludedKeys = (Set<String>) params.get("excludedKeys");
        if (excludedKeys != null && !excludedKeys.isEmpty()) {
            return dao.search(search, excludedKeys);
        }

        return dao.search(search);
    }

    public static Set<String> searchOneNonIndexedKeys(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        final var search = (Search) params.get("search");

        final var excludedKeys = (Set<String>) params.get("excludedKeys");
        if (excludedKeys != null && !excludedKeys.isEmpty()) {
            return dao.searchKeys(List.of(search), excludedKeys);
        }

        final var keys = (Collection<String>) params.get("keys");
        if (keys != null && !keys.isEmpty()) {
            return dao.searchKeys(keys, List.of(search));
        }

        return dao.searchKeys(List.of(search));
    }

    public static List<String> searchOneNonIndexedKeysList(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        final var search = (Search) params.get("search");

        final var excludedKeys = (Set<String>) params.get("excludedKeys");
        if (excludedKeys != null && !excludedKeys.isEmpty()) {
            return dao.searchKeysList(List.of(search), excludedKeys);
        }

        final var keys = (Collection<String>) params.get("keys");
        if (keys != null && !keys.isEmpty()) {
            return dao.searchKeysList(keys, List.of(search));
        }

        return dao.searchKeysList(List.of(search));
    }

    public static Map<String, ?> searchOneIndexed(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        final var search = (Search) params.get("search");

        final var keys = (Collection<String>) params.get("keys");
        if (keys != null && !keys.isEmpty()) {
            return dao.searchMatching(keys, search);
        }

        final var excludedKeys = (Set<String>) params.get("excludedKeys");
        if (excludedKeys != null && !excludedKeys.isEmpty()) {
            return dao.indexedSearch(search, excludedKeys);
        }

        return dao.indexedSearch(search);
    }

    public static Set<String> searchOneIndexedKeys(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        final var search = (Search) params.get("search");

        final var excludedKeys = (Set<String>) params.get("excludedKeys");
        if (excludedKeys != null && !excludedKeys.isEmpty()) {
            return dao.indexedSearchKeys(search, excludedKeys);
        }

        final var keys = (Collection<String>) params.get("keys");
        if (keys != null && !keys.isEmpty()) {
            return dao.searchKeys(keys, List.of(search));
        }

        return dao.indexedSearchKeys(search);
    }

    public static List<String> searchOneIndexedKeysList(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        final var search = (Search) params.get("search");

        final var excludedKeys = (Set<String>) params.get("excludedKeys");
        if (excludedKeys != null && !excludedKeys.isEmpty()) {
            return dao.indexedSearchKeysList(search, excludedKeys);
        }

        final var keys = (Collection<String>) params.get("keys");
        if (keys != null && !keys.isEmpty()) {
            return dao.searchKeysList(keys, List.of(search));
        }

        return dao.indexedSearchKeysList(search);
    }

    public static Map<String, ?> search(final Map<String, Object> params) throws Throwable {
        final var searches = (List<Search>) params.get("search");
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);

        final var keys = (Collection<String>) params.get("keys");
        if (keys != null && !keys.isEmpty()) {
            return dao.multiSearch(keys, searches);
        }

        final var excludedKeys = (Set<String>) params.get("excludedKeys");
        if (excludedKeys != null && !excludedKeys.isEmpty()) {
            return dao.multiSearch(searches, excludedKeys);
        }

        return dao.multiSearch(searches);
    }

    public static Set<String> searchKeys(final Map<String, Object> params) throws Throwable {
        final var searches = (List<Search>) params.get("search");
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);

        final var excludedKeys = (Set<String>) params.get("excludedKeys");
        if (excludedKeys != null && !excludedKeys.isEmpty()) {
            return dao.searchKeys(searches, excludedKeys);
        }

        final var keys = (Collection<String>) params.get("keys");
        if (keys != null && !keys.isEmpty()) {
            return dao.searchKeys(keys, searches);
        }

        return dao.multiSearchKeys(searches);
    }

    public static List<String> searchKeysList(final Map<String, Object> params) throws Throwable {
        final var searches = (List<Search>) params.get("search");
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);

        final var excludedKeys = (Set<String>) params.get("excludedKeys");
        if (excludedKeys != null && !excludedKeys.isEmpty()) {
            return dao.searchKeysList(searches, excludedKeys);
        }

        final var keys = (Collection<String>) params.get("keys");
        if (keys != null && !keys.isEmpty()) {
            return dao.searchKeysList(keys, searches);
        }

        return dao.multiSearchKeysList(searches);
    }

    public static long searchCount(final Map<String, Object> params) throws Throwable {
        final var searches = (List<Search>) params.get("search");
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        return dao.multiSearchCount(searches);
    }

    public static CsvObject searchCsv(final Map<String, Object> params) throws Throwable {
        final var searches = (List<Search>) params.get("search");
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);

        final var keys = (Collection<String>) params.get("keys");
        if (keys != null && !keys.isEmpty()) {
            return dao.multiSearchCsv(keys, searches);
        }

        final var excludedKeys = (Set<String>) params.get("excludedKeys");
        if (excludedKeys != null && !excludedKeys.isEmpty()) {
            return dao.multiSearchCsv(searches, excludedKeys);
        }

        return dao.multiSearchCsv(searches);
    }

    public static <V> Map<String, Map<String, Object>> searchSubset(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        final var fields = (String[]) params.get("subsetFields");
        final var searches = (List<Search>) params.get("search");

        final var keys = (Collection<String>) params.get("keys");
        if (keys != null && !keys.isEmpty()) {
            return dao.multiSearchSubset(keys, searches, fields);
        }

        final var excludedKeys = (Set<String>) params.get("excludedKeys");
        if (excludedKeys != null && !excludedKeys.isEmpty()) {
            return dao.multiSearchSubset(searches, excludedKeys, fields);
        }

        return dao.multiSearchSubset(searches, fields);
    }

    public static CsvObject searchSubsetCsv(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        final var fields = (String[]) params.get("subsetFields");
        final var searches = (List<Search>) params.get("search");

        final var keys = (Collection<String>) params.get("keys");
        if (keys != null && !keys.isEmpty()) {
            return dao.multiSearchSubsetCsv(keys, searches, fields);
        }

        final var excludedKeys = (Set<String>) params.get("excludedKeys");
        if (excludedKeys != null && !excludedKeys.isEmpty()) {
            return dao.multiSearchSubsetCsv(searches, excludedKeys, fields);
        }

        return dao.multiSearchSubsetCsv(searches, fields);
    }

    public static byte[] getFile(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        final var fullPath = dao.dataPath() + ChronicleDao.FILES_DIR + params.get("fileName");
        final var file = new File(fullPath);

        if (file.exists()) {
            try {
                return Files.readAllBytes(file.toPath());
            } catch (final IOException e) {
                Logger.error("Could not get file [{}] from db.", fullPath);
            }
        }

        return null;
    }

    public static Map<String, String> getFiles(final Map<String, Object> params) {
        final var filePath = params.get("filePath").toString();
        final var dirPath = Server.getDbPath() + "/" + params.get("dbDir") + "/" + filePath
                + ChronicleDao.FILES_DIR;
        final var files = new ConcurrentHashMap<String, String>();
        final var fileNames = (List<String>) params.get("fileNames");

        CHRONICLE_UTILS.processInParallel(fileNames, fileName -> {
            final var file = new File(dirPath + fileName);

            if (file.exists()) {
                try {
                    files.put(fileName, Files.readString(file.toPath()));
                } catch (final IOException e) {
                    Logger.error("Could not get file [{}] from db.", file.getAbsolutePath());
                }
            }
        });

        return files;
    }

    public static long entries(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        return dao.entries();
    }

    public static boolean exists(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        return dao.exists(params.get("key").toString());
    }

    public static Map<String, Boolean> existsMap(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        return dao.exists((Collection<String>) params.get("keys"));
    }

    public static Set<String> existsSet(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        return dao.existsSet((Collection<String>) params.get("keys"));
    }

    public static List<String> existsList(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        return dao.existsList((Collection<String>) params.get("keys"));
    }

    public static Set<String> notExists(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        return dao.notExists((Collection<String>) params.get("keys"));
    }

    public static List<String> notExistsList(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        return dao.notExistsList((Collection<String>) params.get("keys"));
    }
}
