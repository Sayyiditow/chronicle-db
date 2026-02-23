package chronicle.db.service;

import static chronicle.db.service.ChronicleDb.CHRONICLE_DB;
import static chronicle.db.utils.ChronicleUtils.CHRONICLE_UTILS;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import chronicle.db.Server;
import chronicle.db.dao.ChronicleDao;

@SuppressWarnings("unchecked")
public class ChronicleDaoService {
    private ChronicleDaoService() {
    }

    public static final ChronicleDaoService CHRONICLE_DAO_SERVICE = new ChronicleDaoService();

    private final Map<String, List<String>> archivePeriodsCache = new ConcurrentHashMap<>();
    private volatile long lastCacheClearTime = System.currentTimeMillis();

    public List<String> getAvailableArchivePeriods(final String dbDir, final String filePath) throws IOException {
        if (System.currentTimeMillis() - lastCacheClearTime > 86400000L) {
            archivePeriodsCache.clear();
            lastCacheClearTime = System.currentTimeMillis();
        }

        final var fullPath = dbDir + "/" + filePath;
        if (archivePeriodsCache.containsKey(fullPath)) {
            return archivePeriodsCache.get(fullPath);
        }

        final var archiveRoot = Server.getDbArchPath();
        final var periods = CHRONICLE_UTILS.getFileList(archiveRoot);
        final var availablePeriods = new ArrayList<String>();

        for (final String period : periods) {
            if (Files.exists(Path.of(archiveRoot, period, fullPath))) {
                availablePeriods.add(period);
            }
        }

        archivePeriodsCache.put(fullPath, availablePeriods);
        return availablePeriods;
    }

    public String getDbPath(final String dbDir, final String filePath) {
        return Server.getDbPath() + "/" + dbDir + "/" + filePath;
    }

    public String getArchiveDbPath(final String archivePeriod, final String dbDir, final String filePath) {
        return Server.getDbArchPath() + "/" + archivePeriod + "/" + dbDir + "/" + filePath;
    }

    public void createArchiveDirs(final String archivePeriod, final String dbDir, final String path)
            throws IOException {
        Files.createDirectories(Path.of(getArchiveDbPath(archivePeriod, dbDir, path)));
        // Clear cache so getAvailableArchivePeriods() sees the new period
        archivePeriodsCache.clear();
    }

    private <V> ChronicleDao<V> getArchiveDao(final String fqn, final String dbDir, final String filePath,
            final String archivePeriod) throws Throwable {
        final var periods = getAvailableArchivePeriods(dbDir, filePath);

        if (!periods.contains(archivePeriod)) {
            throw new IllegalArgumentException("Archive period " + archivePeriod + " not found for " + filePath);
        }

        return CHRONICLE_DB.getChronicleDao(fqn, getArchiveDbPath(archivePeriod, dbDir, filePath));
    }

    public <V> ChronicleDao<V> getDao(final Map<String, Object> params) throws Throwable {
        final var fqn = params.get("fqn").toString();
        final var dbDir = params.get("dbDir").toString();
        final var filePath = params.get("filePath").toString();
        final var isArchived = params.get("isArchived");
        final var archivePeriod = params.get("archivePeriod");

        if (isArchived != null && (boolean) isArchived) {
            return getArchiveDao(fqn, dbDir, filePath, archivePeriod.toString());
        }

        return CHRONICLE_DB.getChronicleDao(fqn, getDbPath(dbDir, filePath));
    }

    public <V> ChronicleDao<V> getDao(final String fqn, final String dbDir, final String filePath) throws Throwable {
        return CHRONICLE_DB.getChronicleDao(fqn, getDbPath(dbDir, filePath));
    }

    public <V> ChronicleDao<V> getDao(final String fqn, final String dbDir, final String filePath,
            final String archivePeriod) throws Throwable {
        if (archivePeriod != null && !archivePeriod.replace("null", "").replace("missing", "").isBlank()) {
            return getArchiveDao(fqn, dbDir, filePath, archivePeriod);
        }
        return getDao(fqn, dbDir, filePath);
    }
}
