package chronicle.db.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;

import org.tinylog.Logger;

import chronicle.db.dao.ChronicleDao;
import chronicle.db.dao.ChronicleDao.VacuumInfo;

public final class VacuumService {
    private VacuumService() {
    }

    /**
     * Analyzes all DAOs across all database directories to identify which
     * objects need vacuuming.
     * <p>
     * Uses {@link ChronicleDao#needsVacuum()} to check each DAO.
     *
     * @param fqnMap Map of FQN to list of filePaths for each DAO to check
     * @return Map of dbDir -> Map of "FQN:path" -> VacuumInfo
     * @throws IOException if database directory list cannot be retrieved
     */
    public static ConcurrentMap<String, ConcurrentMap<String, VacuumInfo>> getVacuumCandidates(
            final Map<String, List<String>> fqnMap) throws IOException {
        final var dbDirs = GetService.getAllDbDirs();
        final var response = new ConcurrentHashMap<String, ConcurrentMap<String, VacuumInfo>>();

        // Use virtual threads - try-with-resources waits for all tasks on close
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (final var dbDir : dbDirs) {
                executor.submit(() -> {
                    final var dirResponse = new ConcurrentHashMap<String, VacuumInfo>();

                    // Sequential within each dbDir to avoid nested parallelism in DAO init
                    for (final var entry : fqnMap.entrySet()) {
                        final var fqn = entry.getKey();
                        final var filePaths = entry.getValue();
                        for (final var filePath : filePaths) {
                            try {
                                final var dao = ChronicleDaoService.CHRONICLE_DAO_SERVICE.getDao(fqn, dbDir, filePath);
                                final var vacuumInfo = dao.needsVacuum();
                                if (vacuumInfo != null) {
                                    dirResponse.put(dao.dataPath(), vacuumInfo);
                                }
                            } catch (final Throwable e) {
                                Logger.error("Vacuum check failed for dir [{}] fqn [{}] path [{}]", dbDir, fqn,
                                        filePath);
                                Logger.error(e);
                            }
                        }
                    }

                    if (!dirResponse.isEmpty()) {
                        response.put(dbDir, dirResponse);
                    }
                });
            }
        }

        return response;
    }

    /**
     * Prints vacuum candidates in a formatted table and returns the map.
     *
     * @param fqnMap Map of FQN to list of filePaths for each DAO to check
     * @return Map of dbDir -> Map of "FQN:path" -> VacuumInfo
     * @throws IOException if database directory list cannot be retrieved
     */
    public static ConcurrentMap<String, ConcurrentMap<String, VacuumInfo>> printVacuumCandidates(
            final Map<String, List<String>> fqnMap) throws IOException {
        final var candidates = getVacuumCandidates(fqnMap);

        if (candidates.isEmpty()) {
            Logger.info("✅ No data objects require vacuuming.");
            return candidates;
        }

        final String header = String.format("%-50s %-10s %-10s %-10s %-12s",
                "Filepath", "Actual", "Expected", "Records", "Entries/File");
        Logger.info(header);
        Logger.info("-".repeat(95));

        for (final var dbDirEntry : candidates.entrySet()) {
            for (final var entry : dbDirEntry.getValue().entrySet()) {
                final var info = entry.getValue();
                final var dataPath = entry.getKey();
                final String line = String.format("%-50s %-10d %-10d %-10d %-12d",
                        dataPath, info.actualFiles(), info.expectedFiles(), info.recordCount(),
                        info.entriesPerFile());
                Logger.info("⚠️  " + line);
            }
        }

        return candidates;
    }
}
