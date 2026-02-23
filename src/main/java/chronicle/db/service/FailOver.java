package chronicle.db.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;

import org.tinylog.Logger;

import chronicle.db.Server;

public class FailOver {
    public static ConcurrentMap<String, ConcurrentMap<String, Integer>> getDbCounts(
            final Map<String, List<String>> fqnMap) throws IOException {
        final var dbDirs = GetService.getAllDbDirs();
        final var response = new ConcurrentHashMap<String, ConcurrentMap<String, Integer>>();

        // Use virtual threads - try-with-resources waits for all tasks on close
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (final var dbDir : dbDirs) {
                executor.submit(() -> {
                    final var dirResponse = new ConcurrentHashMap<String, Integer>();

                    // Sequential within each dbDir to avoid nested parallelism in DAO init
                    for (final var entry : fqnMap.entrySet()) {
                        final var fqn = entry.getKey();
                        final var filePaths = entry.getValue();
                        for (final var filePath : filePaths) {
                            try {
                                final var dao = ChronicleDaoService.CHRONICLE_DAO_SERVICE.getDao(fqn, dbDir, filePath);
                                dirResponse.put(dao.dataPath(), dao.size());
                            } catch (final Throwable e) {
                                Logger.error("DB failover count failed for dbDir [{}] fqn [{}] path [{}]", dbDir, fqn,
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
     * Compares DB counts between primary and standby.
     * Returns a map of dbDir -> Set of "fqn:path" keys that need syncing.
     */
    public static Map<String, Set<String>> printDbCountsSideBySide(
            final Map<String, List<String>> fqnMap,
            final ConcurrentMap<String, ConcurrentMap<String, Integer>> primary,
            final ConcurrentMap<String, ConcurrentMap<String, Integer>> standby) {
        final Set<String> dbDirs = primary.keySet();
        final Map<String, Set<String>> toSync = new HashMap<>();
        boolean hasMismatch = false;

        final String header = String.format("%-50s %-12s %-12s", "Filepath", "Primary", "Standby");

        for (final String dbDir : dbDirs) {
            final var primaryEnumMap = primary.get(dbDir);
            final var standbyEnumMap = standby.get(dbDir);

            if (standbyEnumMap == null) {
                Logger.error("Standby does not have DB Dir [{}]", dbDir);
                toSync.put(dbDir, Set.of(Server.getDbPath() + "/" + dbDir));
                continue;
            }

            for (final var key : primaryEnumMap.keySet()) {
                final int primaryCount = primaryEnumMap.get(key);
                final int standbyCount = standbyEnumMap.getOrDefault(key, 0);

                if (primaryCount != standbyCount) {
                    if (!hasMismatch) {
                        Logger.info(header);
                        Logger.info("-".repeat(75));
                    }
                    hasMismatch = true;

                    final String line = String.format("%-50s %-12d %-12d", key, primaryCount, standbyCount);

                    Logger.error("❌ " + line);
                    toSync.computeIfAbsent(dbDir, k -> new HashSet<String>()).add(key);
                }
            }

        }

        if (!hasMismatch) {
            Logger.info("✅ All counts match between primary and standby.");
        }

        return toSync;
    }

    public static boolean syncData(final Path sourceDir, final String remote) throws IOException, InterruptedException {
        final var sourceDirStr = sourceDir.toString() + "/";
        final List<String> command = List.of(
                "rsync",
                "-avz",
                "--inplace",
                "--no-whole-file",
                "--partial",
                "--delete",
                "--timeout=120",
                "--ignore-errors",
                sourceDirStr,
                remote + ":" + sourceDirStr);

        final ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);

        Logger.info("🔄 Starting sync: {}", String.join(" ", command));

        final Process process = pb.start();

        // Capture the output and log it line by line
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                Logger.info("[sync] {}", line);
            }
        }

        final int exitCode = process.waitFor();
        if (exitCode != 0) {
            Logger.error("❌ Sync failed with exit code [{}] for [{}]", exitCode, sourceDir);
            return false;
        } else {
            Logger.info("✅ Sync completed successfully for [{}]", sourceDir);
            return true;
        }
    }
}
