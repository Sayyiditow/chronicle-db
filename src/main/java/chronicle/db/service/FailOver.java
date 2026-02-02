package chronicle.db.service;

import static chronicle.db.utils.ChronicleUtils.CHRONICLE_UTILS;

import java.io.BufferedReader;
import java.io.File;
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

import org.tinylog.Logger;

import chronicle.db.Server;
import chronicle.db.config.QueryMode;

public class FailOver {
    public static ConcurrentMap<String, ConcurrentMap<String, Integer>> getDbCounts(
            final Map<String, String> fqnMap) throws IOException {
        final var response = new ConcurrentHashMap<String, ConcurrentMap<String, Integer>>();
        final var dbDirs = Server.getDbDirs();
        CHRONICLE_UTILS.processInParallel(dbDirs, dir -> {
            final var countMap = new ConcurrentHashMap<String, Integer>();
            fqnMap.entrySet().parallelStream().forEach(fqn -> {
                final var filePath = fqn.getValue();
                try {
                    final var dao = ChronicleDaoService.CHRONICLE_DAO_SERVICE.getDao(fqn.getKey(), filePath);
                    countMap.put(filePath, dao.size());
                } catch (final Throwable e) {
                    Logger.error("DB failover count failed for dir [{}] and file [{}]", dir, filePath);
                    Logger.error(e);
                }
            });
            response.put(dir, countMap);
        });

        return response;
    }

    public static Map<String, Set<String>> printDbCountsSideBySide(
            final ConcurrentMap<String, ConcurrentMap<String, Integer>> primary,
            final ConcurrentMap<String, ConcurrentMap<String, Integer>> standby) {
        final Set<String> dbDirs = primary.keySet();
        final Map<String, Set<String>> toSync = new HashMap<>();
        boolean hasMismatch = false;

        final String header = String.format("%-12s %-36s %-20s %-20s", "Dir", "Object", "Primary", "Standby");

        for (final String dbDir : dbDirs) {
            final var primaryCountMap = primary.get(dbDir);
            final var standbyCountMap = standby.get(dbDir);
            final var filePaths = primaryCountMap.keySet();

            if (standbyCountMap == null) {
                Logger.error("Standby does not have dir [{}]", dbDir);
                toSync.put(dbDir, new HashSet<>(filePaths));
                continue;
            }

            for (final String filePath : filePaths) {
                final int primaryCount = primaryCountMap.getOrDefault(filePath, 0);
                final int standbyCount = standbyCountMap.getOrDefault(filePath, 0);

                if (primaryCount != standbyCount) {
                    if (!hasMismatch) {
                        // Print header only once
                        Logger.info(header);
                        Logger.info("---------------------------------------------------------------");
                    }
                    hasMismatch = true;

                    final String line = String.format("%-3s %-40s %-12d %-12d",
                            dbDir, new File(filePath).getName(), primaryCount, standbyCount);

                    Logger.error("❌ " + line);
                    toSync.computeIfAbsent(dbDir, k -> new HashSet<String>()).add(filePath);
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

    public static void main(final String[] args) throws InterruptedException {
        final var dbUrl = args[0];
        final int dbPort = Integer.parseInt(args[1]);
        final var clientService = new ClientSocketService(dbUrl, dbPort, 1, 0);
        clientService.execute(Map.of("mode", QueryMode.FAIL_OVER));
    }
}
