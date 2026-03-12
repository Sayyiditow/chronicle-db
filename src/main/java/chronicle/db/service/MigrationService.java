package chronicle.db.service;

import static chronicle.db.service.ChronicleDb.CHRONICLE_DB;
import static chronicle.db.utils.ChronicleUtils.CHRONICLE_UTILS;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.tinylog.Logger;

import com.jsoniter.spi.TypeLiteral;

import chronicle.db.Server;
import chronicle.db.dao.ChronicleDao;
import chronicle.db.utils.JsonUtils;
import chronicle.db.utils.SafeRunnable;

/**
 * This class is if you change fields, that is add or remove fields from
 * chronicle java objects. It will pickup the migration.json file from
 * resources and do the needful before application start.
 * 
 * json file format
 * [
 * {
 * sourceObject: "sourceClassName",
 * destObject: "destClassName",
 * pathPrefix: [""]
 * move:{sourceField: destField},
 * def: {fieldName: defValue}
 * }
 * ]
 */
public final class MigrationService {
    private MigrationService() {
    }

    @SuppressWarnings("unchecked")
    public static void migrateObjects() throws IOException {
        final var migrationFilePath = Server.getResourceDir() + "/migration.json";
        final var config = JsonUtils.fromJsonFileToObj(migrationFilePath,
                new TypeLiteral<List<Map<String, Object>>>() {
                });
        final var statusPathStr = Server.getResourceDir() + "/.migrationStatus.json";
        final var statusPath = Path.of(statusPathStr);
        if (!Files.exists(statusPath)) {
            Files.writeString(statusPath, "{}");
        }
        final var moveStatus = JsonUtils.fromJsonFileToObj(statusPathStr,
                new TypeLiteral<Map<String, Boolean>>() {
                });
        boolean migrationStarted = false;

        for (final var item : config) {
            final var sourceObject = item.get("sourceObject").toString();
            final var destObject = item.get("destObject").toString();
            final var dbDirs = Server.getDbDirs();
            final var pathPrefix = (List<String>) item.get("pathPrefix");
            final var move = (Map<String, String>) item.get("move");
            final var def = (Map<String, Object>) item.get("def");

            if (pathPrefix.isEmpty()) {
                pathPrefix.add("");
            }

            if (moveStatus.get(destObject) != null) {
                continue;
            }
            migrationStarted = true;

            final var runnables = new ArrayList<Runnable>();
            dbDirs.forEach(dbDir -> {
                runnables.add(new SafeRunnable(() -> {
                    Logger.info("\n--------Migrating [" + sourceObject + "] to [" + destObject + "] for dir ["
                            + dbDir + "]--------");
                    for (final var prefix : pathPrefix) {
                        final var dataPath = Server.getDbPath() + "/" + dbDir + "/" + prefix;
                        final var sourceDao = CHRONICLE_DB.getChronicleDao(sourceObject, dataPath);
                        final var destDao = CHRONICLE_DB.getChronicleDao(destObject,
                                dataPath);
                        destDao.backup();
                        destDao.truncate();

                        for (final var file : sourceDao.getDataFileState().fileNames()) {
                            try (final var shared = sourceDao.openDb(file)) {
                                final var updateDb = CHRONICLE_UTILS.moveRecords(shared, sourceObject, destObject,
                                        move, def);
                                destDao.insert(updateDb);
                            }
                        }

                        final var sourceFilesStr = sourceDao.dataPath() + ChronicleDao.FILES_DIR;
                        final var sourceFilesPath = Path.of(sourceFilesStr);
                        if (Files.exists(sourceFilesPath)
                                && CHRONICLE_UTILS.getFileList(sourceFilesStr).size() > 0) {
                            CHRONICLE_UTILS.moveDirContents(sourceFilesPath,
                                    Path.of(destDao.dataPath() + ChronicleDao.FILES_DIR));
                        }
                    }
                    Logger.info("\n--------Migrations from [" + sourceObject + "] to [" + destObject
                            + "] for dir [" + dbDir + "] complete--------");
                }, "Migration " + sourceObject + destObject + dbDir));
            });

            runnables.parallelStream().forEach(Runnable::run);
            moveStatus.put(destObject, true);
            moveStatus.remove(sourceObject);
        }

        if (migrationStarted) {
            JsonUtils.toJsonFileFromObj(statusPathStr, moveStatus);
        }
    }
}
