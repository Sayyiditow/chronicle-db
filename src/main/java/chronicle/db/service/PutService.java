package chronicle.db.service;

import static chronicle.db.service.ChronicleDaoService.CHRONICLE_DAO_SERVICE;
import static chronicle.db.service.SequenceService.SEQUENCE_SERVICE;
import static chronicle.db.utils.ChronicleUtils.CHRONICLE_UTILS;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.tinylog.Logger;

import chronicle.db.dao.ChronicleDao;

@SuppressWarnings("unchecked")
public final class PutService {
    private PutService() {
    }

    public static boolean refreshIndexes(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        dao.refreshIndexes();

        return true;
    }

    public static boolean refreshKeyMap(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        dao.refreshKeyMap();

        return true;
    }

    public static boolean updateFile(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        final var file = ((byte[]) params.get("file"));
        final var fullPath = dao.dataPath() + ChronicleDao.FILES_DIR + params.get("fileName");

        try {
            Files.write(Path.of(fullPath), file);
            return true;
        } catch (final IOException e) {
            Logger.error("Error writing file at [{}].", fullPath);
            Logger.error(e);
            return false;
        }
    }

    public static boolean updateSequences(final Map<String, Object> params) throws IOException {
        SEQUENCE_SERVICE.updateSequenceDb((Map<String, Long>) params.get("sequences"));

        return true;
    }

    public static boolean moveFiles(final Map<String, Object> params) {
        final List<String[]> paths = (List<String[]>) params.get("paths");

        for (final var sourceDest : paths) {
            if (Files.exists(Path.of(sourceDest[0])))
                CHRONICLE_UTILS.move(Path.of(sourceDest[0]), Path.of(sourceDest[1]));
        }

        return true;
    }

    public static boolean backup(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        dao.backup();

        return true;
    }
}
