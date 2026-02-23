package chronicle.db.service;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.tinylog.Logger;

import com.jsoniter.spi.TypeLiteral;

import chronicle.db.Server;
import chronicle.db.utils.JsonUtils;

public final class DeadlockService {
    private DeadlockService() {
    }

    public static void recoverDeadLocks() throws Throwable {
        final var deadlocksJsonPath = Server.getResourceDir() + "/deadlocks.json";
        final var config = JsonUtils.fromJsonFileToObj(deadlocksJsonPath,
                new TypeLiteral<List<Map<String, String>>>() {
                });
        final int configSize = config.size();

        if (configSize > 0) {
            Logger.info("\n-----------Releasing Deadlocks-----------");
        }

        for (final var map : config) {
            final var fqn = map.get("fqn");
            final var dbDir = map.get("dbDir");
            final var filePath = map.get("filePath");
            final var fileName = map.get("fileName");
            final var dao = ChronicleDaoService.CHRONICLE_DAO_SERVICE.getDao(fqn, dbDir, filePath);
            dao.recoverData(fileName);
        }

        if (configSize > 0) {
            JsonUtils.toJsonFileFromObj(deadlocksJsonPath, Collections.emptyList());
            Logger.info("\n-----------Deadlocks Released-----------");
        }
    }
}
