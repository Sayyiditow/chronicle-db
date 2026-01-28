package chronicle.db.service;

import static chronicle.db.utils.ChronicleUtils.CHRONICLE_UTILS;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.tinylog.Logger;

import com.jsoniter.spi.TypeLiteral;

import chronicle.db.Server;

public class DeadlockService {
    private DeadlockService() {
    }

    public static final DeadlockService DEADLOCK_SERVICE = new DeadlockService();

    public void recoverDeadLocks() throws Throwable {
        final var deadlocksJsonPath = Server.getResourceDir() + "/deadlocks.json";
        final var config = CHRONICLE_UTILS.fromJsonFileToObj(deadlocksJsonPath,
                new TypeLiteral<List<Map<String, String>>>() {
                });
        final int configSize = config.size();

        if (configSize > 0) {
            Logger.info("\n-----------Releasing Deadlocks-----------");
        }

        for (final var map : config) {
            final var fqn = map.get("fqn");
            final var filePath = map.get("filePath");
            final var fileName = map.get("fileName");
            final var dao = ChronicleDaoService.CHRONICLE_DAO_SERVICE.getDao(fqn, filePath);
            dao.recoverData(fileName);
        }

        if (configSize > 0) {
            CHRONICLE_UTILS.toJsonFileFromObj(deadlocksJsonPath, Collections.emptyList());
            Logger.info("\n-----------Deadlocks Released-----------");
        }
    }
}
