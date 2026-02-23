package chronicle.db.service;

import static chronicle.db.service.ChronicleDaoService.CHRONICLE_DAO_SERVICE;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.tinylog.Logger;

import chronicle.db.entity.Search;

@SuppressWarnings("unchecked")
public final class DeleteService {
    private DeleteService() {
    }

    public static boolean delete(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        final var searches = (List<Search>) params.get("search");

        if (searches != null) {
            // Just find all the keys here easily
            final var keys = dao.multiSearchKeysList(searches);
            if (!keys.isEmpty()) {
                return dao.delete(keys);
            }
            return false;
        }

        return dao.delete(params.get("key").toString());
    }

    public static boolean deleteMultiple(final Map<String, Object> params) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        return dao.delete((Collection<String>) params.get("keys"));
    }

    public static boolean truncate(final Map<String, Object> params) {
        try {
            final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
            dao.truncate();
            return true;
        } catch (final Throwable t) {
            Logger.error("Error during truncate.");
            Logger.error(t);
            return false;
        }
    }

    public static boolean vacuum(final Map<String, Object> params) {
        try {
            final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
            dao.vacuum();
            return true;
        } catch (final Throwable t) {
            Logger.error("Error during vacuum.");
            Logger.error(t);
            return false;
        }
    }

    public static boolean resize(final String fqn, final String dbDir, final String filePath, final String fileName,
            final long newSize) throws Throwable {
        final var dao = CHRONICLE_DAO_SERVICE.getDao(fqn, dbDir, filePath);

        dao.resizeDb(fileName, newSize);
        return true;
    }
}
