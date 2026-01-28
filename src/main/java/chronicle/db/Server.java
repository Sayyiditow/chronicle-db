package chronicle.db;

import static chronicle.db.service.ChronicleDb.CHRONICLE_DB;
import static chronicle.db.service.MapDb.MAP_DB;
import static chronicle.db.service.SequenceService.SEQUENCE_SERVICE;
import static chronicle.db.utils.ChronicleUtils.CHRONICLE_UTILS;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.tinylog.Logger;

import chronicle.db.config.KryoSerializer;
import chronicle.db.config.QueryMode;
import chronicle.db.service.ClientSocketService;
import chronicle.db.service.DeadlockService;
import chronicle.db.service.FailOver;
import chronicle.db.service.MigrationService;
import chronicle.db.service.ReplicationQueue;

public class Server {
    public static final class DataSocket {
        public final Socket socket;
        public final DataInputStream dis;
        public final DataOutputStream dos;

        public DataSocket(final Socket socket) throws IOException {
            this.socket = socket;
            this.dis = new DataInputStream(socket.getInputStream());
            this.dos = new DataOutputStream(socket.getOutputStream());
        }
    }

    public static final record StandbyServer(String url, int port, ClientSocketService dbService) {
    }

    public static final String DB_DATE_FORMAT = "yyyyMMddHHmmss";
    public static final DateTimeFormatter DB_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(DB_DATE_FORMAT);
    public static final AtomicBoolean SHUTDOWN_REQUESTED = new AtomicBoolean(false);
    public static String LOGS_DIR = "logs";

    private static final String appDir = "app";
    private static final Set<Thread> activeThreads = ConcurrentHashMap.newKeySet();
    private static final List<StandbyServer> standbyServers = new ArrayList<>();
    private static String processIdFile = LOGS_DIR + "/procid";
    private static String resourceDir = "src/main/resources/";
    private static boolean isHosted = false;
    private static boolean isPrimary = false;
    private static String dbPath;
    private static String dbArchPath;
    private static int port;
    private static int queueSize = 512;
    private static String fixedBatchTime;
    private static String dailyBatchTime;
    private static String dailyTransactionSummaryTime;
    private static String dailyEmtpyTrxLineCleanupTime;
    private static String dailyQueueCleanupTime;
    private static String[] dailySequenceResetNames;
    private static String monthlyTrxArchivalTime;
    private static boolean upgrading = false;
    private static ReplicationQueue replicationQueue = null;
    private static final Server DB_SERVER = new Server();

    private Properties loadProperties() {
        final Properties props = new Properties();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("server.properties")) {
            if (is == null) {
                Logger.error("Could not find server.properties");
                System.exit(1);
            }
            props.load(is);
        } catch (final IOException e) {
            Logger.error("Failed to load server.properties", e);
            System.exit(1);
        }
        return props;
    }

    private Server() {
        final var properties = loadProperties();

        if (!properties.isEmpty()) {
            port = Integer.parseInt(properties.getProperty("port"));
            dbPath = properties.getProperty("dbPath");
            dbArchPath = properties.getProperty("dbArchPath");
            queueSize = Integer.parseInt(properties.getProperty("queueSize", "512"));
            fixedBatchTime = properties.getProperty("fixedBatchTime", "-1");
            dailyBatchTime = properties.getProperty("dailyBatchTime", "-1");
            dailyTransactionSummaryTime = properties.getProperty("dailyTransactionSummaryTime", "-1");
            monthlyTrxArchivalTime = properties.getProperty("monthlyTrxArchivalTime", "-1");
            dailyEmtpyTrxLineCleanupTime = properties.getProperty("dailyEmtpyTrxLineCleanupTime", "-1");
            dailyQueueCleanupTime = properties.getProperty("dailyQueueCleanupTime", "-1");
            final var env = properties.getProperty("env");
            final var primary = properties.getProperty("primary");
            isHosted = !env.equals("dev");
            isPrimary = primary.equals("true");

            final var sequenceResetNames = properties.getProperty("dailySequenceResetNames");
            if (sequenceResetNames != null && !sequenceResetNames.isEmpty()) {
                dailySequenceResetNames = sequenceResetNames.split(",");
            }

            if (isHosted) {
                LOGS_DIR = "../logs";
                processIdFile = LOGS_DIR + "/procid";
                resourceDir = "../resources/";

                final var standbyUrls = properties.getProperty("standbyDbUrls");
                if (isPrimary && standbyUrls != null && !standbyUrls.isEmpty()) {
                    final var standbyUrlslArr = standbyUrls.split(",");
                    final var standbyPortsArr = properties.getProperty("standbyDbPorts").split(",");
                    final var standBySize = standbyUrlslArr.length;
                    final String[] tailerNames = new String[standBySize + 1];
                    for (int i = 0; i < standBySize; i++) {
                        final var url = standbyUrlslArr[i];
                        final var rawPort = standbyPortsArr[i];
                        final var port = Integer.parseInt(rawPort);
                        tailerNames[i] = ReplicationQueue.generateTailerName(url, rawPort);
                        standbyServers.add(new StandbyServer(url, port, new ClientSocketService(url, port, 1, 0)));
                    }
                    tailerNames[standBySize] = ReplicationQueue.getPrimaryTailerName();
                    replicationQueue = new ReplicationQueue(tailerNames);
                }
            }
        }
    }

    public static int getQueueSize() {
        return queueSize;
    }

    public static String getDbPath() {
        return dbPath;
    }

    public static String getDbArchPath() {
        return dbArchPath;
    }

    public static String getResourceDir() {
        return resourceDir;
    }

    public static List<String> getDbDirs() throws IOException {
        final var dbDirs = CHRONICLE_UTILS.getFileList(dbPath);
        dbDirs.remove(appDir);
        return dbDirs;
    }

    public static String getAppdir() {
        return appDir;
    }

    private static void closeSocketResources(final DataSocket dataSocket) {
        try {
            if (dataSocket != null) {
                if (dataSocket.dos != null) {
                    dataSocket.dos.close();
                }
                if (dataSocket.dis != null) {
                    dataSocket.dis.close();
                }
                if (dataSocket.socket != null) {
                    dataSocket.socket.close();
                }
            }
        } catch (final IOException ignored) {
        }
    }

    // Updated sendAllPendingWrites to be synchronous
    private static void sendAllPendingWrites(final String tailerName, final ClientSocketService dbService)
            throws InterruptedException {
        while (true) {
            int processedCount = 0;
            try {
                processedCount = replicationQueue.processPending(tailerName, data -> {
                    try {
                        if (!(boolean) dbService.execute(data)) {
                            Logger.warn("Replication for tailer [{}] failed - will retry. Action: {}", tailerName,
                                    KryoSerializer.deserialize(data));
                            Thread.sleep(2000);
                            return false;
                        }
                        return true;
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                });
            } catch (final Exception e) {
                Logger.error(e, "Error in processing pending writes for [{}]", tailerName);
            }

            if (processedCount > 0) {
                Logger.info("Processed [{}] writes for tailer [{}].", processedCount, tailerName);
            } else {
                try {
                    Thread.sleep(5000); // Pause 5 seconds if no writes processed
                } catch (final InterruptedException e) {
                    Logger.info("Replication thread for [{}] interrupted.", tailerName);
                    Thread.currentThread().interrupt();
                    return; // Exit thread on interrupt
                }
            }
        }
    }

    private static void sendAllPendingPrimaryWrites() throws InterruptedException {
        final var tailerName = ReplicationQueue.getPrimaryTailerName();

        // final int processedCount = replicationQueue.processPending(tailerName, data
        // -> {
        // try {
        // final var params = (Map<String, Object>) KryoSerializer.deserialize(data);
        // return handleReplicationRequest(params, (QueryMode) params.get("mode"));
        // } catch (final Throwable e) {
        // Logger.error("Failed to write pending primary writes.");
        // return false;
        // }
        // });

        // if (processedCount > 0) {
        // Logger.info("Processed [{}] pending writes for primary tailer.",
        // processedCount);
        // }
    }

    private static boolean isSafeToFailover(final String tailerName, final ClientSocketService dbService,
            final ConcurrentMap<String, ConcurrentMap<String, Integer>> currentDbCounts)
            throws InterruptedException {
        Logger.info("Failover test in progress for [{}].", tailerName);
        try {
            final boolean empty = replicationQueue.isEmpty(tailerName);
            if (!empty) {
                Logger.error("❌ Tailer [{}] has pending writes, failover is not safe.", tailerName);
                return false;
            }

            Logger.info("✅ Tailer [{}] is at end, no more pending writes. Checking counts.", tailerName);
            final int syncExitCode = new ProcessBuilder("sync").start().waitFor();
            if (syncExitCode != 0) {
                Logger.error("OS Sync failed! Data might be stale.");
            }

            // check counts
            final var countMap = ClientSocketService.prepareDbCountMap();
            final var standbyCounts = (ConcurrentMap<String, ConcurrentMap<String, Integer>>) dbService
                    .execute(countMap);
            final var toSync = FailOver.printDbCountsSideBySide(currentDbCounts, standbyCounts);
            if (!toSync.isEmpty()) {
                Logger.info("❌ Count mismatch, syncing remainig data for [{}].", tailerName);

                var safe = true;
                for (final var entry : toSync.entrySet()) {
                    final var dbDir = entry.getKey();
                    final var filePaths = entry.getValue();

                    for (final var filePath : filePaths) {
                        final Path path = Path.of(dbPath + "/" + dbDir + "/" + filePath);
                        safe &= FailOver.syncData(path, dbService.getDbUrl());
                    }
                }
                return safe;
            }

            Logger.info("Replicating sequences to [{}].", tailerName);
            final var query = ClientSocketService.prepareSequenceReplicationMap();
            final var sequences = SEQUENCE_SERVICE.getSequenceDb();
            ClientSocketService.addSequences(query, sequences);
            if ((boolean) dbService.execute(query)) {
                Logger.info("✅ Sequence replication to [{}] complete.", tailerName);
                return true;
            } else {
                Logger.error("❌ Sequence replication to [{}] failed.", tailerName);
                return false;
            }
        } catch (final IOException e) {
            Logger.error(e);
            return false;
        }
    }

    private static boolean isAllSafeToFailOver(final Map<String, String> fqnMap)
            throws InterruptedException, IOException {
        if (replicationQueue != null) {
            upgrading = true;

            // 1. Ensure Primary is up to date with its own WAL
            final String primaryTailer = ReplicationQueue.getPrimaryTailerName();
            if (!replicationQueue.isEmpty(primaryTailer)) {
                Logger.info("❌ Primary Tailer has pending writes. Retry in a while.");
                upgrading = false;
                return false;
            }
            Logger.info("✅ Primary Tailer is at end, no more pending writes. Checking standby DBs.");

            boolean safe = true;
            final var thisDbCounts = FailOver.getDbCounts(fqnMap);
            for (final var server : standbyServers) {
                final String tailerName = ReplicationQueue.generateTailerName(server.url(),
                        String.valueOf(server.port()));
                final var standByDbSafe = isSafeToFailover(tailerName, server.dbService(), thisDbCounts);
                if (standByDbSafe) {
                    Logger.info("DB at [{}] is up to date and safe to fail over to.", tailerName);
                }
                safe &= standByDbSafe;
            }

            Logger.info("All standby DBs are [{}] to fail over.", safe ? "safe" : "not safe");
            upgrading = false;
            return safe;
        }
        Logger.info("No standby DBs set.");
        return true;
    }

    private static void respond(final Map<String, Object> responseMap, final DataSocket dataSocket) throws IOException {
        // Serialize and send response
        final byte[] responseData = KryoSerializer.serialize(responseMap);
        dataSocket.dos.writeInt(responseData.length); // Send length prefix
        dataSocket.dos.write(responseData); // Send serialized response
        dataSocket.dos.flush();
    }

    // private static boolean handleReplicationRequest(final Map<String, Object>
    // params, final QueryMode queryMode)
    // throws Throwable {
    // switch (queryMode) {
    // case REPLICATE_PUT -> PUT_SERVICE.put(params);
    // case REPLICATE_INSERT -> PUT_SERVICE.insert(params);
    // case REPLICATE_UPDATE -> PUT_SERVICE.update(params);
    // case REPLICATE_PUT_MULTIPLE -> PUT_SERVICE.putMultiple(params);
    // case REPLICATE_UPDATE_MULTIPLE -> PUT_SERVICE.updateMultiple(params);
    // case REPLICATE_INSERT_ALL -> PUT_SERVICE.insertMultiple(params);
    // case REPLICATE_UPDATE_FILE -> PUT_SERVICE.updateFile(params);
    // case REPLICATE_DELETE -> DELETE_SERVICE.delete(params);
    // case REPLICATE_DELETE_MULTIPLE -> DELETE_SERVICE.deleteMultiple(params);
    // case REPLICATE_SEQUENCES -> PUT_SERVICE.replicateSequences(params);
    // case REPLICATE_MOVE_FILES -> PUT_SERVICE.moveFiles(params);
    // default -> throw new IllegalArgumentException("Unknown replication mode: " +
    // queryMode);
    // }
    // return true;
    // }

    private static void handleClient(final DataSocket dataSocket) throws Throwable {
        final int length = dataSocket.dis.readInt(); // Read length
        final byte[] data = new byte[length];
        dataSocket.dis.readFully(data); // Read exactly 'length' bytes
        final var params = (Map<String, Object>) KryoSerializer.deserialize(data);

        if (params == null || params.isEmpty()) {
            Logger.error("Received null/empty params from client");
            respond(Map.of("status", "400", "error", "Invalid request: null/empty params"), dataSocket);
            return;
        }

        if (upgrading) {
            Logger.info("DB is upgrading.");
            respond(Map.of("status", "503"), dataSocket);
            return;
        }

        final var modeObj = params.get("mode");
        if (modeObj == null) {
            Logger.error("Received request with null mode. Params keys: {}", params.keySet());
            respond(Map.of("status", "400", "error", "Invalid request: missing mode"), dataSocket);
            return;
        }

        final var queryMode = (QueryMode) modeObj;
        // this name is fixed and should never change.
        // final var response = switch (queryMode) {
        // // get
        // case GET -> GET_SERVICE.get(params);
        // case GET_JSON -> JsonUtils.toJsonFromObj(GET_SERVICE.get(params));
        // case GET_SUBSET -> GET_SERVICE.getSubset(params);
        // case GET_SUBSET_JSON ->
        // JsonUtils.toJsonFromObj(GET_SERVICE.getSubset(params));
        // case GET_ALL -> GET_SERVICE.getAll(params);
        // case GET_ALL_JSON -> JsonUtils.toJsonFromObj(GET_SERVICE.getAll(params));
        // case GET_ALL_CSV -> GET_SERVICE.getAllCsv(params);
        // case GET_ALL_CSV_JSON ->
        // JsonUtils.toJsonFromObj(GET_SERVICE.getAllCsv(params));
        // case GET_ALL_SUBSET -> GET_SERVICE.getAllSubset(params);
        // case GET_ALL_SUBSET_JSON ->
        // JsonUtils.toJsonFromObj(GET_SERVICE.getAllSubset(params));
        // case GET_ALL_SUBSET_CSV -> GET_SERVICE.getAllSubsetCsv(params);
        // case GET_ALL_SUBSET_CSV_JSON ->
        // JsonUtils.toJsonFromObj(GET_SERVICE.getAllSubsetCsv(params));
        // case GET_ARCHIVE_PERIODS ->
        // CHRONICLE_DAO_SERVICE.getAvailableArchivePeriods(params.get("dbDir").toString(),
        // (ObjectEnum) params.get("objectEnum"));
        // case FETCH -> GET_SERVICE.fetch(params);
        // case FETCH_KEYS -> GET_SERVICE.fetchKeys(params);
        // case FETCH_KEYS_LIST -> GET_SERVICE.fetchKeysList(params);
        // case FETCH_JSON -> JsonUtils.toJsonFromObj(GET_SERVICE.fetch(params));
        // case FETCH_CSV -> GET_SERVICE.fetchCsv(params);
        // case FETCH_CSV_JSON -> JsonUtils.toJsonFromObj(GET_SERVICE.fetchCsv(params));
        // case FETCH_SUBSET -> GET_SERVICE.fetchSubset(params);
        // case FETCH_SUBSET_JSON ->
        // JsonUtils.toJsonFromObj(GET_SERVICE.fetchSubset(params));
        // case FETCH_SUBSET_CSV -> GET_SERVICE.fetchSubsetCsv(params);
        // case FETCH_SUBSET_CSV_JSON ->
        // JsonUtils.toJsonFromObj(GET_SERVICE.fetchSubsetCsv(params));
        // case SEARCH -> GET_SERVICE.search(params);
        // case SEARCH_KEYS -> GET_SERVICE.searchKeys(params);
        // case SEARCH_KEYS_LIST -> GET_SERVICE.searchKeysList(params);
        // case SEARCH_JSON -> JsonUtils.toJsonFromObj(GET_SERVICE.search(params));
        // case SEARCH_CSV -> GET_SERVICE.searchCsv(params);
        // case SEARCH_CSV_JSON ->
        // JsonUtils.toJsonFromObj(GET_SERVICE.searchCsv(params));
        // case SEARCH_SUBSET -> GET_SERVICE.searchSubset(params);
        // case SEARCH_SUBSET_JSON ->
        // JsonUtils.toJsonFromObj(GET_SERVICE.searchSubset(params));
        // case SEARCH_SUBSET_CSV -> GET_SERVICE.searchSubsetCsv(params);
        // case SEARCH_SUBSET_CSV_JSON ->
        // JsonUtils.toJsonFromObj(GET_SERVICE.searchSubsetCsv(params));
        // case SEARCH_NON_INDEXED -> GET_SERVICE.searchOneNonIndexed(params);
        // case SEARCH_NON_INDEXED_KEYS -> GET_SERVICE.searchOneNonIndexedKeys(params);
        // case SEARCH_NON_INDEXED_KEYS_LIST ->
        // GET_SERVICE.searchOneNonIndexedKeysList(params);
        // case SEARCH_INDEXED -> GET_SERVICE.searchOneIndexed(params);
        // case SEARCH_INDEXED_KEYS -> GET_SERVICE.searchOneIndexedKeys(params);
        // case SEARCH_INDEXED_KEYS_LIST ->
        // GET_SERVICE.searchOneIndexedKeysList(params);
        // case GET_FILE -> GET_SERVICE.getFile(params);
        // case GET_FILES -> GET_SERVICE.getFiles(params);
        // case EXISTS -> GET_SERVICE.exists(params);
        // case EXISTS_MULTIPLE -> GET_SERVICE.existsMultiple(params);
        // case EXISTS_LIST -> GET_SERVICE.existsList(params);
        // case NOT_EXISTS -> GET_SERVICE.notExists(params);
        // case NOT_EXISTS_LIST -> GET_SERVICE.notExistsList(params);
        // // put
        // case PUT -> {
        // final var keyObj = params.get("key");
        // if (keyObj == null) {
        // Logger.warn("PUT request missing 'key' parameter.");
        // yield PutStatus.FAILED;
        // }

        // final var key = keyObj.toString();
        // final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        // final var value = PUT_SERVICE.preparePutValue(params);
        // final var safeSupplier = new SafeSupplier<PutStatus>(() -> dao.put(key,
        // value), "PUT - " + key,
        // PutStatus.FAILED);
        // yield ReplicationQueue.prepareAndCall(replicationQueue, params,
        // QueryMode.REPLICATE_PUT, key, value,
        // safeSupplier);
        // }
        // case PUT_JSON -> {
        // final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        // final var keyObj = params.get("key");
        // if (keyObj == null) {
        // Logger.warn("PUT_JSON request missing 'key' parameter.");
        // yield PutStatus.FAILED;
        // }

        // final var key = keyObj.toString();
        // final var value = PUT_SERVICE.preparePutJsonValue(params, dao.jsonType());
        // final var safeSupplier = new SafeSupplier<PutStatus>(() -> dao.put(key,
        // value), "PUT_JSON - " + key,
        // PutStatus.FAILED);
        // yield ReplicationQueue.prepareAndCall(replicationQueue, params,
        // QueryMode.REPLICATE_PUT, key, value,
        // safeSupplier);
        // }
        // case INSERT -> {
        // final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        // final var keyObj = params.get("key");
        // if (keyObj == null) {
        // Logger.warn("INSERT request missing 'key' parameter.");
        // yield PutStatus.FAILED;
        // }

        // final var key = keyObj.toString();
        // final var value = PUT_SERVICE.preparePutValue(params);
        // final var safeSupplier = new SafeSupplier<PutStatus>(() -> dao.insert(key,
        // value), "INSERT - " + key,
        // PutStatus.FAILED);
        // yield ReplicationQueue.prepareAndCall(replicationQueue, params,
        // QueryMode.REPLICATE_INSERT, key, value,
        // safeSupplier);
        // }
        // case UPDATE -> {
        // final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        // final var keyObj = params.get("key");
        // if (keyObj == null) {
        // Logger.warn("UPDATE request missing 'key' parameter.");
        // yield PutStatus.FAILED;
        // }

        // final var key = keyObj.toString();
        // final var value = params.get("value");
        // final var safeSupplier = new SafeSupplier<PutStatus>(() -> dao.update(key,
        // value), "UPDATE - " + key,
        // PutStatus.FAILED);
        // yield ReplicationQueue.prepareAndCall(replicationQueue, params,
        // QueryMode.REPLICATE_UPDATE, key, value,
        // safeSupplier);
        // }
        // case UPDATE_JSON -> {
        // final var keyObj = params.get("key");
        // if (keyObj == null) {
        // Logger.warn("UPDATE_JSON request missing 'key' parameter.");
        // yield PutStatus.FAILED;
        // }

        // final var key = keyObj.toString();
        // final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        // final var value = dao.get(key);

        // if (value == null) {
        // yield PutStatus.FAILED;
        // }
        // final var updatedValue = PUT_SERVICE.prepareUpdateJsonValue(params,
        // dao.jsonType(), value);
        // final var safeSupplier = new SafeSupplier<PutStatus>(() -> dao.update(key,
        // updatedValue),
        // "UPDATE_JSON - " + key, PutStatus.FAILED);
        // yield ReplicationQueue.prepareAndCall(replicationQueue, params,
        // QueryMode.REPLICATE_UPDATE, key,
        // updatedValue, safeSupplier);
        // }
        // case PUT_MULTIPLE -> {
        // final var objects = (Map<String, Object>) params.get("objects");

        // if (objects == null || objects.isEmpty()) {
        // yield PutStatus.FAILED;
        // }
        // final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        // final var updatedObjects = PUT_SERVICE.preparePutMultipleObjects(dao, params,
        // objects);
        // final var safeSupplier = new SafeSupplier<PutStatus>(() ->
        // dao.put(updatedObjects),
        // "PUT_MULTIPLE - " + params.get("objectEnum"), PutStatus.FAILED);
        // yield ReplicationQueue.prepareAndCall(replicationQueue, params,
        // QueryMode.REPLICATE_PUT_MULTIPLE,
        // updatedObjects, safeSupplier);
        // }
        // case PUT_MULTIPLE_JSON -> {
        // final var objects = (Map<String, Object>) params.get("objects");
        // if (objects == null || objects.isEmpty()) {
        // yield PutStatus.FAILED;
        // }
        // final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        // final var updatedObjects = PUT_SERVICE.preparePutMultipleJsonObjects(dao,
        // params, objects);
        // final var safeSupplier = new SafeSupplier<PutStatus>(() ->
        // dao.put(updatedObjects),
        // "PUT_MULTIPLE_JSON - " + params.get("objectEnum"), PutStatus.FAILED);
        // yield ReplicationQueue.prepareAndCall(replicationQueue, params,
        // QueryMode.REPLICATE_PUT_MULTIPLE,
        // updatedObjects, safeSupplier);
        // }
        // case UPDATE_MULTIPLE -> {
        // final var objects = (Map<String, Object>) params.get("objects");
        // if (objects == null || objects.isEmpty()) {
        // yield PutStatus.FAILED;
        // }
        // final var safeSupplier = new SafeSupplier<PutStatus>(() ->
        // PUT_SERVICE.updateMultiple(params),
        // "UPDATE_MULTIPLE - " + params.get("objectEnum"), PutStatus.FAILED);
        // yield ReplicationQueue.prepareAndCall(replicationQueue, params,
        // QueryMode.REPLICATE_UPDATE_MULTIPLE,
        // objects, safeSupplier);
        // }
        // case UPDATE_MULTIPLE_JSON -> {
        // final var objects = (Map<String, Object>) params.get("objects");
        // if (objects == null || objects.isEmpty()) {
        // yield PutStatus.FAILED;
        // }
        // final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        // final var updatedObjects = PUT_SERVICE.prepareUpdateMultipleJsonObjects(dao,
        // params, objects);
        // final var safeSupplier = new SafeSupplier<PutStatus>(() ->
        // dao.update(updatedObjects),
        // "UPDATE_MULTIPLE_JSON - " + params.get("objectEnum"), PutStatus.FAILED);
        // yield ReplicationQueue.prepareAndCall(replicationQueue, params,
        // QueryMode.REPLICATE_UPDATE_MULTIPLE,
        // updatedObjects, safeSupplier);
        // }
        // case INSERT_ALL -> {
        // final var objects = (Map<String, Object>) params.get("objects");
        // if (objects == null || objects.isEmpty()) {
        // yield PutStatus.FAILED;
        // }
        // final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
        // final var updatedObjects = PUT_SERVICE.prepareInsertAllObjects(params,
        // objects);
        // final var safeSupplier = new SafeSupplier<PutStatus>(() ->
        // dao.insert(updatedObjects),
        // "INSERT_ALL - " + params.get("objectEnum"), PutStatus.FAILED);
        // yield ReplicationQueue.prepareAndCall(replicationQueue, params,
        // QueryMode.REPLICATE_INSERT_ALL,
        // updatedObjects, safeSupplier);
        // }
        // case UPDATE_FILE -> {
        // final var safeSupplier = new SafeSupplier<Boolean>(() ->
        // PUT_SERVICE.updateFile(params),
        // "UPDATE_FILE - " + params.get("fileName"), false);
        // yield ReplicationQueue.prepareAndCall(replicationQueue, params,
        // QueryMode.REPLICATE_UPDATE_FILE,
        // safeSupplier);
        // }
        // case MOVE_FILES -> {
        // final var safeSupplier = new SafeSupplier<Boolean>(() ->
        // PUT_SERVICE.moveFiles(params), "MOVE_FILES",
        // false);
        // yield ReplicationQueue.prepareAndCall(replicationQueue, params,
        // QueryMode.REPLICATE_MOVE_FILES,
        // safeSupplier);
        // }
        // // delete
        // case DELETE -> {
        // final var keyObj = params.get("key");
        // if (keyObj == null && params.get("search") == null) {
        // Logger.warn("DELETE request missing 'key' and 'search' parameters.");
        // yield false;
        // }
        // final var key = Objects.toString(keyObj, "NO KEY");
        // final var safeSupplier = new SafeSupplier<Boolean>(() ->
        // DELETE_SERVICE.delete(params),
        // "DELETE - " + key, false);
        // yield ReplicationQueue.prepareAndCall(replicationQueue, params,
        // QueryMode.REPLICATE_DELETE,
        // safeSupplier);
        // }
        // case DELETE_MULTIPLE -> {
        // final var safeSupplier = new SafeSupplier<Boolean>(() ->
        // DELETE_SERVICE.deleteMultiple(params),
        // "DELETE_MULTIPLE", false);
        // yield ReplicationQueue.prepareAndCall(replicationQueue, params,
        // QueryMode.REPLICATE_DELETE_MULTIPLE,
        // safeSupplier);
        // }
        // // replicate
        // case REPLICATE_PUT, REPLICATE_INSERT, REPLICATE_UPDATE,
        // REPLICATE_PUT_MULTIPLE, REPLICATE_UPDATE_MULTIPLE,
        // REPLICATE_INSERT_ALL, REPLICATE_UPDATE_FILE, REPLICATE_DELETE,
        // REPLICATE_DELETE_MULTIPLE,
        // REPLICATE_SEQUENCES, REPLICATE_MOVE_FILES ->
        // handleReplicationRequest(params, queryMode);
        // // misc
        // case RUN_TASK -> {
        // String taskClassName = params.get("taskClass").toString();
        // try {
        // // Instantiate the task from the classpath
        // DbTask task = (DbTask)
        // Class.forName(taskClassName).getDeclaredConstructor().newInstance();
        // yield task.execute(params);
        // } catch (Throwable e) {
        // Logger.error("Failed to execute task [{}]: {}", taskClassName,
        // e.getMessage());
        // yield null;
        // }
        // }
        // case MONITORING_COUNT ->
        // MONITORING_COUNT_SERVICE.getTrxCount(params.get("tenantId").toString(),
        // params.get("invoiceDateSearch").toString(),
        // params.get("supplier").toString(),
        // params.get("source").toString());
        // case TENANT_LIST -> GET_SERVICE.getAllTenants();
        // case ENTRIES -> GET_SERVICE.entries(params);
        // case REFRESH_INDEXES -> {
        // final var safeSupplier = new SafeSupplier<Boolean>(() ->
        // PUT_SERVICE.refreshIndexes(params),
        // "REFRESH_INDEXES - " + params.get("objectEnum"), false);
        // yield ReplicationQueue.call(replicationQueue, params, safeSupplier);
        // }
        // case REFRESH_KEY_MAP -> {
        // final var safeSupplier = new SafeSupplier<Boolean>(() ->
        // PUT_SERVICE.refreshKeyMap(params),
        // "REFRESH_KEY_MAP - " + params.get("objectEnum"), false);
        // yield ReplicationQueue.call(replicationQueue, params, safeSupplier);
        // }
        // case TRUNCATE -> {
        // final var safeSupplier = new SafeSupplier<Boolean>(() ->
        // DELETE_SERVICE.truncate(params),
        // "TRUNCATE - " + params.get("objectEnum"), false);
        // yield ReplicationQueue.call(replicationQueue, params, safeSupplier);
        // }
        // case VACUUM_CANDIDATES -> DB_BATCH_SERVICE.printVacuumCandidates();
        // case VACUUM -> {
        // final var safeSupplier = new SafeSupplier<Boolean>(() ->
        // DELETE_SERVICE.vacuum(params),
        // "VACUUM - " + params.get("objectEnum"), false);
        // yield ReplicationQueue.call(replicationQueue, params, safeSupplier);
        // }
        // case RESIZE -> {
        // final var safeSupplier = new SafeSupplier<Boolean>(() ->
        // ChronicleResizeService.CHRONICLE_RESIZE_SERVICE
        // .resize(params.get("dbDir").toString(),
        // ObjectEnum.valueOf(params.get("objectEnum").toString()),
        // params.get("fileName").toString(),
        // Long.valueOf(params.get("newSize").toString())),
        // "RESIZE - " + params.get("objectEnum"), false);
        // yield ReplicationQueue.call(replicationQueue, params, safeSupplier);
        // }
        // case DASHBOARD_COUNT ->
        // DB_BATCH_SERVICE.updateTransactionSummary(params.get("tenantId").toString(),
        // params.get("startDate").toString(), params.get("endDate").toString(),
        // Objects.toString(params.get("archivePeriod"), ""), replicationQueue);
        // case EMPTY_TRX_CLEANUP ->
        // DB_BATCH_SERVICE.cleanupEmptyLineInvoices(params.get("tenantId").toString(),
        // replicationQueue);
        // case UPDATE_INVOICE_DATES ->
        // DB_BATCH_SERVICE.updateInvoiceDates(params.get("tenantId").toString(),
        // params.get("source").toString(), params.get("status").toString(),
        // params.get("startDate").toString(), params.get("endDate").toString(),
        // params.get("updatedDate").toString(), replicationQueue);
        // case MISSING_SUBMISSION_UIDS ->
        // DB_BATCH_SERVICE.fixMissingSubmissionUids(params.get("tenantId").toString(),
        // replicationQueue);
        // case REVERT_ORIGINAL_REF ->
        // DB_BATCH_SERVICE.revertOriginalRefUuid(params.get("tenantId").toString(),
        // params.get("status").toString(), replicationQueue);
        // case CONSOLIDATE_TRX_BY_IDS ->
        // DB_BATCH_SERVICE.consolidateInvoicesById(params.get("tenantId").toString(),
        // (Map<InvoiceTypeEnum, Set<String>>) params.get("trxIds"), replicationQueue);
        // case CONSOLIDATE_MONTHLY_TRX ->
        // DB_BATCH_SERVICE.consolidateMonthlyTrx(params.get("tenantId").toString(),
        // params.get("source").toString(), replicationQueue);
        // case MOVE_INVOICES_TO_NEW ->
        // DB_BATCH_SERVICE.moveInvoicesBackToNewStage(params.get("tenantId").toString(),
        // params.get("status").toString(), replicationQueue);
        // case UPDATE_PREFERRED_CONTACTS ->
        // DB_BATCH_SERVICE.updatePreferredContacts(params.get("tenantId").toString(),
        // replicationQueue);
        // case BACKUP_MAIN_SUPPLIER ->
        // DB_BATCH_SERVICE.backupMainSuppliers(params.get("tenantId").toString());
        // case COMPANY_COUNT ->
        // MONITORING_COUNT_SERVICE.getCompanyCount(params.get("tenantId").toString());
        // case ARCHIVE_TRX ->
        // DB_BATCH_SERVICE.archiveTransactions(params.get("yearMonth").toString(),
        // Boolean.parseBoolean(params.get("vacuum").toString()), replicationQueue);
        // case ARCHIVE_SUBMISSIONS ->
        // DB_BATCH_SERVICE.archiveOldSubmissions(params.get("tenantId").toString(),
        // replicationQueue);
        // case VALIDATE_INVOICE_TOTALS ->
        // DB_BATCH_SERVICE.validateInvoiceTotals(params.get("tenantId").toString());
        // case DB_COUNT -> FailOver.getDbCounts();
        // case FAIL_OVER -> isAllSafeToFailOver((Map<String,
        // String>)params.get("fqnMap"));
        // case BACKUP -> PUT_SERVICE.backup(params);
        // default -> null;
        // };

        // Prepare response map
        final Map<String, Object> responseMap = new HashMap<>();
        responseMap.put("status", "200");
        responseMap.put("response", null);
        respond(responseMap, dataSocket);
    }

    public static void main(final String[] args) throws Throwable {
        // Write procId to disk
        CHRONICLE_UTILS.logProcessId(processIdFile);

        // Process any leftover writes from previous run synchronously
        if (replicationQueue != null) {
            // send any pending writes
            sendAllPendingPrimaryWrites();

            Logger.info("Starting background replication threads for [{}] standby server(s)...", standbyServers.size());
            for (final var server : standbyServers) {
                final String tailerName = ReplicationQueue.generateTailerName(server.url(),
                        String.valueOf(server.port()));
                Thread.ofVirtual().start(() -> {
                    try {
                        sendAllPendingWrites(tailerName, server.dbService());
                    } catch (final InterruptedException e) {
                        Logger.info("Replication thread for [{}] interrupted.", tailerName);
                        Thread.currentThread().interrupt();
                    }
                });
            }
        }

        // tasks that run only once on DB start
        DeadlockService.DEADLOCK_SERVICE.recoverDeadLocks();
        MigrationService.MIGRATION_SERVICE.migrateObjects();

        // Add shutdown hook to close all queues
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                final var mesg = CHRONICLE_UTILS.getCurrentDate()
                        + " INFO: Shutdown requested. Signal sent to db services. \n";
                Files.writeString(Path.of(LOGS_DIR + "/info.log"), mesg, StandardOpenOption.APPEND);
            } catch (final IOException e) {
            }

            // set to true to close long running tasks in loops
            SHUTDOWN_REQUESTED.set(true);

            // close replication queue
            if (replicationQueue != null) {
                replicationQueue.close();
            }

            // Interrupt all tracked threads
            for (final Thread thread : activeThreads) {
                if (thread.isAlive()) {
                    thread.interrupt();
                }
            }

            // Wait a bit more for interrupted threads to exit
            try {
                Thread.sleep(1000);
            } catch (final InterruptedException e) {
                // Ignore
            }

            // close all DB and Indexes
            CHRONICLE_DB.closeAll();
            MAP_DB.closeAllIndexes();
            MAP_DB.closeAllMaps();

            try {
                final var mesg = CHRONICLE_UTILS.getCurrentDate() + " INFO: Shutdown complete. \n";
                Files.writeString(Path.of(LOGS_DIR + "/info.log"), mesg, StandardOpenOption.APPEND);
            } catch (final IOException e) {
            }
        }));

        try (final ServerSocket serverSocket = new ServerSocket(port)) {
            Logger.info("DB Server is listening on port [{}].", port);

            while (true) {// Loop to handle multiple sockets
                try {
                    final Socket socket = serverSocket.accept();
                    final var dataSocket = new DataSocket(socket);
                    final var addr = socket.getInetAddress();
                    Logger.info("New client connection from [{}].", addr);
                    socket.setTcpNoDelay(true);
                    socket.setKeepAlive(true);
                    // Disable timeout for standby DBs to avoid idle disconnection
                    socket.setSoTimeout(isPrimary ? 600000 : 0);

                    final var thread = Thread.ofVirtual().unstarted(() -> {
                        try {
                            while (true) {
                                // Loop to handle multiple requests on same socket
                                // Check shutdown flag
                                if (SHUTDOWN_REQUESTED.get()) {
                                    Logger.info("Shutdown requested, closing connection from [{}]", addr);
                                    break;
                                }
                                handleClient(dataSocket);
                            }
                        } catch (final EOFException e) {
                            // Client disconnected, socket closed remotely
                        } catch (final SocketException | SocketTimeoutException e) {
                            // Connection lost, socket unusable, client will close n reconnect
                        } catch (final NullPointerException e) {
                            Logger.error("NullPointerException while handling client from [{}]", addr);
                            Logger.error(e);
                        } catch (final Throwable e) {
                            Logger.error("Error handling client from [{}]: {}", addr, e.getMessage());
                            Logger.error(e);
                        } finally {
                            Logger.info("Client connection is exiting from [{}].", addr);
                            activeThreads.remove(Thread.currentThread());
                            closeSocketResources(dataSocket);
                        }
                    });

                    activeThreads.add(thread);
                    thread.start();
                } catch (final IOException e) {
                    Logger.error("Error accepting connection: {}", e.getMessage());
                }
            }
        }
    }
}
