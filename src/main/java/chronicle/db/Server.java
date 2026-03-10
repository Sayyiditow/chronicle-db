package chronicle.db;

import static chronicle.db.service.ChronicleDaoService.CHRONICLE_DAO_SERVICE;
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
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.tinylog.Logger;

import chronicle.db.config.ForySerializer;
import chronicle.db.config.QueryMode;
import chronicle.db.dao.ChronicleDao;
import chronicle.db.entity.PutStatus;
import chronicle.db.service.ClientSocketService;
import chronicle.db.service.DeadlockService;
import chronicle.db.service.DeleteService;
import chronicle.db.service.FailOver;
import chronicle.db.service.GetService;
import chronicle.db.service.MigrationService;
import chronicle.db.service.PutService;
import chronicle.db.service.ReplicationQueue;
import chronicle.db.service.TaskLoader;
import chronicle.db.service.VacuumService;
import chronicle.db.utils.JsonUtils;
import chronicle.db.utils.SafeSupplier;

@SuppressWarnings("unchecked")
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

    public static final AtomicBoolean SHUTDOWN_REQUESTED = new AtomicBoolean(false);

    private static final String dbDateFormat = "yyyyMMddHHmmss";
    private static final DateTimeFormatter dbDateTimeFormatter = DateTimeFormatter.ofPattern(dbDateFormat);
    private static String LOGS_DIR = "logs";
    private static String processId = LOGS_DIR + "/procid";
    private static final String appDir = "app";
    private static final Set<Thread> activeThreads = ConcurrentHashMap.newKeySet();
    private static final List<StandbyServer> standbyServers = new ArrayList<>();
    private static String resourceDir = "src/main/resources/";
    private static boolean isHosted = false;
    private static boolean isPrimary = false;
    private static String dbPath;
    private static String dbArchPath;
    private static int port;
    private static int queueSize = 512;
    private static boolean upgrading = false;
    public static boolean replicationEnabled = false;
    private static ReplicationQueue replicationQueue = null;
    private static final int batchSizeMedium = Integer.getInteger("chronicle.db.batchSizeMedium", 20_000);
    private static final int batchSizeLarge = Integer.getInteger("chronicle.db.batchSizeLarge", 50_000);
    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    static {
        final var properties = loadProperties();

        if (!properties.isEmpty()) {
            port = Integer.parseInt(properties.getProperty("port"));
            dbPath = properties.getProperty("dbPath");
            dbArchPath = properties.getProperty("dbArchPath");
            queueSize = Integer.parseInt(properties.getProperty("queueSize", "512"));
            final var env = properties.getProperty("env");
            final var primary = properties.getProperty("primary");
            isHosted = !env.equals("dev");
            isPrimary = primary.equals("true");

            if (isHosted) {
                LOGS_DIR = "../logs";
                processId = LOGS_DIR + "/procid";
                resourceDir = "../resources/";

                final var standbyUrls = properties.getProperty("standbyDbUrls");
                if (isPrimary && standbyUrls != null && !standbyUrls.isEmpty()) {
                    replicationEnabled = Boolean.parseBoolean(properties.getProperty("replication", "false"));
                    final var standbyUrlslArr = standbyUrls.split(",");
                    final var standbyPortsArr = properties.getProperty("standbyDbPorts").split(",");
                    final Set<String> tailerNames = new HashSet<>();
                    tailerNames.add(ReplicationQueue.getPrimaryTailerName());
                    for (int i = 0; i < standbyUrlslArr.length; i++) {
                        final var url = standbyUrlslArr[i];
                        final var rawPort = standbyPortsArr[i];
                        final var standbyPort = Integer.parseInt(rawPort);
                        tailerNames.add(ReplicationQueue.generateTailerName(url, rawPort));
                        if (replicationEnabled) {
                            standbyServers.add(new StandbyServer(url, standbyPort,
                                    new ClientSocketService(url, standbyPort, 1, 0)));
                        }
                    }
                    replicationQueue = new ReplicationQueue(tailerNames);
                }
            }
        }
    }

    private static Properties loadProperties() {
        final Properties props = new Properties();
        try (InputStream is = Server.class.getClassLoader().getResourceAsStream("server.properties")) {
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

    public static int getBatchSizeMedium() {
        return batchSizeMedium;
    }

    public static int getBatchSizeLarge() {
        return batchSizeLarge;
    }

    public static String getDbDateFormat() {
        return dbDateFormat;
    }

    public static DateTimeFormatter getDbDateTimeFormatter() {
        return dbDateTimeFormatter;
    }

    private static void scheduleQueueCleanup() {
        if (replicationQueue == null) {
            return;
        }

        // Run daily at 3:00 AM
        final var now = LocalDateTime.now();
        var nextRun = now.toLocalDate().atTime(LocalTime.of(3, 0));
        if (now.isAfter(nextRun)) {
            nextRun = nextRun.plusDays(1);
        }

        final long initialDelayMinutes = Duration.between(now, nextRun).toMinutes();
        final long oneDayMinutes = TimeUnit.DAYS.toMinutes(1);

        scheduler.scheduleAtFixedRate(() -> {
            try {
                Logger.info("Starting daily queue cleanup...");
                replicationQueue.cleanupQueue();
                Logger.info("Queue cleanup completed.");
            } catch (final IOException e) {
                Logger.error(e, "Queue cleanup failed.");
            }
        }, initialDelayMinutes, oneDayMinutes, TimeUnit.MINUTES);

        Logger.info("Queue cleanup scheduled daily at 03:00. Next run in [{}] minutes.", initialDelayMinutes);
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
                        dbService.execute(data);
                        return true; // Standby received and processed the command
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return false;
                    } catch (final Exception e) {
                        // Connection/network failure - retry
                        Logger.warn("Replication for tailer [{}] failed - will retry. Error: {}", tailerName,
                                e.getMessage());
                        try {
                            Thread.sleep(2000);
                        } catch (final InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
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

        final int processedCount = replicationQueue.processPending(tailerName, data -> {
            try {
                final var params = (Map<String, Object>) ForySerializer.deserialize(data);
                executeRequest(params, null); // null = don't re-queue
                return true;
            } catch (final Throwable e) {
                Logger.error("Failed to write pending primary writes.");
                Logger.error(e);
                return false;
            }
        });

        if (processedCount > 0) {
            Logger.info("Processed [{}] pending writes for primary tailer.", processedCount);
        }
    }

    private static boolean isSafeToFailover(final Map<String, List<String>> fqnMap, final String tailerName,
            final ClientSocketService dbService,
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
            final var countMap = ClientSocketService.prepareDbCountMap(fqnMap);
            final var standbyCounts = (ConcurrentMap<String, ConcurrentMap<String, Integer>>) dbService
                    .execute(countMap);
            final var toSync = FailOver.printDbCountsSideBySide(fqnMap, currentDbCounts, standbyCounts);
            if (!toSync.isEmpty()) {
                Logger.info("❌ Count mismatch, syncing remaining data for [{}].", tailerName);

                var safe = true;
                for (final var entry : toSync.entrySet()) {
                    final var keys = entry.getValue();
                    for (final var key : keys) {
                        safe &= FailOver.syncData(Path.of(key), dbService.getDbUrl());
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

    private static boolean isAllSafeToFailOver(final Map<String, List<String>> fqnMap)
            throws InterruptedException, IOException {
        if (replicationQueue != null) {
            upgrading = true;

            // 1. Ensure Primary is up to date with its own WAL
            final String primaryTailer = ReplicationQueue.getPrimaryTailerName();
            if (!replicationQueue.isEmpty(primaryTailer)) {
                Logger.info("❌ Primary tailer has pending writes. Retry in a while.");
                upgrading = false;
                return false;
            }
            Logger.info("✅ Primary tailer is at end, no more pending writes. Checking standby DBs.");

            boolean safe = true;
            final var thisDbCounts = FailOver.getDbCounts(fqnMap);
            for (final var server : standbyServers) {
                final String tailerName = ReplicationQueue.generateTailerName(server.url(),
                        String.valueOf(server.port()));
                final var standByDbSafe = isSafeToFailover(fqnMap, tailerName, server.dbService(), thisDbCounts);
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

    private static Object executeRequest(final Map<String, Object> params) throws Throwable {
        return executeRequest(params, replicationQueue);
    }

    private static Object executeRequest(final Map<String, Object> params, final ReplicationQueue queue)
            throws Throwable {
        final var queryMode = (QueryMode) params.get("mode");
        return switch (queryMode) {
            // get
            case GET -> GetService.get(params);
            case GET_JSON -> JsonUtils.toJsonFromObj(GetService.get(params));
            case GET_SUBSET -> GetService.getSubset(params);
            case GET_SUBSET_JSON -> JsonUtils.toJsonFromObj(GetService.getSubset(params));
            case GET_ALL -> GetService.getAll(params);
            case GET_ALL_JSON -> JsonUtils.toJsonFromObj(GetService.getAll(params));
            case GET_ALL_CSV -> GetService.getAllCsv(params);
            case GET_ALL_CSV_JSON -> JsonUtils.toJsonFromObj(GetService.getAllCsv(params));
            case GET_ALL_SUBSET -> GetService.getAllSubset(params);
            case GET_ALL_SUBSET_JSON -> JsonUtils.toJsonFromObj(GetService.getAllSubset(params));
            case GET_ALL_SUBSET_CSV -> GetService.getAllSubsetCsv(params);
            case GET_ALL_SUBSET_CSV_JSON -> JsonUtils.toJsonFromObj(GetService.getAllSubsetCsv(params));
            case GET_ARCHIVE_PERIODS -> CHRONICLE_DAO_SERVICE.getAvailableArchivePeriods(params.get("dbDir").toString(),
                    params.get("filePath").toString());
            case FETCH -> GetService.fetch(params);
            case FETCH_KEYS -> GetService.fetchKeys(params);
            case FETCH_KEYS_LIST -> GetService.fetchKeysList(params);
            case FETCH_JSON -> JsonUtils.toJsonFromObj(GetService.fetch(params));
            case FETCH_CSV -> GetService.fetchCsv(params);
            case FETCH_CSV_JSON -> JsonUtils.toJsonFromObj(GetService.fetchCsv(params));
            case FETCH_SUBSET -> GetService.fetchSubset(params);
            case FETCH_SUBSET_JSON -> JsonUtils.toJsonFromObj(GetService.fetchSubset(params));
            case FETCH_SUBSET_CSV -> GetService.fetchSubsetCsv(params);
            case FETCH_SUBSET_CSV_JSON -> JsonUtils.toJsonFromObj(GetService.fetchSubsetCsv(params));
            case SEARCH -> GetService.search(params);
            case SEARCH_KEYS -> GetService.searchKeys(params);
            case SEARCH_KEYS_LIST -> GetService.searchKeysList(params);
            case SEARCH_JSON -> JsonUtils.toJsonFromObj(GetService.search(params));
            case SEARCH_CSV -> GetService.searchCsv(params);
            case SEARCH_CSV_JSON -> JsonUtils.toJsonFromObj(GetService.searchCsv(params));
            case SEARCH_SUBSET -> GetService.searchSubset(params);
            case SEARCH_SUBSET_JSON -> JsonUtils.toJsonFromObj(GetService.searchSubset(params));
            case SEARCH_SUBSET_CSV -> GetService.searchSubsetCsv(params);
            case SEARCH_SUBSET_CSV_JSON -> JsonUtils.toJsonFromObj(GetService.searchSubsetCsv(params));
            case SEARCH_NON_INDEXED -> GetService.searchOneNonIndexed(params);
            case SEARCH_NON_INDEXED_KEYS -> GetService.searchOneNonIndexedKeys(params);
            case SEARCH_NON_INDEXED_KEYS_LIST -> GetService.searchOneNonIndexedKeysList(params);
            case SEARCH_INDEXED -> GetService.searchOneIndexed(params);
            case SEARCH_INDEXED_KEYS -> GetService.searchOneIndexedKeys(params);
            case SEARCH_INDEXED_KEYS_LIST -> GetService.searchOneIndexedKeysList(params);
            case SEARCH_COUNT -> GetService.searchCount(params);
            case GET_FILE -> GetService.getFile(params);
            case GET_FILES -> GetService.getFiles(params);
            case EXISTS -> GetService.exists(params);
            case EXISTS_MAP -> GetService.existsMap(params);
            case EXISTS_SET -> GetService.existsSet(params);
            case EXISTS_LIST -> GetService.existsList(params);
            case NOT_EXISTS -> GetService.notExists(params);
            case NOT_EXISTS_LIST -> GetService.notExistsList(params);
            case DB_DIRS -> GetService.getAllDbDirs();
            case ENTRIES -> GetService.entries(params);
            // sequence
            case SEQUENCE_NEXT -> {
                final var sequenceName = params.get("sequenceName").toString();
                final var seqLen = params.get("seqLen");
                if (seqLen != null) {
                    yield SEQUENCE_SERVICE.getSequence(sequenceName, (int) seqLen);
                }
                yield SEQUENCE_SERVICE.getSequence(sequenceName);
            }
            case SEQUENCE_NEXT_BATCH -> {
                final var sequenceName = params.get("sequenceName").toString();
                final var count = (int) params.get("count");
                final var seqLen = params.get("seqLen");
                if (seqLen != null) {
                    yield SEQUENCE_SERVICE.getSequences(sequenceName, count, (int) seqLen);
                }
                yield SEQUENCE_SERVICE.getSequences(sequenceName, count);
            }
            case SEQUENCE_CURRENT -> {
                final var sequenceName = params.get("sequenceName").toString();
                final var sequenceDb = SEQUENCE_SERVICE.getSequenceDb();
                yield sequenceDb.getOrDefault(sequenceName, 0L);
            }
            case SEQUENCE_RESET -> {
                final var sequenceName = params.get("sequenceName").toString();
                SEQUENCE_SERVICE.resetSequence(sequenceName);
                yield true;
            }
            // put
            case PUT -> {
                final var keyObj = params.get("key");
                if (keyObj == null) {
                    Logger.warn("PUT request missing 'key' parameter.");
                    yield PutStatus.FAILED;
                }

                final var key = keyObj.toString();
                final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
                final var value = params.get("value");
                final var safeSupplier = new SafeSupplier<PutStatus>(() -> dao.put(key, value), "PUT - " + key,
                        PutStatus.FAILED);
                yield ReplicationQueue.call(queue, params, safeSupplier);
            }
            case PUT_JSON -> {
                final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
                final var keyObj = params.get("key");
                if (keyObj == null) {
                    Logger.warn("PUT_JSON request missing 'key' parameter.");
                    yield PutStatus.FAILED;
                }

                final var key = keyObj.toString();
                final var value = JsonUtils.fromJsonToObj(params.get("value").toString(), dao.jsonType());
                final var safeSupplier = new SafeSupplier<PutStatus>(() -> dao.put(key, value), "PUT_JSON - " + key,
                        PutStatus.FAILED);
                yield ReplicationQueue.call(queue, params, safeSupplier);
            }
            case INSERT -> {
                final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
                final var keyObj = params.get("key");
                if (keyObj == null) {
                    Logger.warn("INSERT request missing 'key' parameter.");
                    yield PutStatus.FAILED;
                }

                final var key = keyObj.toString();
                final var value = params.get("value");
                final var safeSupplier = new SafeSupplier<PutStatus>(() -> dao.insert(key, value), "INSERT - " + key,
                        PutStatus.FAILED);
                yield ReplicationQueue.call(queue, params, safeSupplier);
            }
            case UPDATE -> {
                final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
                final var keyObj = params.get("key");
                if (keyObj == null) {
                    Logger.warn("UPDATE request missing 'key' parameter.");
                    yield PutStatus.FAILED;
                }

                final var key = keyObj.toString();
                final var value = params.get("value");
                final var safeSupplier = new SafeSupplier<PutStatus>(() -> dao.update(key, value), "UPDATE - " + key,
                        PutStatus.FAILED);
                yield ReplicationQueue.call(queue, params, safeSupplier);
            }
            case UPDATE_JSON -> {
                final var keyObj = params.get("key");
                if (keyObj == null) {
                    Logger.warn("UPDATE_JSON request missing 'key' parameter.");
                    yield PutStatus.FAILED;
                }

                final var key = keyObj.toString();
                final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
                final var value = dao.get(key);

                if (value == null) {
                    yield PutStatus.FAILED;
                }
                final var updatedValue = JsonUtils.fromJsonToObjMerge(params.get("value").toString(), dao.jsonType(),
                        value);
                final var safeSupplier = new SafeSupplier<PutStatus>(() -> dao.update(key, updatedValue),
                        "UPDATE_JSON - " + key, PutStatus.FAILED);
                yield ReplicationQueue.call(queue, params, safeSupplier);
            }
            case PUT_MULTIPLE -> {
                final var objects = (Map<String, Object>) params.get("objects");
                if (objects == null || objects.isEmpty()) {
                    yield PutStatus.FAILED;
                }
                final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
                final var safeSupplier = new SafeSupplier<PutStatus>(() -> dao.put(objects),
                        "PUT_MULTIPLE - " + dao.dataPath(), PutStatus.FAILED);
                yield ReplicationQueue.call(queue, params, safeSupplier);
            }
            case PUT_MULTIPLE_JSON -> {
                final var objects = (Map<String, Object>) params.get("objects");
                if (objects == null || objects.isEmpty()) {
                    yield PutStatus.FAILED;
                }
                final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
                final Map<String, Object> preparedObjects = new HashMap<>();

                for (final var entry : objects.entrySet()) {
                    final var key = entry.getKey();
                    final var existingValue = dao.get(key);

                    if (existingValue != null) {
                        // Merge for updates - preserves fields not in the incoming JSON
                        final var mergedValue = JsonUtils.fromJsonToObjMerge(entry.getValue().toString(),
                                dao.jsonType(), existingValue);
                        preparedObjects.put(key, mergedValue);
                    } else {
                        // Direct parse for inserts
                        final var newValue = JsonUtils.fromJsonToObj(entry.getValue().toString(), dao.jsonType());
                        preparedObjects.put(key, newValue);
                    }
                }
                final var safeSupplier = new SafeSupplier<PutStatus>(() -> dao.put(preparedObjects),
                        "PUT_MULTIPLE_JSON - " + dao.dataPath(), PutStatus.FAILED);
                yield ReplicationQueue.call(queue, params, safeSupplier);
            }
            case UPDATE_MULTIPLE -> {
                final var objects = (Map<String, Object>) params.get("objects");
                if (objects == null || objects.isEmpty()) {
                    yield PutStatus.FAILED;
                }
                final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
                final var safeSupplier = new SafeSupplier<PutStatus>(() -> dao.update(objects),
                        "UPDATE_MULTIPLE - " + dao.dataPath(), PutStatus.FAILED);
                yield ReplicationQueue.call(queue, params, safeSupplier);
            }
            case UPDATE_MULTIPLE_JSON -> {
                final var objects = (Map<String, Object>) params.get("objects");
                if (objects == null || objects.isEmpty()) {
                    yield PutStatus.FAILED;
                }
                final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
                final Map<String, Object> updatedObjects = new HashMap<>();

                for (final var entry : objects.entrySet()) {
                    final var key = entry.getKey();
                    final var existingValue = dao.get(key);
                    if (existingValue != null) {
                        final var mergedValue = JsonUtils.fromJsonToObjMerge(entry.getValue().toString(),
                                dao.jsonType(), existingValue);
                        updatedObjects.put(key, mergedValue);
                    }
                }

                if (updatedObjects.isEmpty()) {
                    yield PutStatus.FAILED;
                }

                final var safeSupplier = new SafeSupplier<PutStatus>(() -> dao.update(updatedObjects),
                        "UPDATE_MULTIPLE_JSON - " + dao.dataPath(), PutStatus.FAILED);
                yield ReplicationQueue.call(queue, params, safeSupplier);
            }
            case INSERT_ALL -> {
                final var objects = (Map<String, Object>) params.get("objects");
                if (objects == null || objects.isEmpty()) {
                    yield PutStatus.FAILED;
                }
                final var dao = CHRONICLE_DAO_SERVICE.getDao(params);
                final var safeSupplier = new SafeSupplier<PutStatus>(() -> dao.insert(objects),
                        "INSERT_ALL - " + dao.dataPath(), PutStatus.FAILED);
                yield ReplicationQueue.call(queue, params, safeSupplier);
            }
            case UPDATE_FILE -> {
                final var safeSupplier = new SafeSupplier<Boolean>(() -> PutService.updateFile(params),
                        "UPDATE_FILE - " + params.get("fileName"), false);
                yield ReplicationQueue.call(queue, params, safeSupplier);
            }
            case MOVE_FILES -> {
                final var safeSupplier = new SafeSupplier<Boolean>(() -> PutService.moveFiles(params), "MOVE_FILES",
                        false);
                yield ReplicationQueue.call(queue, params, safeSupplier);
            }
            // delete
            case DELETE -> {
                final var keyObj = params.get("key");
                if (keyObj == null && params.get("search") == null) {
                    Logger.warn("DELETE request missing 'key' and 'search' parameters.");
                    yield false;
                }
                final var key = Objects.toString(keyObj, "NO KEY");
                final var safeSupplier = new SafeSupplier<Boolean>(() -> DeleteService.delete(params),
                        "DELETE - " + key, false);
                yield ReplicationQueue.call(queue, params, safeSupplier);
            }
            case DELETE_MULTIPLE -> {
                final var safeSupplier = new SafeSupplier<Boolean>(() -> DeleteService.deleteMultiple(params),
                        "DELETE_MULTIPLE", false);
                yield ReplicationQueue.call(queue, params, safeSupplier);
            }
            case REFRESH_INDEXES -> {
                final var safeSupplier = new SafeSupplier<Boolean>(() -> PutService.refreshIndexes(params),
                        "REFRESH_INDEXES - " + params.get("filePath"), false);
                yield ReplicationQueue.call(queue, params, safeSupplier);
            }
            case REFRESH_KEY_MAP -> {
                final var safeSupplier = new SafeSupplier<Boolean>(() -> PutService.refreshKeyMap(params),
                        "REFRESH_KEY_MAP - " + params.get("filePath"), false);
                yield ReplicationQueue.call(queue, params, safeSupplier);
            }
            case TRUNCATE -> {
                final var safeSupplier = new SafeSupplier<Boolean>(() -> DeleteService.truncate(params),
                        "TRUNCATE - " + params.get("filePath"), false);
                yield ReplicationQueue.call(queue, params, safeSupplier);
            }
            case VACUUM_CANDIDATES ->
                VacuumService.printVacuumCandidates((Map<String, List<String>>) params.get("fqnMap"));
            case VACUUM -> {
                final var safeSupplier = new SafeSupplier<Boolean>(() -> DeleteService.vacuum(params),
                        "VACUUM - " + params.get("filePath"), false);
                yield ReplicationQueue.call(queue, params, safeSupplier);
            }
            case RESIZE -> {
                final var safeSupplier = new SafeSupplier<Boolean>(
                        () -> DeleteService.resize(params.get("fqn").toString(), params.get("dbDir").toString(),
                                params.get("filePath").toString(), params.get("fileName").toString(),
                                Long.valueOf(params.get("newSize").toString())),
                        "RESIZE - " + params.get("filePath"), false);
                yield ReplicationQueue.call(queue, params, safeSupplier);
            }
            // misc
            case DB_COUNT -> FailOver.getDbCounts((Map<String, List<String>>) params.get("fqnMap"));
            case FAIL_OVER -> isAllSafeToFailOver((Map<String, List<String>>) params.get("fqnMap"));
            case UPDATE_SEQUENCES -> PutService.updateSequences(params);
            case BACKUP -> PutService.backup(params);
            case EXECUTE_TASK -> {
                final var taskClass = params.get("taskClass").toString();
                final var task = TaskLoader.loadTask(taskClass);
                final var taskParams = (Map<String, Object>) params.get("params");
                yield task.execute(taskParams, queue);
            }
            case REFRESH_TASKS -> {
                TaskLoader.refresh();
                yield true;
            }
            default -> null;
        };
    }

    private static void respond(final Map<String, Object> responseMap, final DataSocket dataSocket) throws IOException {
        // Serialize and send response
        final byte[] responseData = ForySerializer.serialize(responseMap);
        dataSocket.dos.writeInt(responseData.length); // Send length prefix
        dataSocket.dos.write(responseData); // Send serialized response
        dataSocket.dos.flush();
    }

    private static void handleClient(final DataSocket dataSocket) throws Throwable {
        final int length = dataSocket.dis.readInt(); // Read length
        final byte[] data = new byte[length];
        dataSocket.dis.readFully(data); // Read exactly 'length' bytes
        final var params = (Map<String, Object>) ForySerializer.deserialize(data);

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

        final var response = executeRequest(params);

        // Prepare response map
        final Map<String, Object> responseMap = new HashMap<>();
        responseMap.put("status", "200");
        responseMap.put("response", response);
        respond(responseMap, dataSocket);
    }

    public static void main(final String[] args) throws Throwable {
        // Write procId to disk
        CHRONICLE_UTILS.logProcessId(processId);

        // Process any leftover writes from previous run synchronously
        if (replicationQueue != null) {
            // Check for manual recovery replay (e.g., after power outage)
            if (Boolean.getBoolean("chronicle.queue.replay")) {
                Logger.warn("Recovery replay enabled via -Dchronicle.queue.replay=true");
                replicationQueue.resetAllTailersToLastWrittenCycle();
            }

            // send any pending writes
            sendAllPendingPrimaryWrites();

            if (replicationEnabled) {
                Logger.info("Starting background replication threads for [{}] standby server(s)...",
                        standbyServers.size());
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
            } else {
                Logger.info("Replication disabled in config. Standby threads will not start.");
            }
        }

        // Schedule daily queue cleanup
        scheduleQueueCleanup();

        // tasks that run only once on DB start
        DeadlockService.recoverDeadLocks();
        MigrationService.migrateObjects();
        TaskLoader.refresh();

        final var infoLogPath = Path.of(LOGS_DIR, "info.log");

        // Add shutdown hook to close all queues
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                final var mesg = "---------INFO: Shutdown requested. Signal sent to db services.---------\n";
                Files.writeString(infoLogPath, mesg);
            } catch (final IOException e) {
            }

            // set to true to close long running tasks in loops
            SHUTDOWN_REQUESTED.set(true);

            // block new requests at server level
            upgrading = true;

            // wait for in-flight writes to complete
            int inFlight = ChronicleDao.IN_FLIGHT_WRITES.get();
            if (inFlight > 0) {
                try {
                    Files.writeString(infoLogPath, "INFO: [" + inFlight + "] In-flight writes pending. Waiting...\n");
                } catch (final IOException e) {
                }
            }
            while (inFlight > 0) {
                try {
                    Thread.sleep(50);
                } catch (final InterruptedException e) {
                    break;
                }
                inFlight = ChronicleDao.IN_FLIGHT_WRITES.get();
            }

            // close replication queue
            if (replicationQueue != null) {
                replicationQueue.close();
            }

            // shutdown scheduler
            scheduler.shutdownNow();

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
                final var mesg = "---------INFO: Shutdown complete.---------\n";
                Files.writeString(infoLogPath, mesg);
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
                            activeThreads.remove(Thread.currentThread());
                            closeSocketResources(dataSocket);
                            Logger.info("Client connection is exiting from [{}]. Active connections: [{}]", addr,
                                    activeThreads.size());
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
