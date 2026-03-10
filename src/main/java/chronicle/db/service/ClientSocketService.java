package chronicle.db.service;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.tinylog.Logger;

import chronicle.db.config.ForySerializer;
import chronicle.db.config.QueryMode;
import chronicle.db.entity.Search;

public class ClientSocketService {
    public static final class PooledSocket {
        public final int index;
        public final Socket socket;
        public final DataInputStream dis;
        public final DataOutputStream dos;

        public PooledSocket(final int index, final Socket socket) throws IOException {
            this.index = index;
            this.socket = socket;
            this.dis = new DataInputStream(socket.getInputStream());
            this.dos = new DataOutputStream(socket.getOutputStream());
        }
    }

    private final BlockingQueue<PooledSocket> connections;
    private final String dbUrl;
    private final int dbPort;
    private final int poolSize;
    private final int waitTimeout = 2000;
    private final int socketTimeout;

    public ClientSocketService(final String dbUrl, final int dbPort, final int poolSize, final int socketTimeout) {
        this.dbUrl = dbUrl;
        this.dbPort = dbPort;
        this.poolSize = poolSize;
        this.socketTimeout = socketTimeout;
        connections = new LinkedBlockingQueue<>(poolSize);
        initializePool();
    }

    public String getDbUrl() {
        return dbUrl;
    }

    private boolean isSocketValid(final PooledSocket pooledSocket) {
        try {
            return pooledSocket != null &&
                    pooledSocket.socket != null &&
                    !pooledSocket.socket.isClosed() &&
                    pooledSocket.socket.isConnected() &&
                    !pooledSocket.socket.isInputShutdown() &&
                    !pooledSocket.socket.isOutputShutdown();
        } catch (final Exception e) {
            return false;
        }
    }

    private void initializePool() {
        Logger.info("Initializing DB connection pool with [{}] socket(s).", poolSize);
        for (int i = 0; i < poolSize; i++) {
            createAndAddSocket(i);
        }
    }

    public void closePool() {
        // Close all connections currently in the pool
        PooledSocket socket;
        while ((socket = connections.poll()) != null) {
            closeSocketResources(socket);
        }

        Logger.info("Closed all {} connections in the socket pool", poolSize);
    }

    private static void closeSocketResources(final PooledSocket pooledSocket) {
        try {
            if (pooledSocket != null) {
                if (pooledSocket.dos != null) {
                    pooledSocket.dos.close();
                }
                if (pooledSocket.dis != null) {
                    pooledSocket.dis.close();
                }
                if (pooledSocket.socket != null) {
                    pooledSocket.socket.close();
                }
            }
        } catch (final IOException ignored) {
        }
    }

    private void createAndAddSocket(final int i) {
        try {
            final Socket socket = new Socket(dbUrl, dbPort);
            socket.setKeepAlive(true);
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(socketTimeout);
            final PooledSocket ps = new PooledSocket(i, socket);
            if (!connections.offer(ps)) {
                closeSocketResources(ps);
            }
        } catch (final IOException e) {
            Logger.warn("Failed to initialize pool: {}. Retrying in {}s...", e.getMessage(), waitTimeout / 1000);
            retryCreateSocket(i);
        }
    }

    private void retryCreateSocket(final int i) {
        try {
            Thread.sleep(waitTimeout);
            createAndAddSocket(i);
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private PooledSocket borrowSocket() throws InterruptedException {
        PooledSocket pooledSocket = connections.take();
        if (!isSocketValid(pooledSocket)) {
            closeSocketResources(pooledSocket);
            Logger.debug("Socket unavailable or stale, creating new one.");
            pooledSocket = createNewPooledSocketForever(pooledSocket.index);
        }
        return pooledSocket;
    }

    private PooledSocket createNewPooledSocketForever(final int index) throws InterruptedException {
        while (true) {
            try {
                final Socket socket = new Socket(dbUrl, dbPort);
                socket.setKeepAlive(true);
                socket.setTcpNoDelay(true);
                socket.setSoTimeout(socketTimeout);
                return new PooledSocket(index, socket);
            } catch (final IOException e) {
                Logger.warn("Socket creation failed: {}. Retrying in {}s...", e.getMessage(), waitTimeout / 1000);
                Thread.sleep(waitTimeout);
            }
        }
    }

    private void renewSocket(final PooledSocket pooledSocket) throws InterruptedException {
        closeSocketResources(pooledSocket);
        final var newSocket = createNewPooledSocketForever(pooledSocket.index);

        // Forcefully add back to pool (even if full, evict oldest if needed)
        while (connections.size() >= poolSize) {
            closeSocketResources(connections.poll()); // Remove oldest if full
        }
        connections.offer(newSocket); // Now guaranteed to succeed
    }

    private void returnSocket(final PooledSocket pooledSocket) throws InterruptedException {
        final PooledSocket socketToReturn;
        if (!isSocketValid(pooledSocket)) {
            closeSocketResources(pooledSocket);
            socketToReturn = createNewPooledSocketForever(pooledSocket.index);
        } else {
            socketToReturn = pooledSocket;
        }

        // Forcefully add back to pool (even if full, evict oldest if needed)
        while (connections.size() >= poolSize) {
            closeSocketResources(connections.poll()); // Remove oldest if full
        }
        connections.offer(socketToReturn); // Now guaranteed to succeed
    }

    public static String getClassNameFromFilePath(final String filePath) {
        return filePath.substring(filePath.lastIndexOf('/') + 1);
    }

    public static Map<String, Object> prepareTruncateMap(final String fqn, final String dbDir) {
        if (fqn == null || fqn.isBlank() || dbDir == null || dbDir.isBlank()) {
            throw new IllegalArgumentException("fqn or dbDir cannot be empty/null.");
        }
        final var map = new HashMap<String, Object>();
        map.put("fqn", fqn);
        map.put("mode", QueryMode.TRUNCATE);
        map.put("isArchived", false);
        map.put("dbDir", dbDir);
        return map;
    }

    public static Map<String, Object> prepareArchivePeriodQueryMap(final String dbDir, final String filePath) {
        if (dbDir == null || dbDir.isBlank()) {
            throw new IllegalArgumentException("Database Directory (dbDir) cannot be null or empty.");
        }
        final var map = new HashMap<String, Object>();
        map.put("dbDir", dbDir);
        map.put("filePath", filePath);
        map.put("mode", QueryMode.GET_ARCHIVE_PERIODS);
        map.put("isArchived", false);

        return map;
    }

    public static Map<String, Object> prepareQueryMap(final String fqn, final QueryMode queryMode,
            final String dbDir, final String filePath) {
        if (fqn == null || fqn.isBlank() || dbDir == null || dbDir.isBlank()) {
            throw new IllegalArgumentException("fqn or dbDir cannot be empty/null.");
        }
        final var map = new HashMap<String, Object>();
        map.put("fqn", fqn);
        map.put("mode", queryMode);
        map.put("isArchived", false);
        map.put("dbDir", dbDir);
        map.put("filePath", filePath);
        return map;
    }

    public static Map<String, Object> prepareSequenceReplicationMap() {
        final var map = new HashMap<String, Object>();
        map.put("mode", QueryMode.UPDATE_SEQUENCES);
        return map;
    }

    public static Map<String, Object> prepareDbCountMap(final Map<String, List<String>> fqnMap) {
        final var map = new HashMap<String, Object>();
        map.put("mode", QueryMode.DB_COUNT);
        map.put("fqnMap", fqnMap);
        return map;
    }

    public static Map<String, Object> prepareSequenceNextMap(final String sequenceName) {
        final var map = new HashMap<String, Object>();
        map.put("mode", QueryMode.SEQUENCE_NEXT);
        map.put("sequenceName", sequenceName);
        return map;
    }

    public static Map<String, Object> prepareSequenceNextMap(final String sequenceName, final int seqLen) {
        final var map = new HashMap<String, Object>();
        map.put("mode", QueryMode.SEQUENCE_NEXT);
        map.put("sequenceName", sequenceName);
        map.put("seqLen", seqLen);
        return map;
    }

    public static Map<String, Object> prepareSequenceNextBatchMap(final String sequenceName, final int count) {
        final var map = new HashMap<String, Object>();
        map.put("mode", QueryMode.SEQUENCE_NEXT_BATCH);
        map.put("sequenceName", sequenceName);
        map.put("count", count);
        return map;
    }

    public static Map<String, Object> prepareSequenceNextBatchMap(final String sequenceName, final int count,
            final int seqLen) {
        final var map = new HashMap<String, Object>();
        map.put("mode", QueryMode.SEQUENCE_NEXT_BATCH);
        map.put("sequenceName", sequenceName);
        map.put("count", count);
        map.put("seqLen", seqLen);
        return map;
    }

    public static Map<String, Object> prepareSequenceCurrentMap(final String sequenceName) {
        final var map = new HashMap<String, Object>();
        map.put("mode", QueryMode.SEQUENCE_CURRENT);
        map.put("sequenceName", sequenceName);
        return map;
    }

    public static Map<String, Object> prepareSequenceResetMap(final String sequenceName) {
        final var map = new HashMap<String, Object>();
        map.put("mode", QueryMode.SEQUENCE_RESET);
        map.put("sequenceName", sequenceName);
        return map;
    }

    public static Map<String, Object> prepareDbDirsMap() {
        final var map = new HashMap<String, Object>();
        map.put("mode", QueryMode.DB_DIRS);
        return map;
    }

    public static Map<String, Object> prepareMoveFiles(final List<String[]> paths) {
        if (paths == null || paths.isEmpty()) {
            throw new IllegalArgumentException("Paths cannot be null or empty.");
        }
        final var map = new HashMap<String, Object>();
        map.put("mode", QueryMode.MOVE_FILES);
        map.put("paths", paths);
        return map;
    }

    public static Map<String, Object> prepareTaskMap(final String taskClass, final Map<String, Object> params) {
        final var map = new HashMap<String, Object>();
        map.put("mode", QueryMode.EXECUTE_TASK);
        map.put("taskClass", taskClass);
        map.put("params", params);
        return map;
    }

    public static void updateFilePath(final Map<String, Object> queryMap, final String filePath) {
        if (filePath == null || filePath.isBlank()) {
            throw new IllegalArgumentException("filePath cannot be null or empty.");
        }
        queryMap.put("filePath", filePath);
    }

    public static void updateQueryMode(final Map<String, Object> queryMap, final QueryMode mode) {
        queryMap.put("mode", mode);
    }

    public static void updateFqn(final Map<String, Object> queryMap, final String fqn) {
        queryMap.put("fqn", fqn);
    }

    public static void updateDbDir(final Map<String, Object> queryMap, final String dbDir) {
        queryMap.put("dbDir", dbDir);
    }

    public static void addKey(final Map<String, Object> queryMap, final String key) {
        queryMap.put("key", key);
    }

    public static void addValue(final Map<String, Object> queryMap, final Object value) {
        queryMap.put("value", value);
    }

    public static void addKeyAndValue(final Map<String, Object> queryMap, final String key, final Object value) {
        queryMap.put("key", key);
        queryMap.put("value", value);
    }

    public static void addKeys(final Map<String, Object> queryMap, final Collection<String> keys) {
        queryMap.put("keys", keys);
    }

    public static void addExcludedKeys(final Map<String, Object> queryMap, final Set<String> keys) {
        queryMap.put("excludedKeys", keys);
    }

    public static void removeKeys(final Map<String, Object> queryMap) {
        queryMap.remove("keys");
    }

    public static void addSearch(final Map<String, Object> queryMap, final Search search) {
        queryMap.put("search", search);
    }

    public static void addSearch(final Map<String, Object> queryMap, final List<Search> search) {
        queryMap.put("search", search);
    }

    public static void addSubsetFields(final Map<String, Object> queryMap, final String[] subsetFields) {
        queryMap.put("subsetFields", subsetFields);
    }

    public static void addDb(final Map<String, Object> queryMap, final Object db) {
        queryMap.put("db", db);
    }

    public static void addFileName(final Map<String, Object> queryMap, final String fileName) {
        queryMap.put("fileName", fileName);
    }

    public static void addFileNames(final Map<String, Object> queryMap, final List<String> fileNames) {
        queryMap.put("fileNames", fileNames);
    }

    public static void addFile(final Map<String, Object> queryMap, final byte[] file) {
        queryMap.put("file", file);
    }

    public static void addFileAndName(final Map<String, Object> queryMap, final byte[] file, final String fileName) {
        queryMap.put("file", file);
        queryMap.put("fileName", fileName);
    }

    public static void addLimit(final Map<String, Object> queryMap, final int limit) {
        queryMap.put("limit", limit);
    }

    public static void addObjects(final Map<String, Object> queryMap, final Map<String, ?> objects) {
        queryMap.put("objects", objects);
    }

    public static void addSequences(final Map<String, Object> queryMap, final Map<String, Long> sequences) {
        queryMap.put("sequences", sequences);
    }

    public static void addIsArchived(final Map<String, Object> queryMap, final String archivePeriod) {
        if (archivePeriod != null && !archivePeriod.isBlank()
                && !archivePeriod.equals("null") && !archivePeriod.contains("missing")) {
            queryMap.put("isArchived", true);
            queryMap.put("archivePeriod", archivePeriod);
        }
    }

    public static void addPaths(final Map<String, Object> queryMap, final List<String[]> paths) {
        queryMap.put("paths", paths);
    }

    public Object execute(final Map<String, Object> queryMap) throws InterruptedException {
        final var pooledSocket = borrowSocket();
        try {
            final var data = ForySerializer.serialize(queryMap);
            // Send the query to the server
            pooledSocket.dos.writeInt(data.length); // Send length first
            pooledSocket.dos.write(data);
            pooledSocket.dos.flush();

            final int length = pooledSocket.dis.readInt(); // Read length
            final byte[] disData = new byte[length];
            pooledSocket.dis.readFully(disData); // Read exactly 'length' bytes
            final var responseMap = (Map<String, Object>) ForySerializer.deserialize(disData);

            if (responseMap == null) {
                return null;
            }

            final var status = responseMap.get("status").toString();
            if ("200".equals(status)) {
                return responseMap.get("response");
            } else if ("400".equals(status)) {
                Logger.error("DB returned 400 (bad request). Error: {}", responseMap.get("error"));
                return null; // Don't retry - bad request is a code issue
            } else if ("503".equals(status)) {
                Logger.info("DB is upgrading. Reconnecting...");
                Thread.sleep(waitTimeout);
                returnSocket(pooledSocket);
                return execute(queryMap); // retry
            } else {
                Logger.warn("DB returned unexpected status [{}]. Not retrying.", status);
                return null;
            }
        } catch (final EOFException eofException) {
            renewSocket(pooledSocket); // immediately
            Logger.info("Server [{}:{}] closed idle connection (EOF). Renewing socket...", dbUrl, dbPort);
            return execute(queryMap); // Retry with fresh socket
        } catch (final SocketTimeoutException e) {
            renewSocket(pooledSocket); // immediately
            Logger.warn("SocketTimeoutException: {}. Renewing socket...", e.getMessage());
            return null;
        } catch (final SocketException e) {
            Logger.warn("SocketException: {}. Renewing socket...", e.getMessage());
            renewSocket(pooledSocket); // immediately
            return execute(queryMap); // Retry with fresh socket
        } catch (final IOException e) {
            Logger.error("Socket IOException: {}. Renewing socket...", e.getMessage());
            returnSocket(pooledSocket);
            return execute(queryMap); // retry
        } finally {
            returnSocket(pooledSocket);
        }
    }

    public Object execute(final byte[] data) throws InterruptedException {
        final var pooledSocket = borrowSocket();
        try {
            pooledSocket.dos.writeInt(data.length); // Send length first
            pooledSocket.dos.write(data);
            pooledSocket.dos.flush();

            final int length = pooledSocket.dis.readInt(); // Read length
            final byte[] disData = new byte[length];
            pooledSocket.dis.readFully(disData); // Read exactly 'length' bytes
            final var responseMap = (Map<String, Object>) ForySerializer.deserialize(disData);

            if (responseMap == null) {
                return null;
            }

            final var status = responseMap.get("status").toString();
            if ("200".equals(status)) {
                return responseMap.get("response");
            } else if ("400".equals(status)) {
                Logger.error("DB returned 400 (bad request). Error: {}", responseMap.get("error"));
                return null; // Don't retry - bad request is a code issue
            } else if ("503".equals(status)) {
                Logger.info("DB is upgrading. Reconnecting...");
                Thread.sleep(waitTimeout);
                returnSocket(pooledSocket);
                return execute(data); // retry
            } else {
                Logger.warn("DB returned unexpected status [{}]. Not retrying.", status);
                return null;
            }
        } catch (final EOFException eofException) {
            renewSocket(pooledSocket); // immediately
            Logger.info("Server [{}:{}] closed idle connection (EOF). Renewing socket...", dbUrl, dbPort);
            return execute(data); // Retry with fresh socket
        } catch (final SocketTimeoutException e) {
            renewSocket(pooledSocket); // immediately
            Logger.warn("SocketTimeoutException: {}. Renewing socket...", e.getMessage());
            return null;
        } catch (final SocketException e) {
            Logger.warn("SocketException: {}. Renewing socket...", e.getMessage());
            renewSocket(pooledSocket); // immediately
            return execute(data); // Retry with fresh socket
        } catch (final IOException e) {
            Logger.error("Socket IOException: {}. Renewing socket...", e.getMessage());
            returnSocket(pooledSocket);
            return execute(data); // retry
        } finally {
            returnSocket(pooledSocket);
        }
    }
}
