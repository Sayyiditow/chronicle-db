package chronicle.db.service;

import static chronicle.db.utils.ChronicleUtils.CHRONICLE_UTILS;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import org.tinylog.Logger;

import chronicle.db.Server;
import chronicle.db.config.KryoSerializer;
import chronicle.db.config.QueryMode;
import chronicle.db.utils.SafeRunnable;
import chronicle.db.utils.SafeSupplier;
import net.openhft.chronicle.bytes.util.DecoratedBufferOverflowException;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.TailerDirection;

/**
 * Manages a persistent replication queue using Chronicle Queue.
 * This class implements a "Replicate First" (WAL) pattern where data change
 * intent
 * is persisted to a local queue before being applied to the database.
 * The queue ensures that all standby servers can eventually synchronize their
 * state
 * with the primary server.
 */
public class ReplicationQueue {
    private static final String queueName = "replication";
    private static final String queueDir = "/queues/";
    private static final String primaryTailerName = "localhost_9099";
    private final ChronicleQueue queue;
    private final ReentrantLock queueLock = new ReentrantLock();
    private static final ConcurrentMap<String, ReentrantLock> tailerLocks = new ConcurrentHashMap<>();
    private final String[] tailerNames;
    private static final long blockSize = Server.getQueueSize() * 1024L * 1024L;

    /**
     * Initializes the Chronicle Queue with configured roll cycle and block size.
     *
     * @return An initialized ChronicleQueue instance.
     */
    private ChronicleQueue initQueue() {
        return CHRONICLE_UTILS.doWithLock(queueLock, () -> ChronicleQueue
                .singleBuilder(Path.of(Server.getDbPath(), Server.getAppdir(), queueDir, queueName).toString())
                .rollCycle(RollCycles.FAST_HOURLY)
                .blockSize(blockSize)
                .build());
    }

    /**
     * Constructs a new ReplicationQueue.
     *
     * @param tailerNames Array of names for the tailers (e.g., consumer
     *                    identifiers) that will track their progress in the queue.
     */
    public ReplicationQueue(final String[] tailerNames) {
        this.tailerNames = tailerNames;
        this.queue = initQueue();
        Logger.info("Initialized replication queue for {}", Arrays.toString(tailerNames));
    }

    /**
     * Appends data to the replication queue.
     * Implements a retry-with-wait mechanism for buffer overflow scenarios.
     *
     * Chronicle Queue guarantees a message can be written only if it's ≤ 25% of
     * block size.
     * Messages larger than this will be dropped immediately to avoid futile retry
     * attempts.
     *
     * @param data The serialized byte array to persist.
     * @return true if the append was successful, false otherwise.
     */
    private boolean append(final byte[] data) {
        return CHRONICLE_UTILS.doWithLock(queueLock, () -> {
            final var len = CHRONICLE_UTILS.humanReadableByteCount(data.length);

            // Chronicle Queue's 20% rule: message must be ≤ 20% of block size
            final var maxMessageSize = blockSize / 5; // 20% of block size
            final var humanReadableSize = CHRONICLE_UTILS.humanReadableByteCount(maxMessageSize);
            if (data.length > maxMessageSize) {
                Logger.error("Message size {} exceeds maximum allowed size {}. Write will be dropped.",
                        len, humanReadableSize);
                return false;
            }

            try (final var appender = queue.createAppender()) {
                appender.writeDocument(w -> w.write("data").bytes(data));
                appender.sync();
                Logger.info("Queued replication data, size={}", len);
                return true;
            } catch (final DecoratedBufferOverflowException e) {
                Logger.error(
                        "Buffer overflow detected. Size: {} exceeds maximum allowed size {}. Write will be dropped.",
                        len, humanReadableSize);

                return false;
            }
        });
    }

    /**
     * Deletes old queue files as long as they have been processed by all tailers.
     * Keeps at least one buffer day of history.
     *
     * @throws IOException If file deletion fails.
     */
    public void cleanupQueue() throws IOException {
        CHRONICLE_UTILS.doWithLock(queueLock, new SafeRunnable(() -> {
            if (queue == null)
                return;

            final File queueDir = queue.file();
            final File[] queueFiles = queueDir.listFiles((dir, name) -> name.endsWith(".cq4"));
            if (queueFiles == null || queueFiles.length == 0) {
                Logger.info("No queue files found.");
                return;
            }

            // Find earliest cycle from all tailers
            long earliestCycle = Long.MAX_VALUE;
            for (final var tailerName : tailerNames) {
                try (final var tailer = queue.createTailer(tailerName).direction(TailerDirection.FORWARD)) {
                    earliestCycle = Math.min(earliestCycle, tailer.cycle());
                }
            }

            final RollCycle rollCycle = queue.rollCycle();
            final var rollCycleLength = rollCycle.lengthInMillis();
            final long tailerCycle = earliestCycle;
            final Instant earliestInstant = Instant.ofEpochMilli(tailerCycle * rollCycleLength);
            Logger.info("Queue cleanup. Earliest tailer cycle = {} ({} UTC)", tailerCycle, earliestInstant);
            // Keep previous day's files
            final var bufferToKeep = Long.parseLong(LocalDate.now(ZoneOffset.UTC).minusDays(1)
                    .format(DateTimeFormatter.ofPattern("yyyyMMdd")));
            final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd-HH").withZone(ZoneOffset.UTC);
            // Remove files older than earliest cycle
            for (final File file : queueFiles) {
                final String fileName = file.getName();
                final var fileDate = Long.parseLong(fileName.substring(0, 8));
                final String cycleStr = fileName.replace("F.cq4", "");
                final long millis = LocalDateTime.parse(cycleStr, formatter).toInstant(ZoneOffset.UTC).toEpochMilli();
                final long fileCycle = millis / rollCycleLength;

                if (fileCycle < tailerCycle && fileDate < bufferToKeep) {
                    Logger.info("Deleting queue file [{}].", fileName);
                    Files.deleteIfExists(file.toPath());
                } else {
                    Logger.info("File [{}] left intact, within tailer cycle.", fileName);
                }
            }
        }, "Queue Cleanup"));
    }

    /**
     * Marks the current message as processed for the specified tailer.
     * This advances the tailer's index in the queue.
     *
     * @param tailerName The unique name identifying the tailer/consumer.
     */
    private void markProcessed(final String tailerName) {
        CHRONICLE_UTILS.doWithLock(tailerLocks, tailerName, () -> {
            try (final var tailer = queue.createTailer(tailerName).direction(TailerDirection.FORWARD)) {
                // Only advance by one message (the one that was successfully processed)
                final boolean advanced = tailer.readDocument(w -> {
                    // Read the bytes but don't do anything with them
                    w.read("data").bytes();
                });

                if (advanced) {
                    final long index = tailer.index();
                    final int cycle = queue.rollCycle().toCycle(index);
                    final long sequence = queue.rollCycle().toSequenceNumber(index);
                    Logger.info("Tailer [{}] advanced to cycle={}, seq={}", tailerName, cycle, sequence);
                } else {
                    Logger.warn("No message to advance for tailer [{}]", tailerName);
                }
            }
        });
    }

    /**
     * Checks if there are any pending messages for the specified tailer.
     *
     * @param tailerName The name of the tailer to check.
     * @return true if there are no more messages to read for this tailer, false
     *         otherwise.
     */
    public boolean isEmpty(final String tailerName) {
        return CHRONICLE_UTILS.doWithLock(tailerLocks, tailerName, () -> {
            try (final var tailer = queue.createTailer(tailerName).direction(TailerDirection.FORWARD)) {
                try (final var tempTailer = queue.createTailer()) {
                    tempTailer.moveToIndex(tailer.index());
                    return !tempTailer.readDocument(w -> {
                    });
                }
            }
        });
    }

    /**
     * Closes the underlying Chronicle Queue.
     */
    public void close() {
        queue.close();
        Logger.info("Closed Replication Queue.");
    }

    /**
     * Helper to generate a standardized tailer name based on the target database
     * address.
     *
     * @param dbUrl  The IP address or hostname of the standby database.
     * @param dbPort The port of the standby database.
     * @return A formatted string used as a tailer name.
     */
    public static String generateTailerName(final String dbUrl, final String dbPort) {
        return dbUrl + "_" + dbPort;
    }

    /**
     * Returns the name of the primary tailer used for local database processing
     * tracking.
     *
     * @return The primary tailer name constant.
     */
    public static String getPrimaryTailerName() {
        return primaryTailerName;
    }

    /**
     * Processes all pending messages in the queue for the given tailer.
     * This method advances the named tailer's persistent index ONLY after
     * successful processing.
     *
     * @param tailerName The name of the tailer whose pending messages should be
     *                   processed.
     * @param processor  A functional interface returning true if processing
     *                   succeeded, false to stop and retry later.
     * @return The number of records successfully processed and marked.
     */
    public int processPending(final String tailerName, final Predicate<byte[]> processor) {
        final var processedCount = new AtomicInteger(0);
        CHRONICLE_UTILS.doWithLock(tailerLocks, tailerName, () -> {
            try (final var tailer = queue.createTailer(tailerName).direction(TailerDirection.FORWARD)) {
                try (var tempTailer = queue.createTailer()) {
                    tempTailer.moveToIndex(tailer.index());
                    while (true) {
                        final var success = new AtomicBoolean(false);
                        boolean found;
                        try {
                            found = tempTailer.readDocument(w -> {
                                final byte[] data = w.read("data").bytes();
                                if (processor.test(data)) {
                                    success.set(true);
                                }
                            });
                        } catch (final Exception e) {
                            // Stop if processor throws exception
                            break;
                        }

                        if (!found) {
                            break;
                        }

                        if (success.get()) {
                            // Advance the named tailer only on success
                            final boolean advanced = tailer.readDocument(w -> w.read("data").bytes());
                            if (advanced) {
                                processedCount.incrementAndGet();
                                final long index = tailer.index();
                                final int cycle = queue.rollCycle().toCycle(index);
                                final long sequence = queue.rollCycle().toSequenceNumber(index);
                                Logger.info("Tailer [{}] advanced to cycle={}, seq={}", tailerName, cycle, sequence);
                            } else {
                                Logger.warn("No message to advance for tailer [{}]", tailerName);
                                break;
                            }
                        } else {
                            // Stop processing if one fails, it will be retried next time
                            break;
                        }
                    }
                }
            }
        });

        return processedCount.get();
    }

    /**
     * Populates the query parameter map with basic replication fields.
     */
    private static void prepareReplicationParams(final Map<String, Object> queryParams, final QueryMode mode,
            final Object key, final Object value) {
        queryParams.put("mode", mode);
        queryParams.put("key", key);
        queryParams.put("value", value);
    }

    /**
     * Populates the query parameter map with batch replication fields.
     */
    private static void prepareReplicationParams(final Map<String, Object> queryParams, final QueryMode mode,
            final Map<String, Object> objects) {
        queryParams.put("mode", mode);
        queryParams.put("objects", objects);
    }

    /**
     * Populates the query parameter map with the replication mode.
     */
    private static void prepareReplicationParams(final Map<String, Object> queryParams, final QueryMode mode) {
        queryParams.put("mode", mode);
    }

    /**
     * Executes a state-changing operation following the Write-Ahead Logging (WAL)
     * pattern.
     * If replication is enabled, the intent is appended to the queue BEFORE the
     * action is executed.
     *
     * @param queue  The active replication queue instance (can be null if
     *               replication is disabled).
     * @param params The data to be replicated.
     * @param action The local database operation to perform.
     */
    public static void run(final ReplicationQueue queue, final Map<String, Object> params,
            final SafeRunnable action) {
        if (queue != null) {
            if (queue.append(KryoSerializer.serialize(params))) {
                action.run();
                queue.markProcessed(primaryTailerName);
            }
        } else {
            action.run();
        }
    }

    /**
     * Executes a state-changing operation that returns a result, following the WAL
     * pattern.
     *
     * @param <T>    The return type of the action.
     * @param queue  The active replication queue instance.
     * @param params The data to be replicated.
     * @param action The local database operation that returns a result.
     * @return The result of the action, or the action's failure value if queue
     *         append fails.
     */
    public static <T> T call(final ReplicationQueue queue, final Map<String, Object> params,
            final SafeSupplier<T> action) {
        if (queue != null) {
            if (queue.append(KryoSerializer.serialize(params))) {
                final T result = action.get();
                queue.markProcessed(primaryTailerName);
                return result;
            }
            return action.failureValue();
        } else {
            return action.get();
        }
    }

    /**
     * Prepares parameters and calls an action following the WAL pattern.
     *
     * @param <T>    Return type.
     * @param queue  Replication queue.
     * @param params Map to populate with query parameters.
     * @param mode   The QueryMode for replication.
     * @param key    The object key.
     * @param value  The object value.
     * @param action The database action.
     * @return Result of the action.
     */
    public static <T> T prepareAndCall(final ReplicationQueue queue, final Map<String, Object> params,
            final QueryMode mode, final Object key, final Object value, final SafeSupplier<T> action) {
        if (queue != null) {
            prepareReplicationParams(params, mode, key, value);
            if (queue.append(KryoSerializer.serialize(params))) {
                final T result = action.get();
                queue.markProcessed(primaryTailerName);
                return result;
            }
            return action.failureValue();
        } else {
            return action.get();
        }
    }

    /**
     * Prepares batch parameters and calls an action following the WAL pattern.
     *
     * @param <T>     Return type.
     * @param queue   Replication queue.
     * @param params  Map to populate with query parameters.
     * @param mode    The QueryMode for replication.
     * @param objects Map of keys to objects for batch processing.
     * @param action  The database action.
     * @return Result of the action.
     */
    public static <T> T prepareAndCall(final ReplicationQueue queue, final Map<String, Object> params,
            final QueryMode mode, final Map<String, Object> objects, final SafeSupplier<T> action) {
        if (queue != null) {
            prepareReplicationParams(params, mode, objects);
            if (queue.append(KryoSerializer.serialize(params))) {
                final T result = action.get();
                queue.markProcessed(primaryTailerName);
                return result;
            }
            return action.failureValue();
        } else {
            return action.get();
        }
    }

    /**
     * Prepares simple parameters (mode only) and calls an action following the WAL
     * pattern.
     *
     * @param <T>    Return type.
     * @param queue  Replication queue.
     * @param params Map to populate with query parameters.
     * @param mode   The QueryMode for replication.
     * @param action The database action.
     * @return Result of the action.
     */
    public static <T> T prepareAndCall(final ReplicationQueue queue, final Map<String, Object> params,
            final QueryMode mode, final SafeSupplier<T> action) {
        if (queue != null) {
            prepareReplicationParams(params, mode);
            if (queue.append(KryoSerializer.serialize(params))) {
                final T result = action.get();
                queue.markProcessed(primaryTailerName);
                return result;
            }
            return action.failureValue();
        } else {
            return action.get();
        }
    }
}
