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
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import org.tinylog.Logger;

import chronicle.db.Server;
import chronicle.db.config.KryoSerializer;
import chronicle.db.config.QueryMode;
import chronicle.db.utils.SafeRunnable;
import chronicle.db.utils.SafeSupplier;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.wire.DocumentContext;

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
    private final ConcurrentMap<String, ConcurrentSkipListSet<Long>> completionSets = new ConcurrentHashMap<>();
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
    private long append(final byte[] data) {
        return CHRONICLE_UTILS.doWithLock(queueLock, () -> {
            final var len = CHRONICLE_UTILS.humanReadableByteCount(data.length);
            final var maxMessageSize = blockSize / 5; // 20% of block size

            if (data.length > maxMessageSize) {
                Logger.error("Message size {} exceeds maximum allowed size {}. Write will be dropped.",
                        len, CHRONICLE_UTILS.humanReadableByteCount(maxMessageSize));
                return -1L;
            }

            try (final var appender = queue.createAppender()) {
                appender.writeDocument(w -> w.write("data").bytes(data));
                final long index = appender.lastIndexAppended();
                appender.sync();
                Logger.info("Queued replication data, size={}", len);
                return index;
            } catch (final Throwable e) {
                Logger.error(e, "Failed to append to replication queue.");
                return -1L;
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
            final var earliestCycle = new AtomicLong(Long.MAX_VALUE);
            for (final var tailerName : tailerNames) {
                CHRONICLE_UTILS.doWithLock(tailerLocks, tailerName, () -> {
                    try (final var tailer = queue.createTailer(tailerName).direction(TailerDirection.FORWARD)) {
                        earliestCycle.set(Math.min(earliestCycle.get(), tailer.cycle()));
                    }
                });
            }

            final RollCycle rollCycle = queue.rollCycle();
            final var rollCycleLength = rollCycle.lengthInMillis();
            final long tailerCycle = earliestCycle.get();
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

    private boolean markProcessed(final String tailerName, final ExcerptTailer tailer) {
        // Only advance by one message (the one that was successfully processed)
        boolean advanced = false;
        try (DocumentContext dc = tailer.readingDocument()) {
            // Just entering and exiting this block advances the tailer
            // if a document was present.
            advanced = dc.isPresent();
        }

        if (advanced) {
            final long index = tailer.index();
            final int cycle = queue.rollCycle().toCycle(index);
            final long sequence = queue.rollCycle().toSequenceNumber(index);
            Logger.info("Tailer [{}] advanced to cycle={}, seq={}", tailerName, cycle, sequence);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Marks a specific message index as successfully processed for a given tailer.
     * This method uses a gap-filling strategy: the tailer only advances past
     * contiguous completed indices to ensure no data is skipped on crash recovery.
     *
     * @param index The specific index that was completed.
     */
    private void markPrimaryProcessed(final long index) {
        if (index == -1L) {
            return;
        }

        // 1. Mark this specific index as complete
        final var completed = completionSets.computeIfAbsent(primaryTailerName, k -> new ConcurrentSkipListSet<>());
        completed.add(index);

        // 2. Try to advance the tailer as far as possible (filling gaps)
        CHRONICLE_UTILS.doWithLock(tailerLocks, primaryTailerName, () -> {
            try (final var tailer = queue.createTailer(primaryTailerName).direction(TailerDirection.FORWARD)) {
                while (!completed.isEmpty()) {
                    boolean advanced = false;
                    // Peek at the NEXT available record in the queue
                    try (DocumentContext dc = tailer.readingDocument()) {
                        if (dc.isPresent()) {
                            final long foundIndex = dc.index();
                            if (completed.contains(foundIndex)) {
                                // This write is finished!
                                completed.remove(foundIndex);
                                advanced = true;
                            } else {
                                dc.rollbackOnClose();
                                break;
                            }
                        } else {
                            break; // End of queue
                        }
                    }

                    if (advanced) {
                        final long newIndex = tailer.index();
                        final int cycle = queue.rollCycle().toCycle(newIndex);
                        final long sequence = queue.rollCycle().toSequenceNumber(newIndex);
                        Logger.info("Tailer [{}] advanced to cycle={}, seq={}",
                                primaryTailerName, cycle, sequence);
                    } else {
                        break;
                    }
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
                final var index = tailer.index();
                final var queueIndex = queue.lastIndex();
                Logger.info("Tailer [{}] index={}, queueIndex={}", tailerName, index, queueIndex);
                return index > queueIndex;
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
                            final var advanced = markProcessed(tailerName, tailer);
                            if (advanced) {
                                processedCount.incrementAndGet();
                            } else {
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
            final long index = queue.append(KryoSerializer.serialize(params));
            if (index != -1L) {
                action.run();
                Logger.info("DB write finished for index={}. Marking primary done.", index);
                queue.markPrimaryProcessed(index);
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
            final long index = queue.append(KryoSerializer.serialize(params));
            if (index != -1L) {
                final T result = action.get();
                Logger.info("DB write finished for index={}. Marking primary done.", index);
                queue.markPrimaryProcessed(index);
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
            final long index = queue.append(KryoSerializer.serialize(params));
            if (index != -1L) {
                final T result = action.get();
                queue.markPrimaryProcessed(index);
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
            final long index = queue.append(KryoSerializer.serialize(params));
            if (index != -1L) {
                final T result = action.get();
                queue.markPrimaryProcessed(index);
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
            final long index = queue.append(KryoSerializer.serialize(params));
            if (index != -1L) {
                final T result = action.get();
                queue.markPrimaryProcessed(index);
                return result;
            }
            return action.failureValue();
        } else {
            return action.get();
        }
    }
}
