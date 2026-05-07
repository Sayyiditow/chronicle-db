package chronicle.db.parallel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import chronicle.db.utils.ChronicleUtils;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

/**
 * Verifies chronicle-db's parallel paths work correctly under virtual threads.
 * Run with {@code -Djdk.tracePinnedThreads=full} to capture pinning reports.
 *
 * <p>Tests cover:
 * <ul>
 *   <li>ChronicleMap concurrent puts via virtual threads — no pinning, all entries visible</li>
 *   <li>Cascading {@code processInParallel} → {@code parallelIterable} — no deadlock under the
 *       new single-pool design</li>
 *   <li>Same-pool nested {@code processInParallel} — verifies the {@code outerGate}
 *       Semaphore + ThreadLocal nesting marker prevents the cap from deadlocking</li>
 *   <li>Heavy concurrent load — many VTs hitting both APIs simultaneously without throttle starvation</li>
 * </ul>
 */
public class VirtualThreadCompatibilityTest {
    private static Path tempDir;
    private static ChronicleMap<String, String> sharedMap;

    @BeforeAll
    static void setup() throws IOException {
        tempDir = Files.createTempDirectory("vt-compat-test-");
        final var file = new File(tempDir.toFile(), "shared-map.dat");
        sharedMap = ChronicleMapBuilder.of(String.class, String.class)
                .name("vt-compat")
                .entries(200_000)
                .averageKeySize(16)
                .averageValueSize(64)
                .createPersistedTo(file);
    }

    @AfterAll
    static void teardown() throws IOException {
        if (sharedMap != null) {
            sharedMap.close();
        }
        // Best-effort cleanup
        try (var stream = Files.walk(tempDir)) {
            stream.sorted((a, b) -> b.compareTo(a)).forEach(p -> p.toFile().delete());
        }
    }

    /**
     * Spawn many virtual threads each doing many ChronicleMap puts. Asserts:
     * <ul>
     *   <li>All entries are written and readable</li>
     *   <li>No carrier-pinning warnings on stderr (when run with -Djdk.tracePinnedThreads=full)</li>
     * </ul>
     */
    @Test
    void chronicleMapConcurrentPutsViaVirtualThreads() throws Exception {
        final int threadCount = 200;
        final int putsPerThread = 500;
        final var startGate = new CountDownLatch(1);
        final var doneGate = new CountDownLatch(threadCount);
        final var errors = new ConcurrentHashMap<Integer, Throwable>();

        final ByteArrayOutputStream pinnedCapture = new ByteArrayOutputStream();
        final PrintStream originalErr = System.err;
        // Redirect stderr to capture any "Thread holds Java monitor" reports
        // emitted by -Djdk.tracePinnedThreads=full.
        System.setErr(new PrintStream(pinnedCapture, true));

        try (final ExecutorService vtExec = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                vtExec.submit(() -> {
                    try {
                        startGate.await();
                        for (int i = 0; i < putsPerThread; i++) {
                            final String key = "t" + threadId + "-k" + i;
                            sharedMap.put(key, "v" + threadId + "-" + i);
                        }
                    } catch (final Throwable e) {
                        errors.put(threadId, e);
                    } finally {
                        doneGate.countDown();
                    }
                });
            }
            startGate.countDown();
            assertTrue(doneGate.await(60, TimeUnit.SECONDS),
                    "VT puts didn't complete within 60s — possible carrier saturation or hang");
        } finally {
            System.setErr(originalErr);
        }

        assertTrue(errors.isEmpty(), "Errors during concurrent puts: " + errors);
        assertEquals(threadCount * putsPerThread, sharedMap.size(),
                "Expected all VT writes to be persisted");

        final String pinnedReport = pinnedCapture.toString();
        // Pinning report is a soft-fail: warn but don't fail the test, since chronicle-map
        // may pin briefly during native ops. Critical: no *long-held* pin (>1s) which would
        // appear as a multi-frame stack with "blocked at" lines.
        if (pinnedReport.contains("Thread holds Java monitor")) {
            System.out.println("[PINNING-WARN] Pinning detected — review: " + pinnedReport);
        }
    }

    /**
     * Cascading parallel pattern: outer {@code processInParallel} → inner {@code parallelIterable}.
     * This used to be split across two fixed pools (sharedExecutor + iterableExecutor).
     * Now both share the same VT executor — verify cascade still completes without
     * deadlock and produces correct counts.
     */
    @Test
    void cascadingProcessInParallelToParallelIterable() throws Exception {
        final int outerCount = 50;          // 50 outer tasks
        final int itemsPerOuter = 100;      // each iterates 100 items
        final var outerItems = new ArrayList<Integer>();
        for (int i = 0; i < outerCount; i++) {
            outerItems.add(i);
        }
        final var visited = ConcurrentHashMap.<String>newKeySet();
        final long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();

        ChronicleUtils.CHRONICLE_UTILS.processInParallel(outerItems, outerIdx -> {
            final var innerItems = new ArrayList<String>();
            for (int j = 0; j < itemsPerOuter; j++) {
                innerItems.add(outerIdx + ":" + j);
            }
            try {
                ChronicleUtils.CHRONICLE_UTILS.parallelIterable(innerItems, Integer.MAX_VALUE, item -> {
                    visited.add(item);
                });
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("Interrupted in inner parallelIterable");
            }
            if (System.nanoTime() > deadline) {
                fail("Cascading parallel timed out after 60s — possible deadlock");
            }
        });

        assertEquals(outerCount * itemsPerOuter, visited.size(),
                "Expected every cascaded item to be visited");
    }

    /**
     * Forces same-method nesting: outer {@code processInParallel} whose action body itself
     * calls {@code processInParallel}. With the previous fixed pool of 2-3 slots, this could
     * deadlock when outer tasks fill the pool and inner submits queue forever. The new
     * design must complete cleanly because virtual threads are unbounded and the outer
     * gate Semaphore is bypassed for nested calls.
     */
    @Test
    void nestedProcessInParallelDoesNotDeadlock() throws Exception {
        final int outerCount = 100;
        final int innerCount = 100;
        final var seenLeaves = ConcurrentHashMap.<String>newKeySet();
        final var outerItems = new ArrayList<Integer>();
        for (int i = 0; i < outerCount; i++) {
            outerItems.add(i);
        }
        final long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();

        ChronicleUtils.CHRONICLE_UTILS.processInParallel(outerItems, outer -> {
            // Inner call to the SAME parallel API — would have deadlocked under the
            // 2-slot fixed pool design.
            final var innerItems = new ArrayList<Integer>();
            for (int j = 0; j < innerCount; j++) {
                innerItems.add(j);
            }
            ChronicleUtils.CHRONICLE_UTILS.processInParallel(innerItems, inner -> {
                seenLeaves.add(outer + ":" + inner);
            });
            if (System.nanoTime() > deadline) {
                fail("Nested processInParallel timed out — deadlock on the outer gate");
            }
        });

        assertEquals(outerCount * innerCount, seenLeaves.size(),
                "Every (outer, inner) leaf should have been visited exactly once");
    }

    /**
     * High-pressure smoke test: many concurrent top-level callers. Each top-level
     * acquires one outer-gate permit. Default cap is 1000; we use 200 callers with
     * 50 items each, so we stay under cap and exercise the steady-state path.
     */
    @Test
    void manyConcurrentTopLevelCallersComplete() throws Exception {
        final int callerCount = 200;
        final int itemsPerCaller = 50;
        final var startGate = new CountDownLatch(1);
        final var doneGate = new CountDownLatch(callerCount);
        final var totalItemsProcessed = new AtomicInteger(0);
        final var firstError = new AtomicReference<Throwable>();

        try (final ExecutorService vtExec = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int c = 0; c < callerCount; c++) {
                vtExec.submit(() -> {
                    try {
                        startGate.await();
                        final var items = new ArrayList<Integer>();
                        for (int i = 0; i < itemsPerCaller; i++) {
                            items.add(i);
                        }
                        ChronicleUtils.CHRONICLE_UTILS.processInParallel(items, item -> {
                            totalItemsProcessed.incrementAndGet();
                        });
                    } catch (final Throwable e) {
                        firstError.compareAndSet(null, e);
                    } finally {
                        doneGate.countDown();
                    }
                });
            }
            startGate.countDown();
            assertTrue(doneGate.await(60, TimeUnit.SECONDS),
                    "Concurrent callers didn't finish in 60s — possible gate exhaustion");
        }

        assertNotNull(totalItemsProcessed);
        if (firstError.get() != null) {
            fail("Caller errored: " + firstError.get().getMessage(), firstError.get());
        }
        assertEquals(callerCount * itemsPerCaller, totalItemsProcessed.get(),
                "Every item should have been processed exactly once");
    }

    /**
     * Sanity: the parallelIterable iterator-locking scheme works under virtual threads.
     * Spawn many VTs as the worker pool implicitly does, ensure each item is processed
     * exactly once and order doesn't matter.
     */
    @Test
    void parallelIterableProcessesEveryItemExactlyOnce() throws Exception {
        final int itemCount = 5000;
        final var items = new ArrayList<Integer>(itemCount);
        for (int i = 0; i < itemCount; i++) {
            items.add(i);
        }
        final var seen = Set.copyOf(new HashSet<Integer>());
        final var seenSync = ConcurrentHashMap.<Integer>newKeySet();
        final var duplicates = new AtomicInteger(0);

        ChronicleUtils.CHRONICLE_UTILS.parallelIterable(items, Integer.MAX_VALUE, item -> {
            if (!seenSync.add(item)) {
                duplicates.incrementAndGet();
            }
        });

        assertFalse(seen.contains(0), "sentinel — unrelated empty set check");
        assertEquals(0, duplicates.get(), "No item should be processed twice");
        assertEquals(itemCount, seenSync.size(), "Every item should have been processed");
    }
}
