package chronicle.db.service;

import static chronicle.db.service.ChronicleDb.CHRONICLE_DB;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

/**
 * Reproduces the open()/close() data race in the reference-counted cache:
 * when close() decrements refCount to 0 outside the cache lock, a concurrent
 * open() can retain() the same entry before close() shuts the underlying map,
 * handing a borrower a map that is then closed under it
 * (ChronicleHashClosedException).
 */
public class ChronicleDbConcurrencyTest {

    private static Lead sample() {
        return new Lead("avg", "linkedin", "fb", "twitter", "avg@test.com", "012", "Engineer", "Loc",
                new ArrayList<>());
    }

    @Test
    void concurrentOpenCloseNeverAccessesAfterClose() throws Exception {
        final Path dir = Path.of("src/test/.data/concurrency");
        Files.createDirectories(dir);
        final String filePath = dir.resolve("data").toString();
        final Lead sample = sample();

        // Seed one entry so get() actually touches the map, then release.
        final var seed = CHRONICLE_DB.open("conc", 1_000, 64, sample, filePath, 1.0);
        seed.put("k", sample);
        seed.close();

        final int threads = 24;
        final int iters = 2_500;
        final var errors = new ConcurrentLinkedQueue<Throwable>();

        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    for (int i = 0; i < iters; i++) {
                        final var m = CHRONICLE_DB.open("conc", 1_000, 64, sample, filePath, 1.0);
                        try {
                            m.get("k");
                        } catch (final Throwable e) {
                            errors.add(e);
                        } finally {
                            m.close();
                        }
                    }
                });
            }
        } // close() awaits termination of all virtual threads

        assertTrue(errors.isEmpty(),
                errors.size() + " access-after-close errors; first = "
                        + (errors.peek() == null ? "none" : errors.peek().toString()));
    }
}
