package chronicle.db.service;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

public final class SequenceDb {
    private SequenceDb() {
    };

    public static final SequenceDb SEQUENCE_DB = new SequenceDb();

    /**
     * Constructs a SequenceDb with a custom file path and number of entries.
     * 
     * @param filePath
     * @param entries  The expected number of unique keys (sequences).
     * @throws IOException If the ChronicleMap cannot be created or persisted.
     */
    public ChronicleMap<String, Long> getDb(final String filePath, final long entries) throws IOException {
        final var file = new File(filePath);

        if (file.exists()) {
            return ChronicleMapBuilder.of(String.class, Long.class).createPersistedTo(file);
        } else {
            return ChronicleMapBuilder.of(String.class, Long.class).name(file.getName()).entries(entries)
                    .averageKey(UUID.randomUUID().toString()).createPersistedTo(file);
        }
    }

    /**
     * Gets the next sequence number for the given key, starting at 1 if absent.
     * 
     * @param key The sequence identifier (e.g., "user", "order").
     * @return The next Long value in the sequence.
     */
    public long getNextSequence(final ChronicleMap<String, Long> db, final String key) {
        return db.compute(key, (k, currentValue) -> (currentValue != null ? currentValue : 0L) + 1L);
    }

    /**
     * Gets the current sequence number for the given key, or 0 if absent.
     * 
     * @param key The sequence identifier.
     * @return The current Long value, or 0 if not set.
     */
    public long getCurrentSequence(final ChronicleMap<String, Long> db, final String key) {
        return db.getOrDefault(key, 0L);
    }

    public String formatSequence(final int seqLen, final long value) {
        return String.format("%0" + seqLen + "d", value);
    }
}
