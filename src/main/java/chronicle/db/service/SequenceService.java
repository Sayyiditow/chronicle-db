package chronicle.db.service;

import static chronicle.db.service.SequenceDb.SEQUENCE_DB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import chronicle.db.Server;

public class SequenceService {
    private SequenceService() {
    }

    public static final SequenceService SEQUENCE_SERVICE = new SequenceService();
    private static final String sequenceFilePath = Server.getDbPath() + "/" + Server.getAppdir() + "/sequences";
    private static final long entries = 100L;

    public Map<String, Long> getSequenceDb() throws IOException {
        final Map<String, Long> map = new HashMap<>();
        try (final var db = SEQUENCE_DB.getDb(sequenceFilePath, entries)) {
            map.putAll(db);
        }

        return map;
    }

    public void updateSequenceDb(final Map<String, Long> map) throws IOException {
        try (final var db = SEQUENCE_DB.getDb(sequenceFilePath, entries)) {
            db.putAll(map);
        }
    }

    public long getSequence(final String key) throws IOException {
        try (final var db = SEQUENCE_DB.getDb(sequenceFilePath, entries)) {
            return SEQUENCE_DB.getNextSequence(db, key);
        }
    }

    public String getSequence(final String key, final int seqLen) throws IOException {
        try (final var db = SEQUENCE_DB.getDb(sequenceFilePath, entries)) {
            return SEQUENCE_DB.formatSequence(seqLen, SEQUENCE_DB.getNextSequence(db, key));
        }
    }

    public List<String> getSequences(final String key, final int increments, final int seqLen)
            throws IOException {
        try (final var db = SEQUENCE_DB.getDb(sequenceFilePath, entries)) {
            final var sequences = new ArrayList<String>(increments);
            for (int i = 0; i < increments; i++) {
                sequences.add(SEQUENCE_DB.formatSequence(seqLen, SEQUENCE_DB.getNextSequence(db, key)));
            }
            return sequences;
        }
    }

    public List<Long> getSequences(final String key, final int increments) throws IOException {
        try (final var db = SEQUENCE_DB.getDb(sequenceFilePath, entries)) {
            final var sequences = new ArrayList<Long>(increments);
            for (int i = 0; i < increments; i++) {
                sequences.add(SEQUENCE_DB.getNextSequence(db, key));
            }
            return sequences;
        }
    }

    public void resetSequence(final String key) throws IOException {
        try (final var db = SEQUENCE_DB.getDb(sequenceFilePath, entries)) {
            db.put(key, 0L);
        }
    }
}
