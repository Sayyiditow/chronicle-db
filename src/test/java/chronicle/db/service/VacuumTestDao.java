package chronicle.db.service;

import java.util.ArrayList;
import java.util.Set;

import com.jsoniter.spi.TypeLiteral;

import chronicle.db.dao.ChronicleDao;

/**
 * Test DAO with a small {@code entries()} so rotation triggers quickly. With
 * 100 entries per file, inserting 500 records produces exactly 5 rotated
 * files — ideal for exercising vacuum with realistic placement scenarios
 * without the throughput cost of using a production-sized DAO.
 */
public class VacuumTestDao implements ChronicleDao<Lead> {
    private final String dataPath;
    private static final ThreadLocal<Lead> USING = ThreadLocal.withInitial(Lead::new);

    public VacuumTestDao(final String dataPath) {
        this.dataPath = dataPath;
        createDataDirs();
        initDefaultIndexes();
    }

    @Override
    public long entries() {
        return 100;
    }

    @Override
    public Lead using() {
        return USING.get();
    }

    @Override
    public Lead averageValue() {
        return new Lead("Hashim Sayyid", "hashim-sayyid-12912", "", "", "abas@asa.com",
                "012109012", "test engineer", "asa", new ArrayList<>());
    }

    @Override
    public String dataPath() {
        return dataPath + name();
    }

    @Override
    public TypeLiteral<Lead> jsonType() {
        return new TypeLiteral<>() {
        };
    }

    @Override
    public Set<String> indexFileNames() {
        return Set.of("fullName");
    }
}
