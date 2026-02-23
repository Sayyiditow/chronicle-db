package chronicle.db.service;

import java.util.ArrayList;
import java.util.Set;

import com.jsoniter.spi.TypeLiteral;

import chronicle.db.dao.ChronicleDao;

public class LeadDao implements ChronicleDao<Lead> {
    private final String dataPath;

    public LeadDao(final String dataPath) {
        this.dataPath = dataPath;
        createDataDirs();
        initDefaultIndexes();
    }

    private static final ThreadLocal<Lead> USING = ThreadLocal.withInitial(Lead::new);

    @Override
    public long entries() {
        return 50_000;
    }

    @Override
    public Lead using() {
        return USING.get();
    }

    @Override
    public Lead averageValue() {
        return new Lead("Hashim Sayyid", "hashim-sayyind-12912", "", "", "abas@asa.com", "012109012", "test engineer",
                "asa", new ArrayList<>());
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
