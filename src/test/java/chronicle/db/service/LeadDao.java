package chronicle.db.service;

import java.util.ArrayList;
import java.util.UUID;

import com.jsoniter.spi.TypeLiteral;

import chronicle.db.dao.SingleChronicleDao;

public class LeadDao implements SingleChronicleDao<String, Lead> {
    private final String dataPath;

    public LeadDao(final String dataPath) {
        this.dataPath = dataPath;
        createDataDirs();
    }

    private static final ThreadLocal<Lead> USING = new ThreadLocal<>();

    @Override
    public long entries() {
        return 100000;
    }

    @Override
    public String averageKey() {
        return UUID.randomUUID().toString();
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

}
