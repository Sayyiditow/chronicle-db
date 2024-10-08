package chronicle.db.service;

import static chronicle.db.service.ChronicleDb.CHRONICLE_DB;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import net.openhft.chronicle.map.ChronicleMapBuilder;

@SuppressWarnings("unchecked")
public class LeadDaoTest {
    private static final String OBJECT = "chronicle.db.service.Lead";
    private static final String DAO = "chronicle.db.service.LeadDao";
    private static final String DATA_PATH = "src/test/.data/";

    @Test
    public void testInsert() throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException,
            NoSuchFieldException, SecurityException, IOException, InstantiationException, InvocationTargetException,
            InterruptedException {
        final var dao = CHRONICLE_DB.getChronicleDao(DAO, DATA_PATH);
        final Map<String, Object> objects = new HashMap<>();
        int i = 100;
        dao.put(UUID.randomUUID().toString(),
                new Lead("Hashim Sayyid", "hashim-sayyind-12912", "", "", "abas@asa.com", "012109012",
                        "test engineer", "asa", new ArrayList<>()));

        while (i > 0) {
            final var obj = new Lead("Hashim Sayyid", "hashim-sayyind-12912", "", "", "abas@asa.com", "012109012",
                    "test engineer",
                    "asa", new ArrayList<>());
            objects.put(UUID.randomUUID().toString(), obj);
            i--;
        }
        dao.putAll(objects);
        System.out.println(dao.size());
    }

    @Test
    public void indexTest() throws IOException {
        final var values = new ArrayList<Object>();

        final var indexDb = ChronicleMapBuilder.of(Object.class, List.class)
                .name("indexDb")
                .entries(1000)
                .averageKeySize(10000)
                .averageValue(values)
                .create();

        int i = 0;
        while (i < 250000) {
            values.add(UUID.randomUUID().toString());
            i++;
        }
        // Storing the nested structure
        indexDb.put("exampleKey", values);

        // Retrieving the data
        final var retrievedMap = indexDb.get("exampleKey");
        System.out.println(retrievedMap.size());
    }
}