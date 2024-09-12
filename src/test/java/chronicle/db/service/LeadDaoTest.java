package chronicle.db.service;

import static chronicle.db.service.ChronicleDb.CHRONICLE_DB;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.junit.jupiter.api.Test;

import com.jsoniter.output.JsonStream;

import chronicle.db.entity.Search;
import chronicle.db.entity.Search.SearchType;
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
        final var dao = CHRONICLE_DB.getSingleChronicleDao(DAO, DATA_PATH);
        final ConcurrentMap<String, Object> objects = new ConcurrentHashMap<>();
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
        dao.put(objects, List.of());
        System.out.println(dao.size());
    }

    @Test
    public void testSearch() throws IOException, ClassNotFoundException, IllegalArgumentException,
            IllegalAccessException, NoSuchFieldException, SecurityException, IntrospectionException,
            InstantiationException, InvocationTargetException {
        final long time = System.currentTimeMillis();
        final var dao = CHRONICLE_DB.getMultiChronicleDao(DAO, DATA_PATH);
        System.out.println(dao.size());
        final var searchValues = dao.search(new Search("fullName", Search.SearchType.LIKE, "pheakdey ton"));
        System.out.println(JsonStream.serialize(searchValues));
        final long end = System.currentTimeMillis();
        System.out.println(end - time);
    }

    @Test
    public void index() throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException,
            NoSuchFieldException, SecurityException, IOException, InstantiationException, InvocationTargetException {
        final var dao = CHRONICLE_DB.getMultiChronicleDao(DAO, DATA_PATH);
        dao.initIndex(new String[] { "fullName" });
    }

    @Test
    public void indexedSearch() throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException,
            NoSuchFieldException, SecurityException, IOException, InstantiationException, InvocationTargetException {
        final long time = System.currentTimeMillis();
        final var dao = CHRONICLE_DB.getMultiChronicleDao(DAO, DATA_PATH);
        final var map = dao.indexedSearch(new Search("fullName", SearchType.EQUAL, "pheakdey ton"));
        final long end = System.currentTimeMillis();
        System.out.println(end - time);
        System.out.println(map);
    }

    @Test
    public void testMultiThreadDelete() throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException,
            SecurityException, ClassNotFoundException, IOException, InstantiationException, InvocationTargetException,
            InterruptedException {
        final var dao = CHRONICLE_DB.getMultiChronicleDao(DAO, DATA_PATH);
        System.out.println(dao.size());
        dao.delete(new HashSet<>(Arrays.asList("1-99000", "2-99001")));
        System.out.println(dao.size());
        System.out.println(dao.get("Test"));
    }

    @Test
    public void testGetAll() throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException,
            NoSuchFieldException, SecurityException, IOException, InstantiationException, InvocationTargetException {
        final var dao = CHRONICLE_DB.getMultiChronicleDao(DAO, DATA_PATH);
        System.out.println(dao.get(new HashSet<>(Arrays.asList("0-10", "0-11"))));
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