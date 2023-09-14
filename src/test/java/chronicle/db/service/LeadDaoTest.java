package chronicle.db.service;

import static chronicle.db.service.ChronicleDb.CHRONICLE_DB;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.junit.Test;

import com.jsoniter.JsonIterator;
import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.TypeLiteral;

import chronicle.db.entity.Search;
import chronicle.db.entity.Search.SearchType;

@SuppressWarnings("unchecked")
public class LeadDaoTest {
    private final String objectClassName = "chronicle.db.service.Lead";
    private final String daoClassName = "chronicle.db.service.LeadDao";
    private static final String DATA_PATH = "src/main/resources/.data/chronicle/";

    @Test
    public void testInsert() throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException,
            NoSuchFieldException, SecurityException, IOException, InstantiationException, InvocationTargetException {
        final var dao = CHRONICLE_DB.getMultiChronicleDao(daoClassName, DATA_PATH);
        final var c = CHRONICLE_DB.getObjectConstructor(objectClassName);
        final String sourcePath = "/home/hashim/Downloads/leads/dat.json.aa";
        final List<Map<String, Object>> source = JsonIterator.deserialize(Files.readAllBytes(Paths.get(sourcePath)),
                new TypeLiteral<>() {

                });
        final ConcurrentMap<String, Object> objects = new ConcurrentSkipListMap<>();

        for (final var map : source) {
            final var emails = (List<Map<String, String>>) map.get("emails");
            final var emailList = new ArrayList<Email>();
            emails.forEach((e) -> emailList.add(new Email(e.get("address"), e.get("type"))));
            final Object obj = c.newInstance(
                    map.get("full_name"),
                    map.get("linkedin_username"),
                    map.get("facebook_username"),
                    map.get("twitter_username"),
                    map.get("work_email"),
                    map.get("mobile_phone"),
                    map.get("job_title"),
                    map.get("location_name"),
                    emailList);
            objects.put(UUID.randomUUID().toString(), obj);
        }
        dao.put(objects, false);
        System.out.println(dao.size());
    }

    @Test
    public void testSearch() throws IOException, ClassNotFoundException, IllegalArgumentException,
            IllegalAccessException, NoSuchFieldException, SecurityException, IntrospectionException,
            InstantiationException, InvocationTargetException {
        final long time = System.currentTimeMillis();
        final var dao = CHRONICLE_DB.getMultiChronicleDao(daoClassName, DATA_PATH);
        System.out.println(dao.size());
        final var searchValues = dao.search(new Search("fullName", Search.SearchType.LIKE, "pheakdey ton"));
        System.out.println(JsonStream.serialize(searchValues));
        final long end = System.currentTimeMillis();
        System.out.println(end - time);
    }

    @Test
    public void index() throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException,
            NoSuchFieldException, SecurityException, IOException, InstantiationException, InvocationTargetException {
        final var dao = CHRONICLE_DB.getMultiChronicleDao(daoClassName, DATA_PATH);
        dao.initIndex("fullName");
    }

    @Test
    public void indexedSearch() throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException,
            NoSuchFieldException, SecurityException, IOException, InstantiationException, InvocationTargetException {
        final long time = System.currentTimeMillis();
        final var dao = CHRONICLE_DB.getMultiChronicleDao(daoClassName, DATA_PATH);
        final var map = dao.indexedSearch(new Search("fullName", SearchType.EQUAL, "pheakdey ton"));
        final long end = System.currentTimeMillis();
        System.out.println(end - time);
        System.out.println(map);
    }

    @Test
    public void testMultiThreadDelete() throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException,
            SecurityException, ClassNotFoundException, IOException, InstantiationException, InvocationTargetException {
        final var dao = CHRONICLE_DB.getMultiChronicleDao(daoClassName, DATA_PATH);
        System.out.println(dao.size());
        dao.delete(new HashSet<>(Arrays.asList("1-99000", "2-99001")));
        System.out.println(dao.size());
        System.out.println(dao.get("Test"));
    }

    @Test
    public void testGetAll() throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException,
            NoSuchFieldException, SecurityException, IOException, InstantiationException, InvocationTargetException {
        final var dao = CHRONICLE_DB.getMultiChronicleDao(daoClassName, DATA_PATH);
        System.out.println(dao.get(new HashSet<>(Arrays.asList("0-10", "0-11"))));
    }
}