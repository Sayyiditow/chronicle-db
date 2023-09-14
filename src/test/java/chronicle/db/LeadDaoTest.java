package chronicle.db;

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
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.junit.Test;

import com.jsoniter.JsonIterator;
import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.TypeLiteral;

import chronicle.db.config.Globals;
import chronicle.db.entity.Search;
import chronicle.db.entity.Search.SearchType;

@SuppressWarnings("unchecked")
public class LeadDaoTest {
    private final String objectClassName = "chronicle.db.service.Lead";
    private final String daoClassName = "chronicle.db.service.LeadDao";

    @Test
    public void testInsert() throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException,
            NoSuchFieldException, SecurityException, IOException, InstantiationException, InvocationTargetException {
        final var dao = CHRONICLE_DB.getMultiChronicleDao(daoClassName, Globals.DATA_PATH);
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
        final var dao = CHRONICLE_DB.getMultiChronicleDao(daoClassName, Globals.DATA_PATH);
        System.out.println(dao.size());
        final var searchValues = dao.search(new Search("fullName", Search.SearchType.LIKE, "pheakdey ton"));
        System.out.println(JsonStream.serialize(searchValues));
        final long end = System.currentTimeMillis();
        System.out.println(end - time);
    }

    @Test
    public void index() throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException,
            NoSuchFieldException, SecurityException, IOException, InstantiationException, InvocationTargetException {
        final var dao = CHRONICLE_DB.getMultiChronicleDao(daoClassName, Globals.DATA_PATH);
        dao.initIndex("fullName");
    }

    @Test
    public void indexedSearch() throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException,
            NoSuchFieldException, SecurityException, IOException, InstantiationException, InvocationTargetException {
        final long time = System.currentTimeMillis();
        final var dao = CHRONICLE_DB.getMultiChronicleDao(daoClassName, Globals.DATA_PATH);
        final var map = dao.indexedSearch(new Search("fullName", SearchType.EQUAL, "pheakdey ton"));
        final long end = System.currentTimeMillis();
        System.out.println(end - time);
        System.out.println(map);
    }

    @Test
    public void testJoin() {
        final ConcurrentMap<String, Customer> map = new ConcurrentHashMap<>();
        map.put("test", new Customer("hashim", 1));
        map.put("test3", new Customer("hashim2", 2));
        final ConcurrentMap<String, Invoice> map2 = new ConcurrentHashMap<>();
        map2.put("1", new Invoice("test", "abcd12"));
        map2.put("2", new Invoice("test", "abcd23"));
        map2.put("3", new Invoice("test", "abcd45"));
        map2.put("4", new Invoice("test3", "abcd67"));
        map2.put("5", new Invoice("test3", "abcd78"));
        map2.put("6", new Invoice("test4", "abcd89"));
        final List<Object> list = new ArrayList<>();

        // inner join
        map2.forEach((key, value) -> {
            final ConcurrentMap<String, Object> valueMap = new ConcurrentHashMap<>();
            final Customer customer = map.get(value.customerNo());
            if (Objects.nonNull(customer)) {
                valueMap.put("Name", customer.name());
                valueMap.put("Type", customer.type());
                valueMap.put("Invoice Id", key);
                list.add(valueMap);
            }
        });
        System.out.println(list);
        list.clear();

        // left join
        map2.forEach((k, v) -> {
            final ConcurrentMap<String, Object> valueMap = new ConcurrentHashMap<>();
            Customer cust = map.get(v.customerNo());
            cust = cust == null ? new Customer() : cust;
            valueMap.put("Name", cust.name());
            valueMap.put("Type", cust.type());
            valueMap.put("Invoice Id", k);
            list.add(valueMap);
        });
        System.out.println(list);
    }

    @Test
    public void testMultiThreadDelete() throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException,
            SecurityException, ClassNotFoundException, IOException, InstantiationException, InvocationTargetException {
        final var dao = CHRONICLE_DB.getMultiChronicleDao(daoClassName, Globals.DATA_PATH);
        System.out.println(dao.size());
        dao.delete(new HashSet<>(Arrays.asList("1-99000", "2-99001")));
        System.out.println(dao.size());
        System.out.println(dao.get("Test"));
    }

    @Test
    public void testGetAll() throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException,
            NoSuchFieldException, SecurityException, IOException, InstantiationException, InvocationTargetException {
        final var dao = CHRONICLE_DB.getMultiChronicleDao(daoClassName, Globals.DATA_PATH);
        System.out.println(dao.get(new HashSet<>(Arrays.asList("0-10", "0-11"))));
    }
}