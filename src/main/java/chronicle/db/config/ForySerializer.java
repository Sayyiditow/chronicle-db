package chronicle.db.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.fory.Fory;
import org.apache.fory.ThreadSafeFory;
import org.apache.fory.config.Language;

public class ForySerializer {
    // ThreadSafeFory is internally pooled and highly optimized
    private static final ThreadSafeFory fory = Fory.builder()
            .withLanguage(Language.JAVA)
            .requireClassRegistration(false)
            .suppressClassRegistrationWarnings(true)
            .withRefTracking(false)
            .buildThreadSafeFory();

    static {
        // Core Java Collections
        fory.register(ArrayList.class);
        fory.register(LinkedList.class);
        fory.register(HashMap.class);
        fory.register(LinkedHashMap.class);
        fory.register(TreeMap.class);
        fory.register(HashSet.class);
        fory.register(LinkedHashSet.class);
        fory.register(TreeSet.class);

        // JDK 21+ Immutable Collections (List.of, Map.of)
        fory.register(List.of().getClass());
        fory.register(Set.of().getClass());
        fory.register(Map.of().getClass());

        // Primitives and Wrappers
        fory.register(String.class);
        fory.register(Integer.class);
        fory.register(Long.class);
        fory.register(Double.class);
        fory.register(Boolean.class);
        fory.register(byte[].class);
        fory.register(Object[].class);

        // Advanced Collections
        fory.register(ConcurrentHashMap.class);
        fory.register(ConcurrentHashMap.KeySetView.class);
    }

    /**
     * Serializes an object to a byte array using Fory's JIT-optimized path.
     */
    public static byte[] serialize(final Object obj) {
        if (obj == null)
            return null;

        return fory.serialize(obj);
    }

    /**
     * Deserializes a byte array back into a Map.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> deserialize(final byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        return (Map<String, Object>) fory.deserialize(data);
    }
}
