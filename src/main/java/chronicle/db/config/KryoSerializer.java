package chronicle.db.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.tinylog.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

public class KryoSerializer {
    private static final Queue<Kryo> kryoPool = new ConcurrentLinkedQueue<>();
    private static final Queue<Output> outputPool = new ConcurrentLinkedQueue<>();
    private static final Queue<Input> inputPool = new ConcurrentLinkedQueue<>();

    private static Kryo createKryo() {
        final Kryo kryo = new Kryo();
        kryo.register(ArrayList.class);
        kryo.register(LinkedList.class);
        kryo.register(HashMap.class);
        kryo.register(LinkedHashMap.class);
        kryo.register(TreeMap.class);
        kryo.register(HashSet.class);
        kryo.register(LinkedHashSet.class);
        kryo.register(TreeSet.class);
        kryo.register(List.of().getClass());
        kryo.register(Set.of().getClass());
        kryo.register(Map.of().getClass());
        kryo.register(String.class);
        kryo.register(Integer.class);
        kryo.register(Long.class);
        kryo.register(Double.class);
        kryo.register(Float.class);
        kryo.register(Boolean.class);
        kryo.register(Byte.class);
        kryo.register(Character.class);
        kryo.register(String[].class);
        kryo.register(Integer[].class);
        kryo.register(Long[].class);
        kryo.register(Boolean[].class);
        kryo.register(Object[].class);
        kryo.register(ConcurrentHashMap.KeySetView.class, new JavaSerializer());
        kryo.setRegistrationRequired(false); // No explicit registration
        kryo.setReferences(false); // Handle object references
        return kryo;
    }

    public static byte[] serialize(final Object obj) {
        Kryo kryo = kryoPool.poll();
        if (kryo == null) {
            kryo = createKryo();
        }
        Output output = outputPool.poll();
        if (output == null) {
            output = new Output(16384, -1);
        }

        try {
            output.setPosition(0); // Reset buffer for reuse
            kryo.writeObject(output, obj);
            return output.toBytes();
        } finally {
            kryo.reset(); // Reset object references
            kryoPool.offer(kryo);
            if (output.getBuffer().length < 1024 * 1024) { // Only pool buffers < 1MB
                outputPool.offer(output);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> deserialize(final byte[] data) {
        if (data == null || data.length == 0) {
            Logger.info("Data is null or empty during Kryo deserialization.");
            return null;
        }
        Kryo kryo = kryoPool.poll();
        if (kryo == null) {
            kryo = createKryo();
        }
        Input input = inputPool.poll();
        if (input == null) {
            input = new Input();
        }

        try {
            input.setBuffer(data); // Reuse input
            return kryo.readObject(input, HashMap.class);
        } finally {
            kryo.reset();
            kryoPool.offer(kryo);
            inputPool.offer(input);
        }
    }
}
