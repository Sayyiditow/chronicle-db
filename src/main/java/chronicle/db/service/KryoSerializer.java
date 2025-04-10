package chronicle.db.service;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoSerializer {
    private static final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(() -> {
        final Kryo kryo = new Kryo();
        kryo.setRegistrationRequired(false); // No explicit registration
        kryo.setReferences(true); // Handle object references
        return kryo;
    });

    public static byte[] serialize(final Object obj) {
        final Kryo kryo = kryoThreadLocal.get();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final Output output = new Output(baos);
        kryo.writeObject(output, obj);
        output.close();
        return baos.toByteArray();
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> deserialize(final byte[] data) {
        final Kryo kryo = kryoThreadLocal.get();
        final Input input = new Input(data);
        final var result = kryo.readObject(input, HashMap.class);
        input.close();
        return result;
    }
}
