package chronicle.db.utils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.TypeLiteral;

/**
 * Utility class for JSON processing.
 * <p>
 * Uses the Jsoniter library for high-performance reading and writing of JSON.
 * Also utilizes Jackson for specific tasks like canonicalization and consistent
 * number processing.
 * </p>
 */
public final class JsonUtils {
    private static final int flushSize = 5_242_880;

    private JsonUtils() {
    }

    /**
     * Converts a Java object to a JSON formatted String.
     * 
     * @param prop The Java object to convert
     * @param <T>  The type of the object
     * @return The JSON string, or null if the input is null
     */
    public static <T> String toJsonFromObj(final T prop) {
        if (prop == null)
            return null;
        return JsonStream.serialize(prop);
    }

    /**
     * Formats a compact JSON string into a pretty-printed version with indentation.
     *
     * @param json The compact JSON string
     * @return The beautified JSON string
     */
    public static String beautifyJson(final String json) {
        if (json == null || json.isEmpty()) {
            return json;
        }
        final int len = json.length();
        final StringBuilder prettyJson = new StringBuilder(len * 2);
        int indentLevel = 0;
        boolean inQuotes = false;

        for (int i = 0; i < len; i++) {
            final char ch = json.charAt(i);
            switch (ch) {
                case '"':
                    inQuotes = !inQuotes; // Toggle the inQuotes flag
                    prettyJson.append(ch);
                    break;
                case '{':
                case '[':
                    if (!inQuotes) {
                        prettyJson.append(ch).append('\n');
                        indentLevel++;
                        prettyJson.append("    ".repeat(indentLevel));
                    } else {
                        prettyJson.append(ch);
                    }
                    break;
                case '}':
                case ']':
                    if (!inQuotes) {
                        prettyJson.append('\n');
                        indentLevel--;
                        prettyJson.append("    ".repeat(Math.max(0, indentLevel)));
                        prettyJson.append(ch);
                    } else {
                        prettyJson.append(ch);
                    }
                    break;
                case ',':
                    prettyJson.append(ch);
                    if (!inQuotes) {
                        prettyJson.append('\n');
                        prettyJson.append("    ".repeat(indentLevel));
                    }
                    break;
                default:
                    prettyJson.append(ch);
            }
        }

        return prettyJson.toString();
    }

    /**
     * Converts a Java object to a pretty-printed JSON string.
     *
     * @param prop The Java object to convert
     * @param <T>  The type of the object
     * @return The pretty-printed JSON string, or null if input is null
     */
    public static <T> String toJsonPrettyFromObj(final T prop) {
        if (prop == null)
            return null;
        return beautifyJson(JsonStream.serialize(prop));
    }

    /**
     * Saves a Java object to a file as JSON.
     * 
     * @param path The full file path
     * @param prop The Java object to save
     * @param <T>  The type of the object
     * @throws IOException If an I/O error occurs
     */
    public static <T> void toJsonFileFromObj(final String path, final T prop) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(path); JsonStream stream = new JsonStream(fos, flushSize)) {
            stream.writeVal(prop);
            stream.flush();
        }
    }

    /**
     * Saves a Java object to a file as pretty-printed JSON.
     *
     * @param path The full file path
     * @param prop The Java object to save
     * @param <T>  The type of the object
     * @throws IOException If an I/O error occurs
     */
    public static <T> void toJsonFilePrettyFromObj(final String path, final T prop) throws IOException {
        Files.writeString(Path.of(path), toJsonPrettyFromObj(prop));
    }

    /**
     * Converts a JSON string to a Jsoniter {@link Any} object.
     * 
     * @param json The JSON string to convert
     * @return The Any object representing the JSON structure
     */
    public static Any fromJsonToAny(final String json) {
        return JsonIterator.deserialize(json);
    }

    /**
     * Converts a JSON string to a specific Java object type.
     * 
     * @param json        The JSON string
     * @param typeLiteral The Jsoniter TypeLiteral representing the target type
     * @param <T>         The type of the target object
     * @return The deserialized object
     */
    public static <T> T fromJsonToObj(final String json, final TypeLiteral<T> typeLiteral) {
        return JsonIterator.deserialize(json, typeLiteral);
    }

    /**
     * Merges a JSON string into an existing Java object.
     *
     * @param json        The JSON string containing updates
     * @param typeLiteral The Jsoniter TypeLiteral representing the target type
     * @param value       The existing object to update
     * @param <T>         The type of the object
     * @return The updated object
     * @throws IOException If an error occurs during parsing
     */
    public static <T> T fromJsonToObjMerge(final String json, final TypeLiteral<T> typeLiteral, final T value)
            throws IOException {
        final var iter = JsonIterator.parse(json);
        return iter.read(typeLiteral, value);
    }

    /**
     * Reads a JSON file and converts it to a Jsoniter {@link Any} object.
     * 
     * @param path The path to the JSON file
     * @return The Any object representing the file content
     * @throws IOException If an I/O error occurs
     */
    public static Any fromJsonFileToAny(final String path) throws IOException {
        return JsonIterator.deserialize(Files.readAllBytes(Path.of(path)));
    }

    /**
     * Reads a JSON file and converts it to a specific Java object type.
     * 
     * @param path        The path to the JSON file
     * @param typeLiteral The Jsoniter TypeLiteral representing the target type
     * @param <T>         The type of the target object
     * @return The deserialized object
     * @throws IOException If an I/O error occurs
     */
    public static <T> T fromJsonFileToObj(final String path, final TypeLiteral<T> typeLiteral) throws IOException {
        return JsonIterator.deserialize(Files.readAllBytes(Path.of(path)), typeLiteral);
    }
}
