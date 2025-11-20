package chronicle.db.entity;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Interface for entities stored in ChronicleDao that support CSV-like
 * operations.
 * <p>
 * Implementing this interface allows entities to be queried and returned in
 * tabular
 * formats (CSV, subset queries). It provides methods to extract field values by
 * name
 * and convert entities to row-based representations.
 * </p>
 * <p>
 * The interface includes a utility main method to generate boilerplate code for
 * implementing the {@link #getFieldValue(String)} method.
 * </p>
 */
public interface IChronicle {
    /**
     * Returns the header (column names) for this entity type.
     * 
     * @return Array of field names representing the columns
     */
    String[] header();

    /**
     * Converts this entity to a row representation with the given key.
     * 
     * @param key The primary key for this entity
     * @return Array where first element is the key, followed by field values
     */
    Object[] row(final String key);

    /**
     * Retrieves the value of a specific field by name.
     * <p>
     * Implementations should use a switch expression to map field names to values.
     * Array fields should be cloned to prevent external modification.
     * </p>
     * 
     * @param fieldName The name of the field to retrieve
     * @return The field value, or throws IllegalArgumentException if field doesn't
     *         exist
     * @throws IllegalArgumentException if the field name is unknown
     */
    Object getFieldValue(final String fieldName);

    /**
     * Extracts a subset of fields from this entity as a Map.
     * 
     * @param fields Array of field names to include in the subset
     * @return Map of field names to their values
     */
    default Map<String, Object> subset(final String[] fields) {
        final var map = new HashMap<String, Object>();
        for (int i = 0; i < fields.length; i++) {
            map.put(fields[i], getFieldValue(fields[i]));
        }
        return map;
    }

    /**
     * Converts a subset of fields to a row representation with the given key.
     * 
     * @param key    The primary key for this entity
     * @param fields Array of field names to include in the row
     * @return Array where first element is the key, followed by the requested field
     *         values
     */
    default Object[] subsetRow(final String key, final String[] fields) {
        final var row = new Object[fields.length + 1];
        row[0] = key;
        for (int i = 0; i < fields.length; i++) {
            row[i + 1] = getFieldValue(fields[i]);
        }
        return row;
    }

    /**
     * Utility method to generate boilerplate code for implementing
     * {@link #getFieldValue(String)}.
     * <p>
     * This interactive tool prompts for field names and generates a switch
     * expression
     * that maps field names to their values. Array fields are automatically cloned.
     * </p>
     * 
     * @param args Command line arguments (not used)
     */
    public static void main(final String[] args) {
        final Scanner scanner = new Scanner(System.in);

        System.out.println("Enter comma-separated regular field names (no cloning):");
        final String regularInput = scanner.nextLine();

        System.out.println("Enter comma-separated array field names (with .clone()):");
        final String arrayInput = scanner.nextLine();

        scanner.close();

        System.out.println("@Override");
        System.out.println("public Object getFieldValue(final String fieldName) {");
        System.out.println("    return switch (fieldName) {");

        // Process regular fields
        if (!regularInput.trim().isEmpty()) {
            final String[] regularFields = regularInput.split(",");
            for (final String field : regularFields) {
                final String trimmed = field.trim();
                System.out.printf("        case \"%s\" -> %s;%n", trimmed, trimmed);
            }
        }

        // Process array fields
        if (!arrayInput.trim().isEmpty()) {
            final String[] arrayFields = arrayInput.split(",");
            for (final String field : arrayFields) {
                final String trimmed = field.trim();
                System.out.printf("        case \"%s\" -> %s != null ? %s.clone() : null;%n",
                        trimmed, trimmed, trimmed);
            }
        }

        System.out.println(
                "        default -> throw new IllegalArgumentException(\"Unknown field: \" + fieldName + \" for object: \" + this.getClass().getSimpleName());");
        System.out.println("    };");
        System.out.println("}");
    }
}
