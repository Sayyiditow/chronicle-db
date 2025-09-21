package chronicle.db.entity;

import java.util.LinkedHashMap;
import java.util.Scanner;

public interface IChronicle {
    String[] header();

    Object[] row(final String key);

    Object getFieldValue(final String fieldName);

    default LinkedHashMap<String, Object> subset(final String[] fields) {
        final var map = new LinkedHashMap<String, Object>();
        for (int i = 0; i < fields.length; i++) {
            map.put(fields[i], getFieldValue(fields[i]));
        }
        return map;
    }

    default Object[] subsetRow(final String key, final String[] fields) {
        final var row = new Object[fields.length + 1];
        row[0] = key;
        for (int i = 0; i < fields.length; i++) {
            row[i + 1] = getFieldValue(fields[i]);
        }
        return row;
    }

    public static void main(final String[] args) {
        final Scanner scanner = new Scanner(System.in);
        System.out.println("Enter comma-separated field names:");
        final String input = scanner.nextLine();
        scanner.close();

        final String[] fields = input.split(",");

        System.out.println("@Override");
        System.out.println("public Object getFieldValue(final String fieldName) {");
        System.out.println("    return switch (fieldName) {");
        for (final String field : fields) {
            final String trimmed = field.trim();
            System.out.printf("        case \"%s\" -> %s;%n", trimmed, trimmed);
        }
        System.out.println("        default -> throw new IllegalArgumentException(\"Unknown field: \" + fieldName);");
        System.out.println("    };");
        System.out.println("}");
    }
}
