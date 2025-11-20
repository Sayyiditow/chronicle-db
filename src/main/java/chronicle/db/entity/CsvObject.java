package chronicle.db.entity;

import java.util.Collections;
import java.util.List;

import com.jsoniter.annotation.JsonCreator;

/**
 * Represents a CSV-like data structure with headers and rows.
 * <p>
 * This record is used to return query results in a tabular format,
 * similar to CSV files, where the first array contains column headers
 * and subsequent arrays contain the data rows.
 * </p>
 * 
 * @param headers Array of column names/headers
 * @param rows    List of data rows, where each row is an Object array matching
 *                the headers
 */
public record CsvObject(String[] headers, List<Object[]> rows) {
    /**
     * Canonical constructor for JSON deserialization.
     * 
     * @param headers Array of column names
     * @param rows    List of data rows
     */
    @JsonCreator
    public CsvObject(final String[] headers, final List<Object[]> rows) {
        this.headers = headers;
        this.rows = rows;
    }

    /**
     * Creates an empty CsvObject with no headers or rows.
     * 
     * @return An empty CsvObject instance
     */
    public static CsvObject empty() {
        return new CsvObject(new String[0], Collections.emptyList());
    }
}
