package chronicle.db.entity;

import java.util.Collections;
import java.util.List;

import com.jsoniter.annotation.JsonCreator;

public record CsvObject(String[] headers, List<Object[]> rows) {
    @JsonCreator
    public CsvObject(final String[] headers, final List<Object[]> rows) {
        this.headers = headers;
        this.rows = rows;
    }

    public static CsvObject empty() {
        return new CsvObject(new String[0], Collections.emptyList());
    }
}
