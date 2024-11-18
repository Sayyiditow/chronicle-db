package chronicle.db.entity;

import java.io.Serializable;
import java.util.List;

import com.jsoniter.annotation.JsonCreator;

public record CsvObject(String[] headers, List<Object[]> rows) implements Serializable {
    @JsonCreator
    public CsvObject(final String[] headers, final List<Object[]> rows) {
        this.headers = headers;
        this.rows = rows;
    }
}
