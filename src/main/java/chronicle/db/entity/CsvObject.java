package chronicle.db.entity;

import java.util.List;

import com.jsoniter.annotation.JsonCreator;

public record CsvObject(String[] header, List<Object[]> rows) {
    @JsonCreator
    public CsvObject(final String[] header, final List<Object[]> rows) {
        this.header = header;
        this.rows = rows;
    }
}
