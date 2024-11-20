package chronicle.db.entity;

import java.io.Serializable;

import com.jsoniter.annotation.JsonCreator;

public record Search(String field, SearchType searchType, Object searchTerm) implements Serializable {
    public enum SearchType {
        EQUAL,
        NOT_EQUAL,
        LESS,
        GREATER,
        LESS_OR_EQUAL,
        GREATER_OR_EQUAL,
        LIKE,
        NOT_LIKE,
        CONTAINS,
        NOT_CONTAINS,
        STARTS_WITH,
        ENDS_WITH,
        IN,
        NOT_IN
    }

    @JsonCreator
    public Search(final String field, final SearchType searchType, final Object searchTerm) {
        this.field = field;
        this.searchType = searchType;
        this.searchTerm = searchTerm;
    }
}
