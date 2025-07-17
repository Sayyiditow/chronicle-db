package chronicle.db.entity;

import com.jsoniter.annotation.JsonCreator;

public record Search(String field, SearchType searchType, Object searchTerm, int limit, boolean skipIndex) {
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
        NOT_IN,
        BETWEEN
    }

    // Canonical constructor with all fields (used by JsonIter)
    @JsonCreator
    public Search(final String field, final SearchType searchType, final Object searchTerm, final int limit,
            final boolean skipIndex) {
        this.field = field;
        this.searchType = searchType;
        this.searchTerm = searchTerm;
        this.limit = limit;
        this.skipIndex = skipIndex;
    }

    // Secondary constructor with default limit = -1 and skipIndex false
    public Search(final String field, final SearchType searchType, final Object searchTerm) {
        this(field, searchType, searchTerm, -1, false);
    }

    // Secondary constructor with default limit = -1
    public Search(final String field, final SearchType searchType, final Object searchTerm, final boolean skipIndex) {
        this(field, searchType, searchTerm, -1, skipIndex);
    }

    // Secondary constructor with default skipIndex = false
    public Search(final String field, final SearchType searchType, final Object searchTerm, final int limit) {
        this(field, searchType, searchTerm, limit, false);
    }

    public Search withSearchTerm(final Object searchTerm) {
        return new Search(this.field, this.searchType, searchTerm, this.limit, this.skipIndex);
    }

    public Search withSkipIndex(final boolean skip) {
        return new Search(this.field, this.searchType, this.searchTerm, this.limit, skip);
    }
}
