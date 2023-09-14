package chronicle.db.entity;

public record Search(String field, SearchType searchType, Object searchTerm) {
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
        STARTS_WITH,
        ENDS_WITH
    }

    public Search(final String field, final SearchType searchType, final Object searchTerm) {
        this.field = field;
        this.searchType = searchType;
        this.searchTerm = searchTerm;
    }
}
