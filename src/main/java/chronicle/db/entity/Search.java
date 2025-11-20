package chronicle.db.entity;

import com.jsoniter.annotation.JsonCreator;

/**
 * Represents a search criterion for querying data in ChronicleDao.
 * <p>
 * This record encapsulates all parameters needed to perform a search operation,
 * including the field to search on, the type of comparison, the search term,
 * result limits, and index usage preferences.
 * </p>
 * 
 * @param field The name of the field to search on
 * @param searchType The type of comparison to perform
 * @param searchTerm The value to search for (can be a single value, List for IN/NOT_IN, or array for BETWEEN)
 * @param limit Maximum number of results to return (-1 for unlimited)
 * @param skipIndex If true, bypasses index usage and performs a full scan
 */
public record Search(String field, SearchType searchType, Object searchTerm, int limit, boolean skipIndex) {
    
    /**
     * Defines the types of search operations supported by ChronicleDao.
     */
    public enum SearchType {
        /** Exact equality match */
        EQUAL,
        
        /** Not equal to */
        NOT_EQUAL,
        
        /** Less than */
        LESS,
        
        /** Greater than */
        GREATER,
        
        /** Less than or equal to */
        LESS_OR_EQUAL,
        
        /** Greater than or equal to */
        GREATER_OR_EQUAL,
        
        /** Case-insensitive pattern matching (supports wildcards) */
        LIKE,
        
        /** Negation of LIKE */
        NOT_LIKE,
        
        /** Case-insensitive substring search */
        CONTAINS,
        
        /** Negation of CONTAINS */
        NOT_CONTAINS,
        
        /** Case-insensitive prefix match */
        STARTS_WITH,
        
        /** Case-insensitive suffix match */
        ENDS_WITH,
        
        /** Value is in the provided list */
        IN,
        
        /** Value is not in the provided list */
        NOT_IN,
        
        /** Value is between two values (inclusive) */
        BETWEEN
    }

    /**
     * Canonical constructor with all fields (used by JsonIter for deserialization).
     * 
     * @param field The field name to search on
     * @param searchType The type of search operation
     * @param searchTerm The search value
     * @param limit Maximum results (-1 for unlimited)
     * @param skipIndex Whether to skip index usage
     */
    @JsonCreator
    public Search(final String field, final SearchType searchType, final Object searchTerm, final int limit,
            final boolean skipIndex) {
        this.field = field;
        this.searchType = searchType;
        this.searchTerm = searchTerm;
        this.limit = limit;
        this.skipIndex = skipIndex;
    }

    /**
     * Constructor with default limit (-1) and skipIndex (false).
     * 
     * @param field The field name to search on
     * @param searchType The type of search operation
     * @param searchTerm The search value
     */
    public Search(final String field, final SearchType searchType, final Object searchTerm) {
        this(field, searchType, searchTerm, -1, false);
    }

    /**
     * Constructor with default limit (-1).
     * 
     * @param field The field name to search on
     * @param searchType The type of search operation
     * @param searchTerm The search value
     * @param skipIndex Whether to skip index usage
     */
    public Search(final String field, final SearchType searchType, final Object searchTerm, final boolean skipIndex) {
        this(field, searchType, searchTerm, -1, skipIndex);
    }

    /**
     * Constructor with default skipIndex (false).
     * 
     * @param field The field name to search on
     * @param searchType The type of search operation
     * @param searchTerm The search value
     * @param limit Maximum results (-1 for unlimited)
     */
    public Search(final String field, final SearchType searchType, final Object searchTerm, final int limit) {
        this(field, searchType, searchTerm, limit, false);
    }

    /**
     * Creates a new Search with a different search term.
     * 
     * @param searchTerm The new search term
     * @return A new Search instance with the updated search term
     */
    public Search withSearchTerm(final Object searchTerm) {
        return new Search(this.field, this.searchType, searchTerm, this.limit, this.skipIndex);
    }

    /**
     * Creates a new Search with a different skipIndex value.
     * 
     * @param skip Whether to skip index usage
     * @return A new Search instance with the updated skipIndex value
     */
    public Search withSkipIndex(final boolean skip) {
        return new Search(this.field, this.searchType, this.searchTerm, this.limit, skip);
    }

    /**
     * Creates a new Search with a different limit.
     * 
     * @param limit Maximum results (-1 for unlimited)
     * @return A new Search instance with the updated limit
     */
    public Search withLimit(final int limit) {
        return new Search(this.field, this.searchType, this.searchTerm, limit, this.skipIndex);
    }
}
