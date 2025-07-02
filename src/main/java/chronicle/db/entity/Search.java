package chronicle.db.entity;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.jsoniter.annotation.JsonCreator;

public record Search(String field, SearchType searchType, Object searchTerm, int limit) {
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

    public static final Set<SearchType> RANGE_SEARCH_TYPE = Set.of(SearchType.LESS, SearchType.LESS_OR_EQUAL,
            SearchType.GREATER, SearchType.GREATER_OR_EQUAL);

    private static final Map<SearchType, Integer> SEARCH_TYPE_PRIORITY = Map.ofEntries(
            Map.entry(SearchType.EQUAL, 0),
            Map.entry(SearchType.STARTS_WITH, 1),
            Map.entry(SearchType.IN, 2),
            Map.entry(SearchType.ENDS_WITH, 3),
            Map.entry(SearchType.BETWEEN, 4),
            Map.entry(SearchType.LIKE, 5),
            Map.entry(SearchType.GREATER, 6),
            Map.entry(SearchType.LESS, 7),
            Map.entry(SearchType.NOT_EQUAL, 8),
            Map.entry(SearchType.NOT_IN, 9),
            Map.entry(SearchType.NOT_LIKE, 10),
            Map.entry(SearchType.NOT_CONTAINS, 11),
            Map.entry(SearchType.CONTAINS, 12));

    public static void sortSearchesByTypePriority(final List<Search> searches) {
        searches.sort(
                Comparator.comparingInt(s -> SEARCH_TYPE_PRIORITY.getOrDefault(s.searchType(), Integer.MAX_VALUE)));
    }

    @JsonCreator
    public Search(final String field, final SearchType searchType, final Object searchTerm, final int limit) {
        this.field = field;
        this.searchType = searchType;
        this.searchTerm = searchTerm;
        this.limit = limit;
    }

    public Search withSearchTerm(final Object searchTerm) {
        return new Search(this.field, this.searchType, searchTerm, this.limit);
    }
}
