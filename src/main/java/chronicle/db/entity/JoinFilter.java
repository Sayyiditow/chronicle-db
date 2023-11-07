package chronicle.db.entity;

import java.util.HashSet;

import com.jsoniter.annotation.JsonCreator;

public record JoinFilter(Object key, HashSet<?> keys, Search search, int limit,
        String[] subsetFields) {
    @JsonCreator
    public JoinFilter(final Object key, final HashSet<?> keys, final Search search, final int limit,
            final String[] subsetFields) {
        this.key = key;
        this.keys = keys;
        this.search = search;
        this.limit = limit;
        this.subsetFields = subsetFields;
    }
}
