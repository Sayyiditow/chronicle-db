package chronicle.db.entity;

import java.util.HashSet;

import com.jsoniter.annotation.JsonCreator;

public record Fetch(String objectName, Object key, HashSet<?> keys, Search[] search, int limit,
        String[] subsetFields) {
    @JsonCreator
    public Fetch(final String objectName, final Object key, final HashSet<?> keys,
            final Search[] search, final int limit, final String[] subsetFields) {
        this.objectName = objectName;
        this.key = key;
        this.keys = keys;
        this.limit = limit;
        this.search = search;
        this.subsetFields = subsetFields;
    }
}
