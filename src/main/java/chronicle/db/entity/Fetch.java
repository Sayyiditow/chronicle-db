package chronicle.db.entity;

import java.util.HashSet;

import com.jsoniter.annotation.JsonCreator;

public record Fetch(String objectName, String dataPath, HashSet<?> keys, Search search,
        String[] subsetFields, Object key, int limit) {
    @JsonCreator
    public Fetch(final String objectName, final String dataPath, final HashSet<?> keys,
            final Search search, final String[] subsetFields, final Object key, final int limit) {
        this.objectName = objectName;
        this.dataPath = dataPath;
        this.keys = keys;
        this.search = search;
        this.subsetFields = subsetFields;
        this.key = key;
        this.limit = limit;
    }
}
