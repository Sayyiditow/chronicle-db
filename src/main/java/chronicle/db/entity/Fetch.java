package chronicle.db.entity;

import java.util.List;
import java.util.Set;

import com.jsoniter.annotation.JsonCreator;

public record Fetch(String pathPrefix, String objectEnum, Object key, Set<?> keys, List<Search> search, int limit,
        String[] subsetFields) {
    @JsonCreator
    public Fetch(final String pathPrefix, final String objectEnum, final Object key, final Set<?> keys,
            final List<Search> search, final int limit, final String[] subsetFields) {
        this.pathPrefix = pathPrefix;
        this.objectEnum = objectEnum;
        this.key = key;
        this.keys = keys;
        this.limit = limit;
        this.search = search;
        this.subsetFields = subsetFields;
    }
}
