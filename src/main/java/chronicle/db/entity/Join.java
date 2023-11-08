package chronicle.db.entity;

import com.jsoniter.annotation.JsonCreator;

public record Join(String primaryPath, String foreignPath, String primaryDaoClassName, String foreignDaoClassName,
        String foreignKeyName, JoinObjMultiMode joinObjMultiMode, JoinFilter primaryFilter, JoinFilter foreignFilter) {
    @JsonCreator
    public Join(final String primaryPath, final String foreignPath, final String primaryDaoClassName,
            final String foreignDaoClassName, final String foreignKeyName, final JoinObjMultiMode joinObjMultiMode,
            final JoinFilter primaryFilter, final JoinFilter foreignFilter) {
        this.primaryPath = primaryPath;
        this.foreignPath = foreignPath;
        this.primaryDaoClassName = primaryDaoClassName;
        this.foreignDaoClassName = foreignDaoClassName;
        this.foreignKeyName = foreignKeyName;
        this.joinObjMultiMode = joinObjMultiMode;
        this.primaryFilter = primaryFilter;
        this.foreignFilter = foreignFilter;
    }
}
