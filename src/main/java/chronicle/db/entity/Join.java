package chronicle.db.entity;

import com.jsoniter.annotation.JsonCreator;

public record Join(String dataPath, String primaryDaoClassName, String foreignDaoClassName, String foreignKeyName,
        JoinObjMultiMode joinObjMultiMode) {
    @JsonCreator
    public Join(final String dataPath, final String primaryDaoClassName, final String foreignDaoClassName,
            final String foreignKeyName, final JoinObjMultiMode joinObjMultiMode) {
        this.dataPath = dataPath;
        this.primaryDaoClassName = primaryDaoClassName;
        this.foreignDaoClassName = foreignDaoClassName;
        this.foreignKeyName = foreignKeyName;
        this.joinObjMultiMode = joinObjMultiMode;
    }
}
