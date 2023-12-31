package chronicle.db.entity;

import com.jsoniter.annotation.JsonCreator;

public record Join(String objPath, String foreignKeyObjPath, String objDaoName, String foreignKeyObjDaoName,
        String foreignKeyName, JoinObjMultiMode joinObjMultiMode, JoinFilter objFilter, JoinFilter foreignKeyObjFilter,
        boolean isInnerJoin) {
    @JsonCreator
    public Join(final String objPath, final String foreignKeyObjPath, final String objDaoName,
            final String foreignKeyObjDaoName, final String foreignKeyName, final JoinObjMultiMode joinObjMultiMode,
            final JoinFilter objFilter, final JoinFilter foreignKeyObjFilter, final boolean isInnerJoin) {
        this.objPath = objPath;
        this.foreignKeyObjPath = foreignKeyObjPath;
        this.objDaoName = objDaoName;
        this.foreignKeyObjDaoName = foreignKeyObjDaoName;
        this.foreignKeyName = foreignKeyName;
        this.joinObjMultiMode = joinObjMultiMode;
        this.objFilter = objFilter;
        this.foreignKeyObjFilter = foreignKeyObjFilter;
        this.isInnerJoin = isInnerJoin;
    }
}
