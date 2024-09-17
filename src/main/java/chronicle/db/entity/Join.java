package chronicle.db.entity;

import com.jsoniter.annotation.JsonCreator;

public record Join(String objPath, String foreignKeyObjPath, String objDaoName, String foreignKeyObjDaoName,
        String foreignKeyName, JoinFilter objFilter, JoinFilter foreignKeyObjFilter,
        boolean isInnerJoin, boolean foreignIsMainObject) {
    @JsonCreator
    public Join(final String objPath, final String foreignKeyObjPath, final String objDaoName,
            final String foreignKeyObjDaoName, final String foreignKeyName,
            final JoinFilter objFilter, final JoinFilter foreignKeyObjFilter, final boolean isInnerJoin,
            final boolean foreignIsMainObject) {
        this.objPath = objPath;
        this.foreignKeyObjPath = foreignKeyObjPath;
        this.objDaoName = objDaoName;
        this.foreignKeyObjDaoName = foreignKeyObjDaoName;
        this.foreignKeyName = foreignKeyName;
        this.objFilter = objFilter;
        this.foreignKeyObjFilter = foreignKeyObjFilter;
        this.isInnerJoin = isInnerJoin;
        this.foreignIsMainObject = foreignIsMainObject;
    }
}
