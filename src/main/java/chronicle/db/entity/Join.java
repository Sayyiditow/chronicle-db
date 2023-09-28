package chronicle.db.entity;

import java.util.Map;

import net.openhft.chronicle.map.ChronicleMap;

public class Join {
    public String primaryObjectName, foreignObjectName, foreignKeyName, foreignKeyIndexPath;
    public Map<String, ChronicleMap<Object, Object>> primaryObject, foreignObject;
    public Object primaryUsing, foreignUsing;

    public Join(final String primaryObjectName, final String foreignObjectName, final String foreignKeyName,
            final String foreignKeyIndexPath, final Map<String, ChronicleMap<Object, Object>> primaryObject,
            final Map<String, ChronicleMap<Object, Object>> foreignObject, final Object primaryUsing,
            final Object foreignUsing) {
        this.primaryObjectName = primaryObjectName;
        this.foreignObjectName = foreignObjectName;
        this.foreignKeyName = foreignKeyName;
        this.foreignKeyIndexPath = foreignKeyIndexPath;
        this.primaryObject = primaryObject;
        this.foreignObject = foreignObject;
        this.primaryUsing = primaryUsing;
        this.foreignUsing = foreignUsing;
    }

    public Join() {
    }

    @Override
    public String toString() {
        return "Join [primaryObjectName=" + primaryObjectName + ", foreignObjectName=" + foreignObjectName
                + ", foreignKeyName=" + foreignKeyName + ", foreignKeyIndexPath=" + foreignKeyIndexPath
                + ", primaryObject=" + primaryObject + ", foreignObject=" + foreignObject + ", primaryUsing="
                + primaryUsing + ", foreignUsing=" + foreignUsing + "]";
    }

}
