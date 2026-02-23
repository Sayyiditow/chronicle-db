package chronicle.db.service;

import java.util.Map;

/**
 * Interface for tasks that need to run with direct memory access
 * on the database server for maximum performance.
 */
public interface DbTask {
    /**
     * Executes the task logic.
     * 
     * @param params Parameters passed from the client (e.g., tenantId, source, date
     *               range).
     * @return Any result object that Kryo can serialize back to the client.
     */
    Object execute(Map<String, Object> params, ReplicationQueue replicationQueue) throws Throwable;
}