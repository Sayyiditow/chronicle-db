package chronicle.db.entity;

/**
 * Represents the status of a put operation in ChronicleDao.
 * <p>
 * This enum is used to indicate the outcome of insert/update operations
 * when persisting data to the Chronicle database.
 * </p>
 */
public enum PutStatus {
    /** The record was successfully inserted as a new entry */
    INSERTED,

    /** The record was successfully updated (already existed) */
    UPDATED,

    /** The operation failed completely */
    FAILED,

    /** The operation partially succeeded (used for batch operations) */
    PARTIAL
}
