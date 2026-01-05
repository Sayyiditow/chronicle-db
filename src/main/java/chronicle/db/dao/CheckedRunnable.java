package chronicle.db.dao;

/**
 * A functional interface similar to Runnable but allows throwing checked
 * exceptions.
 * <p>
 * This is useful when working with APIs that throw checked exceptions inside
 * lambda expressions,
 * allowing the exception to propagate or be handled by a wrapper.
 * </p>
 */
@FunctionalInterface
public interface CheckedRunnable {
    /**
     * Performs this operation.
     *
     * @throws Throwable if unable to compute a result
     */
    void run() throws Throwable;
}
