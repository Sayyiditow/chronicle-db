package chronicle.db.utils;

import java.util.function.Supplier;

import org.tinylog.Logger;

/**
 * A Supplier wrapper that catches and logs any exceptions thrown during
 * execution, returning a default failure value.
 *
 * @param <T>          The type of results supplied by this supplier
 * @param task         The CheckedSupplier task to execute
 * @param taskId       A unique identifier for the task, used for logging
 * @param failureValue The value to return if an exception occurs
 */
public record SafeSupplier<T>(CheckedSupplier<T> task, String taskId, T failureValue) implements Supplier<T> {
    /**
     * Executes the wrapped task.
     * <p>
     * If an exception occurs, it is logged with the task ID and the failureValue is
     * returned.
     * </p>
     *
     * @return the result of the task, or failureValue if an exception occurred
     */
    @Override
    public T get() {
        try {
            return task.get();
        } catch (final Throwable e) {
            Logger.error("Error on supplier task [{}]. {}", taskId, e.getMessage());
            Logger.error(e);
            return failureValue;
        }
    }
}
