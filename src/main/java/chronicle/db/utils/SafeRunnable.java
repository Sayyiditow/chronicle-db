package chronicle.db.utils;

import org.tinylog.Logger;

/**
 * A Runnable wrapper that catches and logs any exceptions thrown during
 * execution.
 * <p>
 * Ensures that a scheduled task or thread does not terminate silently due to an
 * unhandled exception.
 * </p>
 *
 * @param task   The CheckedRunnable task to execute
 * @param taskId A unique identifier for the task, used for logging
 */
public record SafeRunnable(CheckedRunnable task, String taskId) implements Runnable {
    /**
     * Executes the wrapped task.
     * <p>
     * If an exception occurs, it is logged with the task ID.
     * </p>
     */
    @Override
    public void run() {
        try {
            task.run();
        } catch (final Throwable e) {
            Logger.error("Error on runnable task [{}]. {}", taskId, e.getMessage());
            Logger.error(e);
        }
    }
}