package chronicle.db.service;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import org.tinylog.Logger;

/**
 * Loads DbTask implementations from a plugin directory.
 * Supports hot-reloading by refreshing the classloader.
 */
public final class TaskLoader {
    private static final Path TASK_PLUGIN_DIR = Path.of(System.getProperty("chronicle.tasks.dir", "../lib/tasks"));
    private static URLClassLoader taskClassLoader;

    private TaskLoader() {}

    /**
     * Refreshes the task classloader by scanning the plugin directory for JARs.
     * Call this to pick up new or updated task JARs without restarting.
     */
    public static synchronized void refresh() throws IOException {
        if (taskClassLoader != null) {
            try {
                taskClassLoader.close();
            } catch (final IOException e) {
                Logger.warn("Failed to close previous task classloader: {}", e.getMessage());
            }
            taskClassLoader = null;
        }

        if (!Files.exists(TASK_PLUGIN_DIR)) {
            Logger.info("Task plugin directory [{}] does not exist, using system classloader.", TASK_PLUGIN_DIR);
            return;
        }

        final var urls = new ArrayList<URL>();

        // Add all JAR files in the plugin directory
        try (var stream = Files.list(TASK_PLUGIN_DIR)) {
            stream.filter(p -> p.toString().endsWith(".jar"))
                  .forEach(p -> {
                      try {
                          urls.add(p.toUri().toURL());
                          Logger.info("Loaded task JAR: {}", p.getFileName());
                      } catch (final Exception e) {
                          Logger.error("Failed to load task JAR [{}]: {}", p, e.getMessage());
                      }
                  });
        }

        if (!urls.isEmpty()) {
            taskClassLoader = new URLClassLoader(
                urls.toArray(new URL[0]),
                TaskLoader.class.getClassLoader()
            );
            Logger.info("Task classloader refreshed with [{}] JAR(s) from [{}]", urls.size(), TASK_PLUGIN_DIR);
        } else {
            Logger.info("No task JARs found in [{}]", TASK_PLUGIN_DIR);
        }
    }

    /**
     * Loads a task class by fully qualified name.
     * Tries the plugin classloader first, falls back to system classloader.
     */
    public static DbTask loadTask(final String taskClass) throws Exception {
        Class<?> clazz = null;

        // Try plugin classloader first
        if (taskClassLoader != null) {
            try {
                clazz = taskClassLoader.loadClass(taskClass);
                Logger.debug("Loaded task [{}] from plugin classloader", taskClass);
            } catch (final ClassNotFoundException e) {
                // Fall through to system classloader
            }
        }

        // Fall back to system classloader
        if (clazz == null) {
            clazz = Class.forName(taskClass);
            Logger.debug("Loaded task [{}] from system classloader", taskClass);
        }

        return (DbTask) clazz.getDeclaredConstructor().newInstance();
    }

    /**
     * Returns the plugin directory path.
     */
    public static Path getPluginDir() {
        return TASK_PLUGIN_DIR;
    }
}
