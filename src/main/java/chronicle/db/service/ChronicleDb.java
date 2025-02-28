package chronicle.db.service;

import static chronicle.db.dao.ChronicleUtils.CHRONICLE_UTILS;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.tinylog.Logger;

import chronicle.db.dao.ChronicleDao;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

/**
 * Using this DB requires to use Value interfaces from Chronical Map:
 * https://github.com/OpenHFT/Chronicle-Values
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public final class ChronicleDb {
    public static final ChronicleDb CHRONICLE_DB = new ChronicleDb();

    /**
     * Create or fetch a db
     * 
     * @param entries    the number of entries of the db as a starter
     * @param averageKey the average key
     * @param filePath   the path to the file to create
     * @param keyClass   the class of the key
     * @param valueClass the class of the value (best to implement Value interface
     *                   for complex structures)
     * @throws IOException
     */
    public <K, V> ChronicleMap<K, V> getDb(final String name, final long entries,
            final K averageKey, final V averageValue, final String filePath, final double maxBloatFactor)
            throws IOException {
        Logger.info("Opening ChronicleMap at: {}", filePath);
        final File file = new File(filePath);
        final Class<K> keyClass = (Class<K>) averageKey.getClass();
        final Class<V> valueClass = (Class<V>) averageValue.getClass();

        if (file.exists()) {
            return ChronicleMapBuilder.of(keyClass, valueClass).maxBloatFactor(maxBloatFactor)
                    .createPersistedTo(file);
        }

        return ChronicleMapBuilder.of(keyClass, valueClass).name(name).entries(entries).averageKey(averageKey)
                .averageValue(averageValue).maxBloatFactor(maxBloatFactor).createPersistedTo(file);
    }

    /**
     * Run this on app startup to check and fix if there were any abnormal
     * terminations
     * 
     * @param filePath   the path to the file to with the data
     * @param keyClass   the class of the key
     * @param valueClass the class of the value (best to implement Value interface
     *                   for complex structures)
     */
    public <K, V> ChronicleMap<K, V> recoverDb(final String name, final long entries,
            final K averageKey, final V averageValue, final String filePath, final double maxBloatFactor)
            throws IOException {
        Logger.info("Restoring ChronicleMap {} at: {}", name, filePath);
        final File file = new File(filePath);
        final Class<K> keyClass = (Class<K>) averageKey.getClass();
        final Class<V> valueClass = (Class<V>) averageValue.getClass();

        return ChronicleMap.of(keyClass, valueClass).name(name).entries(entries).averageKey(averageKey)
                .averageValue(averageValue).maxBloatFactor(maxBloatFactor).recoverPersistedTo(file, true);
    }

    /**
     * Get the object constructor reflectively to be used when inserting/updating
     * records
     * 
     * @throws ClassNotFoundException
     * @return Constructor<?>
     */

    public Constructor<?> getObjectConstructor(final String objectClassName) throws ClassNotFoundException {
        final var objClass = Class.forName(objectClassName);

        final Constructor<?>[] constructors = objClass.getDeclaredConstructors();
        Constructor<?> c = null;
        for (final Constructor<?> con : constructors) {
            if (con.getParameterCount() > 0) {
                c = con;
            }
        }

        return c;
    }

    /**
     * Constructs the class using reflection
     * 
     * @throws ClassNotFoundException
     * @throws InvocationTargetException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public Object constructObject(final String objectClassName, final Object[] values) throws ClassNotFoundException,
            InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        final var con = getObjectConstructor(objectClassName);
        final var params = con.getParameterTypes();
        final Object[] preparedValues = new Object[params.length];

        if (params.length != values.length) {
            Logger.error("Length of parameters supplied does not match.");
            return null;
        }

        for (int i = 0; i < params.length; i++) {
            preparedValues[i] = params[i].isEnum() ? CHRONICLE_UTILS.toEnum(params[i], values[i]) : values[i];
        }

        return con.newInstance(preparedValues);
    }

    /**
     * Gets the Chronicle dao object to run different methods such as CRUD
     * reflectively
     * 
     * @param daoClassName       the full package class name for the dao
     * @param daoClassObjectName the static object name
     * 
     * @return ChronicleDao
     * @throws ClassNotFoundException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws InstantiationException
     */
    public ChronicleDao getChronicleDao(final String daoClassName, final String dataPath) throws ClassNotFoundException,
            InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        final var c = getObjectConstructor(daoClassName);
        return (ChronicleDao) c.newInstance(dataPath);
    }

    public <K, V> Map<K, V> getMapForMultiInserts(final ChronicleDao<K, V> dao) {
        return new HashMap<K, V>();
    }
}
