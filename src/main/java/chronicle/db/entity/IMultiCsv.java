package chronicle.db.entity;

public interface IMultiCsv<K> {
    String[] header();

    Object[] row(final String fileName, final K key);
}
