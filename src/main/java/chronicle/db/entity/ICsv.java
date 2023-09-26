package chronicle.db.entity;

public interface ICsv<K> {
    String[] header();

    Object[] row(final K key);
}
