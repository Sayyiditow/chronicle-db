package chronicle.db.entity;

public interface ICsv<K> {
    String[] headers();

    Object[] row(final K key);
}
