package chronicle.db.entity;

public interface ICsv<K> {
    Object[] row(final K key);

    String[] headers();
}
