package chronicle.db.utils;

@FunctionalInterface
public interface CheckedSupplier<T> {
    T get() throws Throwable;
}
