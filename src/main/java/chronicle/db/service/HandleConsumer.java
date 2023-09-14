package chronicle.db.service;

import java.util.function.Consumer;

@FunctionalInterface
public interface HandleConsumer<Target, ExceptionObject extends Exception> {
    void accept(Target target) throws ExceptionObject;

    static <Target> Consumer<Target> handleConsumerBuilder(
            final HandleConsumer<Target, Exception> handleConsumer) {
        return obj -> {
            try {
                handleConsumer.accept(obj);
            } catch (final Exception ex) {
                throw new RuntimeException(ex);
            }
        };
    }
}
