package io.afloatdb.client.kv;

import javax.annotation.Nonnull;

public interface Set {

    default void set(String key, @Nonnull byte[] value) {
        setOrdered(key, value);
    }

    long setOrdered(String key, @Nonnull byte[] value);

    default void set(String key, int value) {
        setOrdered(key, value);
    }

    long setOrdered(String key, int value);

    default void set(String key, long value) {
        setOrdered(key, value);
    }

    long setOrdered(String key, long value);

    default void set(String key, @Nonnull String value) {
        setOrdered(key, value);
    }

    long setOrdered(String key, @Nonnull String value);

}
