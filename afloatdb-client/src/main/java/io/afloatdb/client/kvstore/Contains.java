package io.afloatdb.client.kvstore;

import javax.annotation.Nonnull;

public interface Contains {

    boolean contains(@Nonnull String key);

    boolean contains(@Nonnull String key, boolean value);

    boolean contains(@Nonnull String key, byte value);

    boolean contains(@Nonnull String key, @Nonnull byte[] value);

    boolean contains(@Nonnull String key, char value);

    boolean contains(@Nonnull String key, double value);

    boolean contains(@Nonnull String key, float value);

    boolean contains(@Nonnull String key, int value);

    boolean contains(@Nonnull String key, long value);

    boolean contains(@Nonnull String key, short value);

    boolean contains(@Nonnull String key, @Nonnull String value);

}
