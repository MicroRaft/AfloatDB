package io.afloatdb.client.kv;

import javax.annotation.Nonnull;

public interface Remove {

    <T> T remove(@Nonnull String key);

    @Nonnull
    <T> Ordered<T> removeOrdered(@Nonnull String key);

    boolean remove(@Nonnull String key, byte[] value);

    @Nonnull
    Ordered<Boolean> removeOrdered(@Nonnull String key, byte[] value);

    boolean remove(@Nonnull String key, int value);

    @Nonnull
    Ordered<Boolean> removeOrdered(@Nonnull String key, int value);

    boolean remove(@Nonnull String key, long value);

    @Nonnull
    Ordered<Boolean> removeOrdered(@Nonnull String key, long value);

    boolean remove(@Nonnull String key, @Nonnull String value);

    @Nonnull
    Ordered<Boolean> removeOrdered(@Nonnull String key, @Nonnull String value);

}
