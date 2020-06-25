package io.afloatdb.client.kv;

import javax.annotation.Nonnull;

public interface Put {

    <T> T put(@Nonnull String key, @Nonnull byte[] value);

    @Nonnull
    <T> Ordered<T> putOrdered(@Nonnull String key, @Nonnull byte[] value);

    <T> T put(@Nonnull String key, int value);

    @Nonnull
    <T> Ordered<T> putOrdered(@Nonnull String key, int value);

    <T> T put(@Nonnull String key, long value);

    @Nonnull
    <T> Ordered<T> putOrdered(@Nonnull String key, long value);

    <T> T put(@Nonnull String key, @Nonnull String value);

    @Nonnull
    <T> Ordered<T> putOrdered(@Nonnull String key, @Nonnull String value);

    <T> T putIfAbsent(@Nonnull String key, @Nonnull byte[] value);

    @Nonnull
    <T> Ordered<T> putIfAbsentOrdered(@Nonnull String key, @Nonnull byte[] value);

    <T> T putIfAbsent(@Nonnull String key, int value);

    @Nonnull
    <T> Ordered<T> putIfAbsentOrdered(@Nonnull String key, int value);

    <T> T putIfAbsent(@Nonnull String key, long value);

    @Nonnull
    <T> Ordered<T> putIfAbsentOrdered(@Nonnull String key, long value);

    <T> T putIfAbsent(@Nonnull String key, @Nonnull String value);

    @Nonnull
    <T> Ordered<T> putIfAbsentOrdered(@Nonnull String key, @Nonnull String value);

}
