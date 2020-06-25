package io.afloatdb.client.kv;

import javax.annotation.Nonnull;

public interface Contains {

    default boolean containsKey(@Nonnull String key) {
        return containsKey(key, 0L);
    }

    boolean containsKey(@Nonnull String key, long minCommitIndex);

    default @Nonnull
    Ordered<Boolean> containsKeyOrdered(@Nonnull String key) {
        return containsKeyOrdered(key, 0L);
    }

    @Nonnull
    Ordered<Boolean> containsKeyOrdered(@Nonnull String key, long minCommitIndex);

    default boolean contains(@Nonnull String key, @Nonnull byte[] value) {
        return contains(key, value, 0L);
    }

    boolean contains(@Nonnull String key, @Nonnull byte[] value, long minCommitIndex);

    default @Nonnull
    Ordered<Boolean> containsOrdered(@Nonnull String key, @Nonnull byte[] value) {
        return containsOrdered(key, value, 0L);
    }

    @Nonnull
    Ordered<Boolean> containsOrdered(@Nonnull String key, @Nonnull byte[] value, long minCommitIndex);

    default boolean contains(@Nonnull String key, int value) {
        return contains(key, value, 0L);
    }

    boolean contains(@Nonnull String key, int value, long minCommitIndex);

    default @Nonnull
    Ordered<Boolean> containsOrdered(@Nonnull String key, int value) {
        return containsOrdered(key, value, 0L);
    }

    @Nonnull
    Ordered<Boolean> containsOrdered(@Nonnull String key, int value, long minCommitIndex);

    default boolean contains(@Nonnull String key, long value) {
        return contains(key, value, 0L);
    }

    boolean contains(@Nonnull String key, long value, long minCommitIndex);

    default @Nonnull
    Ordered<Boolean> containsOrdered(@Nonnull String key, long value) {
        return containsOrdered(key, value, 0L);
    }

    @Nonnull
    Ordered<Boolean> containsOrdered(@Nonnull String key, long value, long minCommitIndex);

    default boolean contains(@Nonnull String key, @Nonnull String value) {
        return contains(key, value, 0L);
    }

    boolean contains(@Nonnull String key, @Nonnull String value, long minCommitIndex);

    default @Nonnull
    Ordered<Boolean> containsOrdered(@Nonnull String key, @Nonnull String value) {
        return containsOrdered(key, value, 0L);
    }

    @Nonnull
    Ordered<Boolean> containsOrdered(@Nonnull String key, @Nonnull String value, long minCommitIndex);

}
