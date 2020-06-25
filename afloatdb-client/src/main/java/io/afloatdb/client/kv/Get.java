package io.afloatdb.client.kv;

import javax.annotation.Nonnull;

public interface Get {

    default <T> T get(@Nonnull String key) {
        return get(key, 0L);
    }

    <T> T get(@Nonnull String key, long minCommitIndex);

    default @Nonnull
    <T> Ordered<T> getOrdered(@Nonnull String key) {
        return getOrdered(key, 0L);
    }

    @Nonnull
    <T> Ordered<T> getOrdered(@Nonnull String key, long minCommitIndex);

    default @Nonnull
    <T> Ordered<T> getOrDefaultOrdered(@Nonnull String key, T defaultValue) {
        return getOrDefaultOrdered(key, defaultValue, 0L);
    }

    @Nonnull
    <T> Ordered<T> getOrDefaultOrdered(@Nonnull String key, T defaultValue, long minCommitIndex);

    default byte[] getOrDefault(@Nonnull String key, byte[] defaultValue) {
        return getOrDefault(key, defaultValue, 0L);
    }

    default byte[] getOrDefault(@Nonnull String key, byte[] defaultValue, long minCommitIndex) {
        byte[] val = get(key, minCommitIndex);
        return val != null ? val : defaultValue;
    }

    default int getOrDefault(@Nonnull String key, int defaultValue) {
        return getOrDefault(key, defaultValue, 0L);
    }

    default int getOrDefault(@Nonnull String key, int defaultValue, long minCommitIndex) {
        Integer val = get(key, minCommitIndex);
        return val != null ? val : defaultValue;
    }

    default long getOrDefault(@Nonnull String key, long defaultValue) {
        return getOrDefault(key, defaultValue, 0L);
    }

    default long getOrDefault(@Nonnull String key, long defaultValue, long minCommitIndex) {
        Long val = get(key, minCommitIndex);
        return val != null ? val : defaultValue;
    }

    default String getOrDefault(@Nonnull String key, String defaultValue) {
        return getOrDefault(key, defaultValue, 0L);
    }

    default String getOrDefault(@Nonnull String key, String defaultValue, long minCommitIndex) {
        String val = get(key, minCommitIndex);
        return val != null ? val : defaultValue;
    }

}
