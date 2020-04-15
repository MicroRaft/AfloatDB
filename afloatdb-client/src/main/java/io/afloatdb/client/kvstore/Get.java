package io.afloatdb.client.kvstore;

import javax.annotation.Nonnull;

public interface Get {

    <T> T get(String key);

    default byte[] getOrDefault(@Nonnull String key, byte[] defaultValue) {
        byte[] val = get(key);
        return val != null ? val : defaultValue;
    }

    default int getOrDefault(@Nonnull String key, int defaultValue) {
        Integer val = get(key);
        return val != null ? val : defaultValue;
    }

    default long getOrDefault(@Nonnull String key, long defaultValue) {
        Long val = get(key);
        return val != null ? val : defaultValue;
    }

    default String getOrDefault(@Nonnull String key, String defaultValue) {
        String val = get(key);
        return val != null ? val : defaultValue;
    }

}
