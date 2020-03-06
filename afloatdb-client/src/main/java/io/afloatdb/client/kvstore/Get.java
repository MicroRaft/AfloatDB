package io.afloatdb.client.kvstore;

import javax.annotation.Nonnull;

public interface Get {

    default boolean getOrDefault(@Nonnull String key, boolean defaultValue) {
        Boolean val = get(key);
        return val != null ? val : defaultValue;
    }

    <T> T get(String key);

    default byte getOrDefault(@Nonnull String key, byte defaultValue) {
        Byte val = get(key);
        return val != null ? val : defaultValue;
    }

    default byte[] getOrDefault(@Nonnull String key, byte[] defaultValue) {
        byte[] val = get(key);
        return val != null ? val : defaultValue;
    }

    default char getOrDefault(@Nonnull String key, char defaultValue) {
        Character val = get(key);
        return val != null ? val : defaultValue;
    }

    default double getOrDefault(@Nonnull String key, double defaultValue) {
        Double val = get(key);
        return val != null ? val : defaultValue;
    }

    default float getOrDefault(@Nonnull String key, float defaultValue) {
        Float val = get(key);
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

    default short getOrDefault(@Nonnull String key, short defaultValue) {
        Short val = get(key);
        return val != null ? val : defaultValue;
    }

    default String getOrDefault(@Nonnull String key, String defaultValue) {
        String val = get(key);
        return val != null ? val : defaultValue;
    }

}
