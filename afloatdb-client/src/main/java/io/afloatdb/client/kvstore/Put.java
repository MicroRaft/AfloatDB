package io.afloatdb.client.kvstore;

import javax.annotation.Nonnull;

public interface Put {

    <T> T put(@Nonnull String key, boolean value);

    <T> T put(@Nonnull String key, byte value);

    <T> T put(@Nonnull String key, @Nonnull byte[] value);

    <T> T put(@Nonnull String key, char value);

    <T> T put(@Nonnull String key, double value);

    <T> T put(@Nonnull String key, float value);

    <T> T put(@Nonnull String key, int value);

    <T> T put(@Nonnull String key, long value);

    <T> T put(@Nonnull String key, short value);

    <T> T put(@Nonnull String key, @Nonnull String value);

    <T> T putIfAbsent(@Nonnull String key, boolean value);

    <T> T putIfAbsent(@Nonnull String key, byte value);

    <T> T putIfAbsent(@Nonnull String key, @Nonnull byte[] value);

    <T> T putIfAbsent(@Nonnull String key, char value);

    <T> T putIfAbsent(@Nonnull String key, double value);

    <T> T putIfAbsent(@Nonnull String key, float value);

    <T> T putIfAbsent(@Nonnull String key, int value);

    <T> T putIfAbsent(@Nonnull String key, long value);

    <T> T putIfAbsent(@Nonnull String key, short value);

    <T> T putIfAbsent(@Nonnull String key, @Nonnull String value);

}
