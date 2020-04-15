package io.afloatdb.client.kvstore;

import javax.annotation.Nonnull;

public interface Remove {

    <T> T remove(@Nonnull String key);

    boolean remove(@Nonnull String key, byte[] value);

    boolean remove(@Nonnull String key, int value);

    boolean remove(@Nonnull String key, long value);

    boolean remove(@Nonnull String key, @Nonnull String value);

}
