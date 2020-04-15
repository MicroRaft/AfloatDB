package io.afloatdb.client.kvstore;

import javax.annotation.Nonnull;

public interface Set {

    void set(String key, @Nonnull byte[] value);

    void set(String key, int value);

    void set(String key, long value);

    void set(String key, @Nonnull String value);

}
