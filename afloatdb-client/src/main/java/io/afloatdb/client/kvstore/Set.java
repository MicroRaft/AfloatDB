package io.afloatdb.client.kvstore;

import javax.annotation.Nonnull;

public interface Set {

    void set(String key, boolean value);

    void set(String key, byte value);

    void set(String key, @Nonnull byte[] value);

    void set(String key, char value);

    void set(String key, double value);

    void set(String key, float value);

    void set(String key, int value);

    void set(String key, long value);

    void set(String key, short value);

    void set(String key, @Nonnull String value);

}
