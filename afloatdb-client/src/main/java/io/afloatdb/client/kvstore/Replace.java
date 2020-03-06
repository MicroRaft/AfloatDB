package io.afloatdb.client.kvstore;

import javax.annotation.Nonnull;

public interface Replace {

    boolean replace(@Nonnull String key, @Nonnull Object oldValue, @Nonnull Object newValue);

}
