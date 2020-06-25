package io.afloatdb.client.kv;

import javax.annotation.Nonnull;

public interface Replace {

    boolean replace(@Nonnull String key, @Nonnull Object oldValue, @Nonnull Object newValue);

    @Nonnull
    Ordered<Boolean> replaceOrdered(@Nonnull String key, @Nonnull Object oldValue, @Nonnull Object newValue);

}
