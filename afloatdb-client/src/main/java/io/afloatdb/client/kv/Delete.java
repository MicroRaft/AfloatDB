package io.afloatdb.client.kv;

import javax.annotation.Nonnull;

public interface Delete {

    boolean delete(@Nonnull String key);

    @Nonnull
    Ordered<Boolean> deleteOrdered(@Nonnull String key);

}
