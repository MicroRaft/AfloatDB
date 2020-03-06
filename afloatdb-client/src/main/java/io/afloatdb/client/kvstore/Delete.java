package io.afloatdb.client.kvstore;

import javax.annotation.Nonnull;

public interface Delete {

    boolean delete(@Nonnull String key);

}
