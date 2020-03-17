package io.afloatdb.client.internal.channel;

import io.grpc.ManagedChannel;

import javax.annotation.Nonnull;
import java.util.Collection;

public interface ChannelManager {

    @Nonnull
    ManagedChannel getOrCreateChannel(@Nonnull String address);

    void checkChannel(String address, ManagedChannel channel);

    void retainChannels(Collection<String> addresses);

}
