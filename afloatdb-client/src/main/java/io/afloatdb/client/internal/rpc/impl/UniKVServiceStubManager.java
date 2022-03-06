package io.afloatdb.client.internal.rpc.impl;

import io.afloatdb.client.config.AfloatDBClientConfig;
import io.afloatdb.client.internal.channel.ChannelManager;
import io.afloatdb.kv.proto.KVRequestHandlerGrpc;
import io.afloatdb.kv.proto.KVRequestHandlerGrpc.KVRequestHandlerBlockingStub;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.afloatdb.client.internal.di.AfloatDBClientModule.CONFIG_KEY;

@Singleton
public class UniKVServiceStubManager implements Supplier<KVRequestHandlerBlockingStub> {

    private final int rpcTimeoutSecs;
    private final KVRequestHandlerBlockingStub stub;

    @Inject
    public UniKVServiceStubManager(@Named(CONFIG_KEY) AfloatDBClientConfig config, ChannelManager channelManager) {
        this.rpcTimeoutSecs = config.getRpcTimeoutSecs();
        this.stub = KVRequestHandlerGrpc.newBlockingStub(channelManager.getOrCreateChannel(config.getServerAddress()));
    }

    @Override
    public KVRequestHandlerBlockingStub get() {
        return stub.withDeadlineAfter(rpcTimeoutSecs, TimeUnit.SECONDS);
    }

}
