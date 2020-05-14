package io.afloatdb.client.internal.rpc.impl;

import io.afloatdb.client.config.AfloatDBClientConfig;
import io.afloatdb.client.internal.channel.ChannelManager;
import io.afloatdb.kv.proto.KVServiceGrpc;
import io.afloatdb.kv.proto.KVServiceGrpc.KVServiceBlockingStub;
import io.afloatdb.kv.proto.SizeRequest;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.afloatdb.client.internal.di.AfloatDBClientModule.CONFIG_KEY;

@Singleton
public class UniKVServiceStubManager
        implements Supplier<KVServiceBlockingStub> {

    private static final Logger LOGGER = LoggerFactory.getLogger(UniKVServiceStubManager.class);

    private final KVServiceBlockingStub stub;

    @Inject
    public UniKVServiceStubManager(@Named(CONFIG_KEY) AfloatDBClientConfig config, ChannelManager channelManager) {
        this.stub = KVServiceGrpc.newBlockingStub(channelManager.getOrCreateChannel(config.getServerAddress()));
    }

    @Override
    public KVServiceBlockingStub get() {
        return stub;
    }

    @PostConstruct
    public void start() {
        // TODO [basri] make it configurable
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(60);
        while (System.currentTimeMillis() < deadline) {
            try {
                stub.size(SizeRequest.getDefaultInstance());
                return;
            } catch (StatusRuntimeException e1) {
                LOGGER.error("Could not get response from server. STATUS: {}", e1.getStatus());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e2) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

}
