/*
 * Copyright (c) 2020, AfloatDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.afloatdb.internal.rpc.impl;

import io.afloatdb.config.AfloatDBConfig;
import io.afloatdb.internal.di.AfloatDBModule;
import io.afloatdb.internal.lifecycle.ProcessTerminationLogger;
import io.afloatdb.internal.rpc.RaftMessageDispatcher;
import io.afloatdb.raft.proto.ProtoRaftMessage;
import io.afloatdb.raft.proto.ProtoRaftResponse;
import io.afloatdb.raft.proto.RaftMessageServiceGrpc.RaftMessageServiceStub;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.microraft.RaftEndpoint;
import io.microraft.model.message.RaftMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import static io.afloatdb.internal.di.AfloatDBModule.LOCAL_ENDPOINT_KEY;
import static io.afloatdb.internal.di.AfloatDBModule.RAFT_ENDPOINT_ADDRESSES_KEY;
import static io.afloatdb.internal.di.AfloatDBModule.RAFT_NODE_EXECUTOR_KEY;
import static io.afloatdb.internal.utils.Exceptions.runSilently;
import static io.afloatdb.internal.utils.Serialization.wrap;
import static io.afloatdb.raft.proto.RaftMessageServiceGrpc.newStub;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@Singleton
public class RaftMessageDispatcherImpl
        implements RaftMessageDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftMessageDispatcher.class);
    private static final long LEADER_HEARTBEAT_TIMEOUT_EXTENSION_MILLIS = 1000;

    private final RaftEndpoint localEndpoint;
    private final Map<RaftEndpoint, String> addresses;
    private final AfloatDBConfig config;
    private final ScheduledExecutorService executor;
    private final Map<RaftEndpoint, ClientContext> clients = new ConcurrentHashMap<>();
    private final Set<RaftEndpoint> initializingEndpoints = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final ProcessTerminationLogger processTerminationLogger;

    @Inject
    public RaftMessageDispatcherImpl(@Named(LOCAL_ENDPOINT_KEY) RaftEndpoint localEndpoint,
                                     @Named(RAFT_ENDPOINT_ADDRESSES_KEY) Map<RaftEndpoint, String> addresses,
                                     @Named(AfloatDBModule.CONFIG_KEY) AfloatDBConfig config,
                                     @Named(RAFT_NODE_EXECUTOR_KEY) ScheduledExecutorService executor,
                                     ProcessTerminationLogger processTerminationLogger) {
        this.localEndpoint = localEndpoint;
        this.addresses = new ConcurrentHashMap<>(addresses);
        this.config = config;
        this.executor = executor;
        this.processTerminationLogger = processTerminationLogger;
    }

    @PreDestroy
    public void shutdown() {
        clients.values().forEach(ClientContext::shutdown);
        clients.clear();

        processTerminationLogger.logInfo(LOGGER, localEndpoint.getId() + " RaftMessageDispatcher is shut down.");
    }

    public void send(@Nonnull RaftEndpoint target, @Nonnull RaftMessage message) {
        requireNonNull(target);
        requireNonNull(message);

        ClientContext context = getOrCreateChannel(target);
        if (context == null) {
            LOGGER.debug("{} has no connection to {} for sending {}.", localEndpoint.getId(), target, message);
            return;
        }

        try {
            context.observer.onNext(wrap(message));
        } catch (Throwable t) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.error(
                        localEndpoint.getId() + " failure during sending " + message.getClass().getSimpleName() + " to " + target,
                        t);
            } else {
                LOGGER.error("{} failure during sending {} to {}. Exception: {} Message: {}", localEndpoint.getId(),
                        message.getClass().getSimpleName(), target, t.getClass().getSimpleName(), t.getMessage());
            }
        }
    }

    @Override
    public void add(@Nonnull RaftEndpoint endpoint, @Nonnull String address) {
        requireNonNull(endpoint);
        requireNonNull(address);

        String currentAddress = addresses.put(endpoint, address);
        if (currentAddress == null) {
            LOGGER.info("{} added address: {} for {}", localEndpoint.getId(), address, endpoint.getId());
        } else if (!currentAddress.equals(address)) {
            LOGGER.warn("{} replaced current address: {} new address: {} for {}", localEndpoint.getId(), currentAddress, address,
                    endpoint.getId());
        }
    }

    @Override
    public Map<RaftEndpoint, String> getAddresses() {
        return new HashMap<>(addresses);
    }

    @Override
    public RaftMessageServiceStub getStub(RaftEndpoint target) {
        requireNonNull(target);
        ClientContext context = getOrCreateChannel(target);
        return context != null ? context.stub : null;
    }

    private ClientContext getOrCreateChannel(RaftEndpoint target) {
        if (localEndpoint.equals(target)) {
            LOGGER.error("{} cannot send Raft message to itself...", localEndpoint.getId());
            return null;
        }

        ClientContext context = clients.get(target);
        if (context != null) {
            return context;
        } else if (!addresses.containsKey(target)) {
            LOGGER.error("{} unknown target: {}", localEndpoint.getId(), target);
            return null;
        }

        return connect(target);
    }

    private ClientContext connect(RaftEndpoint target) {
        if (!initializingEndpoints.add(target)) {
            return null;
        }

        String address = addresses.get(target);

        ManagedChannel channel = ManagedChannelBuilder.forTarget(address).disableRetry().keepAliveWithoutCalls(true)
                                                      .keepAliveTime(60, SECONDS).usePlaintext().directExecutor().build();

        // TODO [basri] introduce deadline
        //        RaftConfig raftConfig = config.getRaftConfig();
        //        long deadline = raftConfig.getLeaderHeartbeatTimeoutMillis() + LEADER_HEARTBEAT_TIMEOUT_EXTENSION_MILLIS;
        //        RaftMessageServiceStub stub = newStub(channel).withDeadlineAfter(deadline, MILLISECONDS);

        RaftMessageServiceStub stub = newStub(channel);
        ClientContext context = new ClientContext(target, channel, stub);
        ResponseStreamObserver responseObserver = new ResponseStreamObserver(context);

        context.observer = stub.handle(responseObserver);
        clients.put(target, context);
        initializingEndpoints.remove(target);

        return context;
    }

    private void closeContext(ClientContext context) {
        if (clients.remove(context.endpoint, context)) {
            context.shutdown();
        }

        delayChannelCreationFor(context.endpoint);
    }

    private void delayChannelCreationFor(RaftEndpoint target) {
        if (initializingEndpoints.add(target)) {
            LOGGER.debug("{} delaying channel creation to {}.", localEndpoint.getId(), target.getId());
            try {
                executor.schedule(() -> {
                    initializingEndpoints.remove(target);
                }, 5, SECONDS);
            } catch (RejectedExecutionException e) {
                LOGGER.warn("{} could not schedule task for channel creation to: {}", localEndpoint.getId(), target.getId());
                initializingEndpoints.remove(target);
            }
        }
    }

    private static class ClientContext {
        final RaftEndpoint endpoint;
        final ManagedChannel channel;
        final RaftMessageServiceStub stub;
        StreamObserver<ProtoRaftMessage> observer;

        ClientContext(RaftEndpoint endpoint, ManagedChannel channel, RaftMessageServiceStub stub) {
            this.endpoint = endpoint;
            this.channel = channel;
            this.stub = stub;
        }

        void shutdown() {
            runSilently(observer::onCompleted);
            runSilently(channel::shutdown);
        }
    }

    private class ResponseStreamObserver
            implements StreamObserver<ProtoRaftResponse> {
        final ClientContext context;

        private ResponseStreamObserver(ClientContext context) {
            this.context = context;
        }

        @Override
        public void onNext(ProtoRaftResponse response) {
            LOGGER.warn("{} received {} from Raft RPC stream to {}", localEndpoint.getId(), response, context.endpoint.getId());
        }

        @Override
        public void onError(Throwable t) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.error(localEndpoint.getId() + " streaming Raft RPC to " + context.endpoint.getId() + " has failed.", t);
            } else {
                LOGGER.error("{} Raft RPC stream to {} has failed. Exception: {} Message: {}", localEndpoint.getId(),
                        context.endpoint.getId(), t.getClass().getSimpleName(), t.getMessage());
            }

            closeContext(context);
        }

        @Override
        public void onCompleted() {
            LOGGER.warn("{} Raft RPC stream to {} has completed.", localEndpoint.getId(), context.endpoint.getId());
        }

    }

}
