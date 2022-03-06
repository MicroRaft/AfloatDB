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

import com.google.common.util.concurrent.ListenableFuture;
import io.afloatdb.config.AfloatDBConfig;
import io.afloatdb.internal.lifecycle.ProcessTerminationLogger;
import io.afloatdb.internal.rpc.RaftRpc;
import io.afloatdb.internal.rpc.RaftRpcService;
import io.afloatdb.kv.proto.KVResponse;
import io.afloatdb.raft.proto.QueryRequest;
import io.afloatdb.raft.proto.RaftInvocationHandlerGrpc;
import io.afloatdb.raft.proto.RaftInvocationHandlerGrpc.RaftInvocationHandlerFutureStub;
import io.afloatdb.raft.proto.RaftMessageHandlerGrpc;
import io.afloatdb.raft.proto.RaftMessageHandlerGrpc.RaftMessageHandlerStub;
import io.afloatdb.raft.proto.RaftMessageRequest;
import io.afloatdb.raft.proto.RaftMessageResponse;
import io.afloatdb.raft.proto.ReplicateRequest;
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

import static io.afloatdb.internal.di.AfloatDBModule.CONFIG_KEY;
import static io.afloatdb.internal.di.AfloatDBModule.LOCAL_ENDPOINT_KEY;
import static io.afloatdb.internal.di.AfloatDBModule.RAFT_ENDPOINT_ADDRESSES_KEY;
import static io.afloatdb.internal.utils.Exceptions.runSilently;
import static io.afloatdb.internal.utils.Serialization.wrap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

@Singleton
public class RaftRpcServiceImpl implements RaftRpcService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftRpcService.class);

    private final RaftEndpoint localEndpoint;
    private final Map<RaftEndpoint, String> addresses;
    private final Map<RaftEndpoint, RaftRpcContext> stubs = new ConcurrentHashMap<>();
    private final Set<RaftEndpoint> initializingEndpoints = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final ProcessTerminationLogger processTerminationLogger;
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
    private final long rpcTimeoutSecs;

    @Inject
    public RaftRpcServiceImpl(@Named(LOCAL_ENDPOINT_KEY) RaftEndpoint localEndpoint,
            @Named(CONFIG_KEY) AfloatDBConfig config,
            @Named(RAFT_ENDPOINT_ADDRESSES_KEY) Map<RaftEndpoint, String> addresses,
            ProcessTerminationLogger processTerminationLogger) {
        this.localEndpoint = localEndpoint;
        this.addresses = new ConcurrentHashMap<>(addresses);
        this.processTerminationLogger = processTerminationLogger;
        this.rpcTimeoutSecs = config.getRpcConfig().getRpcTimeoutSecs();
    }

    @PreDestroy
    public void shutdown() {
        stubs.values().forEach(RaftRpcContext::shutdownSilently);
        stubs.clear();
        executor.shutdownNow();

        processTerminationLogger.logInfo(LOGGER, localEndpoint.getId() + " RaftMessageDispatcher is shut down.");
    }

    @Override
    public void addAddress(@Nonnull RaftEndpoint endpoint, @Nonnull String address) {
        requireNonNull(endpoint);
        requireNonNull(address);

        String currentAddress = addresses.put(endpoint, address);
        if (currentAddress == null) {
            LOGGER.info("{} added address: {} for {}", localEndpoint.getId(), address, endpoint.getId());
        } else if (!currentAddress.equals(address)) {
            LOGGER.warn("{} replaced current address: {} with new address: {} for {}", localEndpoint.getId(),
                    currentAddress, address, endpoint.getId());
        }
    }

    @Override
    public Map<RaftEndpoint, String> getAddresses() {
        return new HashMap<>(addresses);
    }

    @Override
    public RaftRpc getRpcStub(RaftEndpoint target) {
        return getOrCreateStub(requireNonNull(target));
    }

    @Override
    public void send(@Nonnull RaftEndpoint target, @Nonnull RaftMessage message) {
        RaftRpc stub = getRpcStub(target);
        if (stub != null) {
            executor.submit(() -> {
                stub.send(message);
            });
        }
    }

    @Override
    public boolean isReachable(@Nonnull RaftEndpoint endpoint) {
        return stubs.containsKey(endpoint);
    }

    private RaftRpcContext getOrCreateStub(RaftEndpoint target) {
        if (localEndpoint.equals(target)) {
            LOGGER.error("{} cannot send Raft message to itself...", localEndpoint.getId());
            return null;
        }

        RaftRpcContext context = stubs.get(target);
        if (context != null) {
            return context;
        } else if (!addresses.containsKey(target)) {
            LOGGER.error("{} unknown target: {}", localEndpoint.getId(), target);
            return null;
        }

        return connect(target);
    }

    private RaftRpcContext connect(RaftEndpoint target) {
        if (!initializingEndpoints.add(target)) {
            return null;
        }

        try {
            String address = addresses.get(target);
            ManagedChannel channel = ManagedChannelBuilder.forTarget(address).disableRetry().usePlaintext()
                    // .directExecutor()
                    .build();

            RaftMessageHandlerStub replicationStub = RaftMessageHandlerGrpc.newStub(channel);
            RaftInvocationHandlerFutureStub invocationStub = RaftInvocationHandlerGrpc.newFutureStub(channel);
            RaftRpcContext context = new RaftRpcContext(target, channel, replicationStub, invocationStub);
            context.raftMessageSender = replicationStub.handle(new ResponseStreamObserver(context));

            stubs.put(target, context);

            return context;
        } finally {
            initializingEndpoints.remove(target);
        }
    }

    private void checkChannel(RaftRpcContext context) {
        if (stubs.remove(context.targetEndpoint, context)) {
            context.shutdownSilently();
        }

        delayChannelCreation(context.targetEndpoint);
    }

    private void delayChannelCreation(RaftEndpoint target) {
        if (initializingEndpoints.add(target)) {
            LOGGER.debug("{} delaying channel creation to {}.", localEndpoint.getId(), target.getId());
            try {
                executor.schedule(() -> {
                    initializingEndpoints.remove(target);
                }, 1, SECONDS);
            } catch (RejectedExecutionException e) {
                LOGGER.warn("{} could not schedule task for channel creation to: {}.", localEndpoint.getId(),
                        target.getId());
                initializingEndpoints.remove(target);
            }
        }
    }

    private class RaftRpcContext implements RaftRpc {
        final RaftEndpoint targetEndpoint;
        final ManagedChannel channel;
        final RaftMessageHandlerStub replicationStub;
        final RaftInvocationHandlerFutureStub invocationStub;
        StreamObserver<RaftMessageRequest> raftMessageSender;

        RaftRpcContext(RaftEndpoint targetEndpoint, ManagedChannel channel, RaftMessageHandlerStub replicationStub,
                RaftInvocationHandlerFutureStub invocationStub) {
            this.targetEndpoint = targetEndpoint;
            this.channel = channel;
            this.replicationStub = replicationStub;
            this.invocationStub = invocationStub;
        }

        void shutdownSilently() {
            runSilently(raftMessageSender::onCompleted);
            runSilently(channel::shutdown);
        }

        @Override
        public void send(@Nonnull RaftMessage message) {
            try {
                raftMessageSender.onNext(wrap(message));
            } catch (Throwable t) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.error(localEndpoint.getId() + " failure during sending " + message.getClass().getSimpleName()
                            + " to " + targetEndpoint, t);
                } else {
                    LOGGER.error("{} failure during sending {} to {}. Exception: {} Message: {}", localEndpoint.getId(),
                            message.getClass().getSimpleName(), targetEndpoint, t.getClass().getSimpleName(),
                            t.getMessage());
                }
            }
        }

        @Override
        public ListenableFuture<KVResponse> replicate(ReplicateRequest request) {
            return invocationStub.withDeadlineAfter(rpcTimeoutSecs, SECONDS).replicate(request);
        }

        @Override
        public ListenableFuture<KVResponse> query(QueryRequest request) {
            return invocationStub.withDeadlineAfter(rpcTimeoutSecs, SECONDS).query(request);
        }
    }

    private class ResponseStreamObserver implements StreamObserver<RaftMessageResponse> {
        final RaftRpcContext context;

        private ResponseStreamObserver(RaftRpcContext context) {
            this.context = context;
        }

        @Override
        public void onNext(RaftMessageResponse response) {
            LOGGER.warn("{} received {} from Raft RPC stream to {}", localEndpoint.getId(), response,
                    context.targetEndpoint.getId());
        }

        @Override
        public void onError(Throwable t) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.error(localEndpoint.getId() + " streaming Raft RPC to " + context.targetEndpoint.getId()
                        + " has failed.", t);
            } else {
                LOGGER.error("{} Raft RPC stream to {} has failed. Exception: {} Message: {}", localEndpoint.getId(),
                        context.targetEndpoint.getId(), t.getClass().getSimpleName(), t.getMessage());
            }

            checkChannel(context);
        }

        @Override
        public void onCompleted() {
            LOGGER.warn("{} Raft RPC stream to {} has completed.", localEndpoint.getId(),
                    context.targetEndpoint.getId());
        }

    }

}
