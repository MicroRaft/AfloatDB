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

import io.afloatdb.internal.lifecycle.ProcessTerminationLogger;
import io.afloatdb.internal.rpc.RaftRpcStub;
import io.afloatdb.internal.rpc.RaftRpcStubManager;
import io.afloatdb.raft.proto.ProtoOperationResponse;
import io.afloatdb.raft.proto.ProtoQueryRequest;
import io.afloatdb.raft.proto.ProtoRaftMessage;
import io.afloatdb.raft.proto.ProtoRaftResponse;
import io.afloatdb.raft.proto.ProtoReplicateRequest;
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
import static io.afloatdb.internal.utils.Exceptions.runSilently;
import static io.afloatdb.internal.utils.Serialization.wrap;
import static io.afloatdb.raft.proto.RaftMessageServiceGrpc.newStub;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

@Singleton
public class RaftRpcStubManagerImpl
        implements RaftRpcStubManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftRpcStubManager.class);
    //    private static final long LEADER_HEARTBEAT_TIMEOUT_EXTENSION_MILLIS = 1000;

    private final RaftEndpoint localEndpoint;
    private final Map<RaftEndpoint, String> addresses;
    private final Map<RaftEndpoint, ChannelContext> channels = new ConcurrentHashMap<>();
    private final Set<RaftEndpoint> initializingEndpoints = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final ProcessTerminationLogger processTerminationLogger;
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();

    @Inject
    public RaftRpcStubManagerImpl(@Named(LOCAL_ENDPOINT_KEY) RaftEndpoint localEndpoint,
                                  @Named(RAFT_ENDPOINT_ADDRESSES_KEY) Map<RaftEndpoint, String> addresses,
                                  ProcessTerminationLogger processTerminationLogger) {
        this.localEndpoint = localEndpoint;
        this.addresses = new ConcurrentHashMap<>(addresses);
        this.processTerminationLogger = processTerminationLogger;
    }

    @PreDestroy
    public void shutdown() {
        channels.values().forEach(ChannelContext::shutdownSilently);
        channels.clear();
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
            LOGGER.warn("{} replaced current address: {} new address: {} for {}", localEndpoint.getId(), currentAddress, address,
                    endpoint.getId());
        }
    }

    @Override
    public Map<RaftEndpoint, String> getAddresses() {
        return new HashMap<>(addresses);
    }

    @Override
    public RaftRpcStub getRpcStub(RaftEndpoint target) {
        requireNonNull(target);
        return getOrCreateChannel(target);
    }

    private ChannelContext getOrCreateChannel(RaftEndpoint target) {
        if (localEndpoint.equals(target)) {
            LOGGER.error("{} cannot send Raft message to itself...", localEndpoint.getId());
            return null;
        }

        ChannelContext context = channels.get(target);
        if (context != null) {
            return context;
        } else if (!addresses.containsKey(target)) {
            LOGGER.error("{} unknown target: {}", localEndpoint.getId(), target);
            return null;
        }

        return connect(target);
    }

    private ChannelContext connect(RaftEndpoint target) {
        if (!initializingEndpoints.add(target)) {
            return null;
        }

        String address = addresses.get(target);

        ManagedChannel channel = ManagedChannelBuilder.forTarget(address).disableRetry().usePlaintext().directExecutor().build();

        // TODO [basri] introduce deadline
        //        RaftConfig raftConfig = config.getRaftConfig();
        //        long deadline = raftConfig.getLeaderHeartbeatTimeoutMillis() + LEADER_HEARTBEAT_TIMEOUT_EXTENSION_MILLIS;
        //        RaftMessageServiceStub invocationStub = newStub(channel).withDeadlineAfter(deadline, MILLISECONDS);
        RaftMessageServiceStub invocationStub = newStub(channel);

        ChannelContext context = new ChannelContext(target, channel, invocationStub);
        context.raftMessageSender = invocationStub.handle(new ResponseStreamObserver(context));

        channels.put(target, context);
        initializingEndpoints.remove(target);

        return context;
    }

    private void closeChannel(ChannelContext context) {
        if (channels.remove(context.targetEndpoint, context)) {
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
                }, 5, SECONDS);
            } catch (RejectedExecutionException e) {
                LOGGER.warn("{} could not schedule task for channel creation to: {}.", localEndpoint.getId(), target.getId());
                initializingEndpoints.remove(target);
            }
        }
    }

    private class ChannelContext
            implements RaftRpcStub {
        final RaftEndpoint targetEndpoint;
        final ManagedChannel channel;
        final RaftMessageServiceStub invocationStub;
        StreamObserver<ProtoRaftMessage> raftMessageSender;

        ChannelContext(RaftEndpoint targetEndpoint, ManagedChannel channel, RaftMessageServiceStub invocationStub) {
            this.targetEndpoint = targetEndpoint;
            this.channel = channel;
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
                    LOGGER.error(localEndpoint.getId() + " failure during sending " + message.getClass().getSimpleName() + " to "
                            + targetEndpoint, t);
                } else {
                    LOGGER.error("{} failure during sending {} to {}. Exception: {} Message: {}", localEndpoint.getId(),
                            message.getClass().getSimpleName(), targetEndpoint, t.getClass().getSimpleName(), t.getMessage());
                }
            }
        }

        @Override
        public void replicate(ProtoReplicateRequest request, StreamObserver<ProtoOperationResponse> responseObserver) {
            invocationStub.replicate(request, responseObserver);
        }

        @Override
        public void query(ProtoQueryRequest request, StreamObserver<ProtoOperationResponse> responseObserver) {
            invocationStub.query(request, responseObserver);
        }
    }

    private class ResponseStreamObserver
            implements StreamObserver<ProtoRaftResponse> {
        final ChannelContext context;

        private ResponseStreamObserver(ChannelContext context) {
            this.context = context;
        }

        @Override
        public void onNext(ProtoRaftResponse response) {
            LOGGER.warn("{} received {} from Raft RPC stream to {}", localEndpoint.getId(), response,
                    context.targetEndpoint.getId());
        }

        @Override
        public void onError(Throwable t) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.error(localEndpoint.getId() + " streaming Raft RPC to " + context.targetEndpoint.getId() + " has failed.",
                        t);
            } else {
                LOGGER.error("{} Raft RPC stream to {} has failed. Exception: {} Message: {}", localEndpoint.getId(),
                        context.targetEndpoint.getId(), t.getClass().getSimpleName(), t.getMessage());
            }

            closeChannel(context);
        }

        @Override
        public void onCompleted() {
            LOGGER.warn("{} Raft RPC stream to {} has completed.", localEndpoint.getId(), context.targetEndpoint.getId());
        }

    }

}
