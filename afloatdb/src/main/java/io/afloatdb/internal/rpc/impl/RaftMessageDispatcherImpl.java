/*
 * Copyright (c) 2020, MicroRaft.
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

import io.afloatdb.internal.lifecycle.ProcessTerminationReporter;
import io.afloatdb.internal.rpc.RaftMessageDispatcher;
import io.afloatdb.raft.proto.PingRequest;
import io.afloatdb.raft.proto.PingResponse;
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
import static io.afloatdb.internal.raft.impl.RaftMessages.wrap;
import static io.afloatdb.raft.proto.RaftMessageServiceGrpc.newStub;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@Singleton
public class RaftMessageDispatcherImpl
        implements RaftMessageDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftMessageDispatcher.class);

    private final RaftEndpoint localEndpoint;
    private final Map<RaftEndpoint, String> addresses;
    private final ScheduledExecutorService executor;
    private final Map<RaftEndpoint, ClientContext> clients = new ConcurrentHashMap<>();
    private final Set<RaftEndpoint> initializingClients = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final ProcessTerminationReporter processTerminationReporter;

    @Inject
    public RaftMessageDispatcherImpl(@Named(LOCAL_ENDPOINT_KEY) RaftEndpoint localEndpoint,
                                     @Named(RAFT_ENDPOINT_ADDRESSES_KEY) Map<RaftEndpoint, String> addresses,
                                     @Named(RAFT_NODE_EXECUTOR_KEY) ScheduledExecutorService executor,
                                     ProcessTerminationReporter processTerminationReporter) {
        this.localEndpoint = localEndpoint;
        this.addresses = new ConcurrentHashMap<>(addresses);
        this.executor = executor;
        this.processTerminationReporter = processTerminationReporter;
    }

    @PreDestroy
    public void shutdown() {
        clients.values().stream().map(context -> (ManagedChannel) context.stub.getChannel()).forEach(ManagedChannel::shutdownNow);
        clients.clear();

        if (processTerminationReporter.isCurrentProcessTerminating()) {
            System.err.println(localEndpoint.getId() + " RaftMessage dispatcher is shut down.");
        } else {
            LOGGER.warn("{} RaftMessage dispatcher is shut down.", localEndpoint.getId());
        }
    }

    public void send(@Nonnull RaftEndpoint target, @Nonnull RaftMessage message) {
        requireNonNull(target);
        requireNonNull(message);

        StreamObserver<ProtoRaftMessage> observer = getOrTriggerConnect(target);
        if (observer == null) {
            LOGGER.debug("{} has no connection to {} for sending {}.", localEndpoint.getId(), target, message);
            return;
        }

        try {
            observer.onNext(wrap(message));
        } catch (Throwable t) {
            LOGGER.error(localEndpoint.getId() + " failure during sending " + message + " to " + target, t);
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

    private StreamObserver<ProtoRaftMessage> getOrTriggerConnect(RaftEndpoint target) {
        if (localEndpoint.equals(target)) {
            LOGGER.error("{} cannot send message to itself...", localEndpoint.getId());
            return null;
        }

        ClientContext context = clients.get(target);
        if (context != null) {
            return context.observer;
        }

        if (!addresses.containsKey(target)) {
            LOGGER.error("{} unknown target: {}", localEndpoint.getId(), target);
            return null;
        }

        if (initializingClients.add(target)) {
            connectAsync(target);
        }

        return null;
    }

    private void connectAsync(RaftEndpoint target) {
        try {
            String address = addresses.get(target);

            LOGGER.info("{} connecting to {} on {}.", localEndpoint.getId(), target.getId(), address);

            ManagedChannel channel = ManagedChannelBuilder.forTarget(address).disableRetry()
                                                          //                    .idleTimeout(4, HOURS)
                                                          //                    .keepAliveTimeout(10, SECONDS)
                                                          .usePlaintext().directExecutor().build();

            RaftMessageServiceStub stub = newStub(channel);
            StreamObserver<PingResponse> observer = new InitialPingResponseObserver(target, address, channel, stub);
            stub.ping(PingRequest.getDefaultInstance(), observer);
        } catch (Throwable t) {
            LOGGER.error(localEndpoint.getId() + " could not create channel to " + target.getId(), t);
            removeInitializingClientDelayed(target);
        }
    }

    private void removeInitializingClientDelayed(RaftEndpoint target) {
        executor.schedule(() -> {
            initializingClients.remove(target);
        }, 5, SECONDS);
    }

    private void checkChannelAsync(RaftEndpoint target, ManagedChannel channel) {
        if (initializingClients.add(target)) {
            try {
                executor.submit(() -> checkChannel(target, channel));
            } catch (RejectedExecutionException e) {
                LOGGER.error("Could not check connection to " + target);
            }
        }
    }

    private void checkChannel(RaftEndpoint target, ManagedChannel channel) {
        ClientContext context = clients.get(target);
        if (context == null || context.stub.getChannel() != channel) {
            LOGGER.error("{} channel to {} is already removed.", localEndpoint.getId(), target);
            removeInitializingClientDelayed(target);
            return;
        }

        if (channel.isTerminated() || channel.isShutdown()) {
            LOGGER.error("{} removing channel to {} since it is closed.", localEndpoint.getId(), target);
            clients.remove(target, context);
            removeInitializingClientDelayed(target);
            return;
        }

        HealthCheckPingResponseObserver observer = new HealthCheckPingResponseObserver(target, channel, context);
        context.stub.ping(PingRequest.getDefaultInstance(), observer);
    }

    private static class ClientContext {
        final RaftMessageServiceStub stub;
        final StreamObserver<ProtoRaftMessage> observer;

        ClientContext(RaftMessageServiceStub stub, StreamObserver<ProtoRaftMessage> observer) {
            this.stub = stub;
            this.observer = observer;
        }

    }

    private class ResponseStreamObserver
            implements StreamObserver<ProtoRaftResponse> {
        final RaftEndpoint target;
        final ManagedChannel channel;

        private ResponseStreamObserver(RaftEndpoint target, ManagedChannel channel) {
            this.target = target;
            this.channel = channel;
        }

        @Override
        public void onNext(ProtoRaftResponse response) {
            LOGGER.warn("{} received {} for streaming RaftMessage rpc to {}", localEndpoint.getId(), response, target.getId());
            checkChannelAsync(target, channel);
        }

        @Override
        public void onError(Throwable t) {
            LOGGER.error(localEndpoint.getId() + " streaming RaftMessage rpc to " + target.getId() + " has " + "failed.", t);
            checkChannelAsync(target, channel);
        }

        @Override
        public void onCompleted() {
            LOGGER.warn("{} streaming RaftMessage rpc to {} has completed.", localEndpoint.getId(), target.getId());
            checkChannelAsync(target, channel);
        }

    }

    private class InitialPingResponseObserver
            implements StreamObserver<PingResponse> {
        final RaftEndpoint target;
        final String address;
        final ManagedChannel channel;
        final RaftMessageServiceStub stub;

        InitialPingResponseObserver(RaftEndpoint target, String address, ManagedChannel channel, RaftMessageServiceStub stub) {
            this.target = target;
            this.address = address;
            this.channel = channel;
            this.stub = stub;
        }

        @Override
        public void onNext(PingResponse response) {
            StreamObserver<ProtoRaftResponse> observer = new ResponseStreamObserver(target, channel);
            clients.put(target, new ClientContext(stub, stub.handle(observer)));
            initializingClients.remove(target);
            LOGGER.info(localEndpoint.getId() + " connected to " + address + " for " + target.getId());
        }

        @Override
        public void onError(Throwable t) {
            LOGGER.error(localEndpoint.getId() + " could not connect to " + target.getId(), t);
            channel.shutdownNow();
            removeInitializingClientDelayed(target);
        }

        @Override
        public void onCompleted() {
        }

    }

    private class HealthCheckPingResponseObserver
            implements StreamObserver<PingResponse> {
        final RaftEndpoint target;
        final ManagedChannel channel;
        final ClientContext context;

        HealthCheckPingResponseObserver(RaftEndpoint target, ManagedChannel channel, ClientContext context) {
            this.target = target;
            this.channel = channel;
            this.context = context;
        }

        @Override
        public void onNext(PingResponse response) {
            RaftMessageServiceStub stub = context.stub;
            StreamObserver<ProtoRaftMessage> observer = stub.handle(new ResponseStreamObserver(target, channel));
            if (!clients.replace(target, context, new ClientContext(stub, observer))) {
                LOGGER.error("{} could not set new client context for {}", localEndpoint.getId(), target);
            }
            initializingClients.remove(target);
        }

        @Override
        public void onError(Throwable t) {
            LOGGER.error("{} could not ping {}", localEndpoint.getId(), target.getId());
            channel.shutdownNow();
            clients.remove(target, context);

            removeInitializingClientDelayed(target);
        }

        @Override
        public void onCompleted() {
        }

    }

}
