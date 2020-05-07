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

package io.afloatdb.internal;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.lifecycle.LifecycleManager;
import io.afloatdb.AfloatDB;
import io.afloatdb.AfloatDBException;
import io.afloatdb.config.AfloatDBConfig;
import io.afloatdb.config.AfloatDBEndpointConfig;
import io.afloatdb.internal.di.AfloatDBModule;
import io.afloatdb.internal.lifecycle.TerminationAware;
import io.afloatdb.internal.raft.RaftNodeReportObserver;
import io.afloatdb.internal.raft.impl.model.AfloatDBEndpoint;
import io.afloatdb.management.proto.AddRaftEndpointAddressRequest;
import io.afloatdb.management.proto.AddRaftEndpointRequest;
import io.afloatdb.management.proto.GetRaftNodeReportRequest;
import io.afloatdb.management.proto.GetRaftNodeReportResponse;
import io.afloatdb.management.proto.ManagementServiceGrpc;
import io.afloatdb.management.proto.ProtoRaftNodeReport;
import io.afloatdb.management.proto.ProtoRaftNodeStatus;
import io.afloatdb.raft.proto.ProtoRaftEndpoint;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.report.RaftNodeReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.inject.name.Names.named;
import static io.afloatdb.internal.di.AfloatDBModule.RAFT_NODE_SUPPLIER_KEY;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class AfloatDBImpl
        implements AfloatDB {

    private static final Logger LOGGER = LoggerFactory.getLogger(AfloatDB.class);

    private final AfloatDBConfig config;
    private final RaftEndpoint localEndpoint;
    private final Injector injector;
    private final LifecycleManager lifecycleManager;
    private final RaftNode raftNode;
    private final Supplier<RaftNodeReport> raftNodeReportSupplier;
    private final AtomicReference<Status> status = new AtomicReference<>(Status.LATENT);
    private final AtomicBoolean processTerminationFlag = new AtomicBoolean();
    private volatile boolean terminationCompleted;

    private AfloatDBImpl(AfloatDBConfig config, RaftEndpoint localEndpoint, List<RaftEndpoint> initialEndpoints,
                         Map<RaftEndpoint, String> endpointAddresses) {
        try {
            this.config = config;
            this.localEndpoint = localEndpoint;
            Module module = new AfloatDBModule(config, localEndpoint, initialEndpoints, endpointAddresses,
                    processTerminationFlag);
            this.injector = LifecycleInjector.builder().withModules(module).build().createInjector();
            this.lifecycleManager = injector.getInstance(LifecycleManager.class);

            lifecycleManager.start();
            status.set(Status.RUNNING);

            Supplier<RaftNode> raftNodeSupplier = injector.getInstance(Key.get(new TypeLiteral<Supplier<RaftNode>>() {
            }, named(RAFT_NODE_SUPPLIER_KEY)));
            this.raftNode = raftNodeSupplier.get();
            this.raftNodeReportSupplier = injector.getInstance(RaftNodeReportObserver.class);

            registerShutdownHook();
        } catch (Throwable t) {
            shutdown();
            throw new AfloatDBException("Could not start server!", t);
        }
    }

    private static ProtoRaftEndpoint toProtoRaftEndpoint(AfloatDBEndpointConfig endpointConfig) {
        return ProtoRaftEndpoint.newBuilder().setId(endpointConfig.getId()).build();
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            processTerminationFlag.set(true);

            if (!isShutdown()) {
                System.out.println(localEndpoint.getId() + " shutting down...");
            }

            shutdown();
        }));
    }

    @Nonnull
    @Override
    public AfloatDBConfig getConfig() {
        return config;
    }

    @Nonnull
    @Override
    public RaftEndpoint getLocalEndpoint() {
        return localEndpoint;
    }

    @Nonnull
    @Override
    public RaftNodeReport getRaftNodeReport() {
        return raftNodeReportSupplier.get();
    }

    @Override
    public void shutdown() {
        if (status.compareAndSet(Status.RUNNING, Status.SHUTTING_DOWN)) {
            try {
                lifecycleManager.close();
            } finally {
                status.set(Status.SHUT_DOWN);
            }
        } else {
            status.compareAndSet(Status.LATENT, Status.SHUT_DOWN);
        }
    }

    @Override
    public boolean isShutdown() {
        return status.get() == Status.SHUT_DOWN;
    }

    @Override
    public void awaitTermination() {
        if (terminationCompleted) {
            return;
        }

        injector.getAllBindings().values().stream().filter(binding -> binding.getProvider().get() instanceof TerminationAware)
                .map(binding -> (TerminationAware) binding.getProvider().get()).forEach(TerminationAware::awaitTermination);
        terminationCompleted = true;
    }

    public RaftNode getRaftNode() {
        return raftNode;
    }

    // Only for testing
    public Injector getInjector() {
        return injector;
    }

    private enum Status {
        LATENT, RUNNING, SHUTTING_DOWN, SHUT_DOWN
    }

    public static class AfloatDBBootstrapper
            implements Supplier<AfloatDBImpl> {
        final AfloatDBConfig config;

        public AfloatDBBootstrapper(AfloatDBConfig config) {
            this.config = config;
        }

        @Override
        public AfloatDBImpl get() {
            ProtoRaftEndpoint localEndpoint = toProtoRaftEndpoint(config.getLocalEndpointConfig());
            List<RaftEndpoint> initialEndpoints = getInitialEndpoints(config);
            Map<RaftEndpoint, String> endpointAddresses = getEndpointAddresses(config);
            return new AfloatDBImpl(config, AfloatDBEndpoint.wrap(localEndpoint), initialEndpoints, endpointAddresses);
        }

        private List<RaftEndpoint> getInitialEndpoints(AfloatDBConfig config) {
            List<RaftEndpoint> initialEndpoints = config.getRaftGroupConfig().getInitialEndpoints().stream()
                                                        .map(AfloatDBImpl::toProtoRaftEndpoint).map(AfloatDBEndpoint::wrap)
                                                        .collect(toList());
            if (initialEndpoints.size() < 2) {
                throw new AfloatDBException(
                        "Cannot bootstrap new AfloatDB cluster with " + initialEndpoints.size() + " endpoint!");
            }

            return initialEndpoints;
        }

        private Map<RaftEndpoint, String> getEndpointAddresses(AfloatDBConfig config) {
            return config.getRaftGroupConfig().getInitialEndpoints().stream()
                         .collect(toMap(c -> AfloatDBEndpoint.wrap(toProtoRaftEndpoint(c)), AfloatDBEndpointConfig::getAddress));
        }

    }

    public static class AfloatDBJoiner
            implements Supplier<AfloatDBImpl> {

        final AfloatDBConfig config;
        final List<RaftEndpoint> initialMembers = new ArrayList<>();
        final Map<RaftEndpoint, String> endpointAddresses = new HashMap<>();
        final ProtoRaftEndpoint localEndpoint;

        public AfloatDBJoiner(AfloatDBConfig config) {
            this.config = config;
            this.localEndpoint = toProtoRaftEndpoint(config.getLocalEndpointConfig());
        }

        public AfloatDBImpl get() {
            String joinAddress = config.getRaftGroupConfig().getJoinTo();
            if (joinAddress == null) {
                throw new AfloatDBException("Join address is missing!");
            }

            LOGGER.debug("{} joining via {}", localEndpoint.getId(), joinAddress);

            GetRaftNodeReportResponse reportResponse = getReport(joinAddress);

            verifyReport(joinAddress, reportResponse);

            if (reportResponse.getReport().getCommittedMembers().getMemberList().contains(localEndpoint)) {
                populateDBInitState(reportResponse);

                LOGGER.warn("{} already joined to the Raft group before. AfloatDB will be created with initial "
                        + "endpoints: {} and addresses: {}", localEndpoint.getId(), initialMembers, endpointAddresses);

                return new AfloatDBImpl(config, AfloatDBEndpoint.wrap(localEndpoint), initialMembers, endpointAddresses);
            }

            String localAddress = config.getLocalEndpointConfig().getAddress();
            AddRaftEndpointAddressRequest request = AddRaftEndpointAddressRequest.newBuilder().setEndpoint(localEndpoint)
                                                                                 .setAddress(localAddress).build();

            for (ProtoRaftEndpoint endpoint : reportResponse.getReport().getEffectiveMembers().getMemberList()) {
                String address = reportResponse.getEndpointAddressOrDefault(endpoint.getId(), null);
                broadcastLocalAddress(request, endpoint, address);
            }

            addRaftEndpoint(reportResponse);

            LOGGER.info("{} joined to the Raft group. AfloatDB is created with initial endpoints: {} and " + "addresses: {}",
                    localEndpoint.getId(), initialMembers, endpointAddresses);

            return new AfloatDBImpl(config, AfloatDBEndpoint.wrap(localEndpoint), initialMembers, endpointAddresses);
        }

        private GetRaftNodeReportResponse getReport(String joinAddress) {
            ManagedChannel reportChannel = createChannel(joinAddress);
            GetRaftNodeReportResponse reportResponse;
            try {
                reportResponse = ManagementServiceGrpc.newBlockingStub(reportChannel)
                                                      .getReport(GetRaftNodeReportRequest.getDefaultInstance());
            } finally {
                reportChannel.shutdownNow();
            }

            return reportResponse;
        }

        private ManagedChannel createChannel(String address) {
            return ManagedChannelBuilder.forTarget(address).usePlaintext().disableRetry().directExecutor().build();
        }

        private void verifyReport(String joinAddress, GetRaftNodeReportResponse reportResponse) {
            ProtoRaftNodeReport report = reportResponse.getReport();
            if (report.getStatus() != ProtoRaftNodeStatus.ACTIVE) {
                throw new AfloatDBException(
                        "Cannot join via " + joinAddress + " because the Raft node status is " + report.getStatus());
            }

            if (report.getEffectiveMembers().getLogIndex() != report.getCommittedMembers().getLogIndex()) {
                throw new AfloatDBException(
                        "Cannot join via " + joinAddress + " because there is another ongoing " + "membership change!");
            }

            for (ProtoRaftEndpoint endpoint : report.getEffectiveMembers().getMemberList()) {
                if (reportResponse.getEndpointAddressOrDefault(endpoint.getId(), null) == null) {
                    throw new AfloatDBException(
                            "Cannot join via " + joinAddress + " because the address of the Raft " + "endpoint: " + endpoint
                                    .getId() + " is not known!");
                }
            }

            if (report.getTerm().getLeaderEndpoint() == null) {
                throw new AfloatDBException("Cannot join via " + joinAddress + " because the Raft leader is not " + "known!");
            }
        }

        private void broadcastLocalAddress(AddRaftEndpointAddressRequest request, ProtoRaftEndpoint target,
                                           String targetAddress) {
            LOGGER.debug("{} adding local address: {} to {} at {}", localEndpoint.getId(), request.getAddress(), target,
                    targetAddress);
            ManagedChannel channel = createChannel(targetAddress);
            try {
                ManagementServiceGrpc.newBlockingStub(channel).addRaftEndpointAddress(request);
            } catch (Throwable t) {
                throw new AfloatDBException("Could not add Raft endpoint address to " + target + " at " + targetAddress, t);
            } finally {
                channel.shutdownNow();
            }
        }

        private void addRaftEndpoint(GetRaftNodeReportResponse reportResponse) {
            ProtoRaftNodeReport report = reportResponse.getReport();
            ProtoRaftEndpoint leaderEndpoint = report.getTerm().getLeaderEndpoint();
            String leaderAddress = reportResponse.getEndpointAddressOrDefault(leaderEndpoint.getId(), null);
            ManagedChannel leaderChannel = createChannel(leaderAddress);
            long groupMembersCommitIndex = report.getCommittedMembers().getLogIndex();

            LOGGER.info("{} adding Raft endpoint at group members commit index: {} via the Raft leader: {} at {}",
                    localEndpoint.getId(), groupMembersCommitIndex, leaderEndpoint.getId(), leaderAddress);

            AddRaftEndpointRequest request = AddRaftEndpointRequest.newBuilder().setEndpoint(localEndpoint)
                                                                   .setGroupMembersCommitIndex(groupMembersCommitIndex).build();
            try {
                ManagementServiceGrpc.newBlockingStub(leaderChannel).addRaftEndpoint(request);
            } catch (Throwable t) {
                throw new AfloatDBException(
                        localEndpoint.getId() + " failure during add Raft endpoint via the Raft " + "leader: " + leaderEndpoint
                                + " at " + leaderAddress, t);
            } finally {
                leaderChannel.shutdownNow();
            }

            populateDBInitState(reportResponse);
        }

        private void populateDBInitState(GetRaftNodeReportResponse reportResponse) {
            for (ProtoRaftEndpoint endpoint : reportResponse.getReport().getInitialMembers().getMemberList()) {
                initialMembers.add(AfloatDBEndpoint.wrap(endpoint));
            }

            for (ProtoRaftEndpoint endpoint : reportResponse.getReport().getEffectiveMembers().getMemberList()) {
                String address = reportResponse.getEndpointAddressOrDefault(endpoint.getId(), null);
                endpointAddresses.put(AfloatDBEndpoint.wrap(endpoint), address);
            }
        }

    }

}
