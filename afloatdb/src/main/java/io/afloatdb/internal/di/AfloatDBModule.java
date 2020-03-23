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

package io.afloatdb.internal.di;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import io.afloatdb.config.AfloatDBConfig;
import io.afloatdb.internal.invocation.RaftInvocationManager;
import io.afloatdb.internal.invocation.impl.RaftInvocationManagerImpl;
import io.afloatdb.internal.lifecycle.ProcessTerminationLogger;
import io.afloatdb.internal.lifecycle.impl.ProcessTerminationLoggerImpl;
import io.afloatdb.internal.raft.RaftNodeReportObserver;
import io.afloatdb.internal.raft.impl.AfloatDBClusterEndpointsPublisher;
import io.afloatdb.internal.raft.impl.AfloatDBRaftNodeRuntime;
import io.afloatdb.internal.raft.impl.KVStoreStateMachine;
import io.afloatdb.internal.raft.impl.RaftNodeSupplier;
import io.afloatdb.internal.raft.impl.RaftNodeThread;
import io.afloatdb.internal.raft.impl.model.GrpcRaftModelFactory;
import io.afloatdb.internal.rpc.RaftRpcStubManager;
import io.afloatdb.internal.rpc.RpcServer;
import io.afloatdb.internal.rpc.impl.KVRequestHandler;
import io.afloatdb.internal.rpc.impl.ManagementRequestHandler;
import io.afloatdb.internal.rpc.impl.RaftMessageHandler;
import io.afloatdb.internal.rpc.impl.RaftRpcStubManagerImpl;
import io.afloatdb.internal.rpc.impl.RpcServerImpl;
import io.afloatdb.kv.proto.KVServiceGrpc.KVServiceImplBase;
import io.afloatdb.management.proto.ManagementServiceGrpc.ManagementServiceImplBase;
import io.afloatdb.raft.proto.RaftMessageServiceGrpc.RaftMessageServiceImplBase;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.integration.RaftNodeRuntime;
import io.microraft.integration.StateMachine;
import io.microraft.model.RaftModelFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.google.inject.name.Names.named;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class AfloatDBModule
        extends AbstractModule {

    public static final String CONFIG_KEY = "Config";
    public static final String LOCAL_ENDPOINT_KEY = "LocalEndpoint";
    public static final String INITIAL_ENDPOINTS_KEY = "InitialEndpoints";
    public static final String RAFT_ENDPOINT_ADDRESSES_KEY = "RaftEndpointAddresses";
    public static final String RAFT_NODE_EXECUTOR_KEY = "RaftNodeExecutor";
    public static final String RAFT_NODE_SUPPLIER_KEY = "RaftNodeSupplier";
    public static final String PROCESS_TERMINATION_FLAG_KEY = "ProcessTerminationFlag";

    private final AfloatDBConfig config;
    private final RaftEndpoint localEndpoint;
    private final List<RaftEndpoint> initialEndpoints;
    private final Map<RaftEndpoint, String> addresses;
    private final AtomicBoolean processTerminationFlag;

    public AfloatDBModule(AfloatDBConfig config, RaftEndpoint localEndpoint, List<RaftEndpoint> initialEndpoints,
                          Map<RaftEndpoint, String> addresses, AtomicBoolean processTerminationFlag) {
        this.config = config;
        this.localEndpoint = localEndpoint;
        this.initialEndpoints = initialEndpoints;
        this.addresses = addresses;
        this.processTerminationFlag = processTerminationFlag;
    }

    @Override
    protected void configure() {
        ThreadGroup raftNodeThreadGroup = new ThreadGroup("RaftNodeThread");
        ScheduledExecutorService raftNodeExecutor = newSingleThreadScheduledExecutor(
                runnable -> new RaftNodeThread(raftNodeThreadGroup, runnable));
        bind(ScheduledExecutorService.class).annotatedWith(named(RAFT_NODE_EXECUTOR_KEY)).toInstance(raftNodeExecutor);

        bind(AtomicBoolean.class).annotatedWith(named(PROCESS_TERMINATION_FLAG_KEY)).toInstance(processTerminationFlag);
        bind(ProcessTerminationLogger.class).to(ProcessTerminationLoggerImpl.class);

        bind(AfloatDBConfig.class).annotatedWith(named(CONFIG_KEY)).toInstance(config);
        bind(RaftEndpoint.class).annotatedWith(named(LOCAL_ENDPOINT_KEY)).toInstance(localEndpoint);
        bind(new TypeLiteral<Collection<RaftEndpoint>>() {
        }).annotatedWith(named(INITIAL_ENDPOINTS_KEY)).toInstance(initialEndpoints);
        bind(new TypeLiteral<Map<RaftEndpoint, String>>() {
        }).annotatedWith(named(RAFT_ENDPOINT_ADDRESSES_KEY)).toInstance(addresses);

        bind(RaftNodeRuntime.class).to(AfloatDBRaftNodeRuntime.class);
        bind(StateMachine.class).to(KVStoreStateMachine.class);
        bind(RaftModelFactory.class).to(GrpcRaftModelFactory.class);
        bind(RaftMessageServiceImplBase.class).to(RaftMessageHandler.class);
        bind(RpcServer.class).to(RpcServerImpl.class);
        bind(RaftRpcStubManager.class).to(RaftRpcStubManagerImpl.class);
        bind(KVServiceImplBase.class).to(KVRequestHandler.class);
        bind(ManagementServiceImplBase.class).to(ManagementRequestHandler.class);
        bind(RaftNodeReportObserver.class).to(AfloatDBClusterEndpointsPublisher.class);
        bind(RaftInvocationManager.class).to(RaftInvocationManagerImpl.class);

        bind(new TypeLiteral<Supplier<RaftNode>>() {
        }).annotatedWith(named(RAFT_NODE_SUPPLIER_KEY)).to(RaftNodeSupplier.class);
    }

}
