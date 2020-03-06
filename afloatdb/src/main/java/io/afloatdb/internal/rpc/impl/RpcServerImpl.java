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

import io.afloatdb.AfloatDBException;
import io.afloatdb.config.AfloatDBConfig;
import io.afloatdb.internal.lifecycle.ProcessTerminationReporter;
import io.afloatdb.internal.lifecycle.TerminationAware;
import io.afloatdb.internal.rpc.RpcServer;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.microraft.RaftEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;

import static io.afloatdb.internal.di.AfloatDBModule.CONFIG_KEY;
import static io.afloatdb.internal.di.AfloatDBModule.LOCAL_ENDPOINT_KEY;

@Singleton
public class RpcServerImpl
        implements RpcServer, TerminationAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcServer.class);

    private final RaftEndpoint localEndpoint;
    private final Server server;
    private final ProcessTerminationReporter processTerminationReporter;

    @Inject
    public RpcServerImpl(@Named(LOCAL_ENDPOINT_KEY) RaftEndpoint localEndpoint, @Named(CONFIG_KEY) AfloatDBConfig config,
                         KVRequestHandler requestHandler, RaftMessageHandler raftMessageHandler,
                         ProcessTerminationReporter processTerminationReporter,
                         ManagementRequestHandler managementRequestHandler) {
        this.localEndpoint = localEndpoint;
        this.server = NettyServerBuilder.forAddress(config.getLocalEndpointConfig().getSocketAddress()).addService(requestHandler)
                                        .addService(raftMessageHandler).addService(managementRequestHandler).directExecutor()
                                        .build();
        this.processTerminationReporter = processTerminationReporter;
    }

    @PostConstruct
    public void start() {
        try {
            server.start();
            LOGGER.info(localEndpoint.getId() + " RPC server started.");
        } catch (IOException e) {
            throw new AfloatDBException(localEndpoint.getId() + " RPC server start failed!", e);
        }
    }

    @PreDestroy
    public void shutdown() {
        if (processTerminationReporter.isCurrentProcessTerminating()) {
            System.err.println(localEndpoint.getId() + " terminating RPC server...");
        } else {
            LOGGER.info("{} terminating RPC server...", localEndpoint.getId());
        }

        try {
            server.shutdownNow();

            if (processTerminationReporter.isCurrentProcessTerminating()) {
                System.err.println(localEndpoint.getId() + " RPC server is shut down.");
            } else {
                LOGGER.warn("{} RPC server is shut down.", localEndpoint.getId());
            }
        } catch (Throwable t) {
            String message = localEndpoint.getId() + " failure during termination of RPC server";
            if (processTerminationReporter.isCurrentProcessTerminating()) {
                System.err.println(message);
                t.printStackTrace(System.err);
            } else {
                LOGGER.error(message, t);
            }
        }
    }

    @Override
    public void awaitTermination() {
        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            if (processTerminationReporter.isCurrentProcessTerminating()) {
                System.err.println(localEndpoint.getId() + " await termination of RPC server interrupted!");
            } else {
                LOGGER.warn("{} await termination of RPC server interrupted!", localEndpoint.getId());
            }
        }
    }

}
