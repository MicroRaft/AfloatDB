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

package io.afloatdb.internal.raft.impl;

import io.afloatdb.AfloatDBException;
import io.afloatdb.config.AfloatDBConfig;
import io.afloatdb.internal.lifecycle.ProcessTerminationReporter;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.integration.RaftNodeRuntime;
import io.microraft.integration.StateMachine;
import io.microraft.model.RaftModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Collection;
import java.util.function.Supplier;

import static io.afloatdb.internal.di.AfloatDBModule.CONFIG_KEY;
import static io.afloatdb.internal.di.AfloatDBModule.INITIAL_ENDPOINTS_KEY;
import static io.afloatdb.internal.di.AfloatDBModule.LOCAL_ENDPOINT_KEY;

@Singleton
public class RaftNodeSupplier
        implements Supplier<RaftNode> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNodeSupplier.class);

    private final RaftNode raftNode;
    private final ProcessTerminationReporter processTerminationReporter;

    @Inject
    public RaftNodeSupplier(@Named(CONFIG_KEY) AfloatDBConfig config, @Named(LOCAL_ENDPOINT_KEY) RaftEndpoint localEndpoint,
                            @Named(INITIAL_ENDPOINTS_KEY) Collection<RaftEndpoint> initialGroupMembers, RaftNodeRuntime runtime,
                            StateMachine stateMachine, RaftModelFactory modelFactory,
                            ProcessTerminationReporter processTerminationReporter) {
        this.raftNode = RaftNode.newBuilder().setGroupId(config.getRaftGroupConfig().getId()).setLocalEndpoint(localEndpoint)
                                .setInitialGroupMembers(initialGroupMembers).setConfig(config.getRaftConfig()).setRuntime(runtime)
                                .setStateMachine(stateMachine).setModelFactory(modelFactory).build();
        this.processTerminationReporter = processTerminationReporter;
    }

    @PostConstruct
    public void start() {
        LOGGER.info("{} starting Raft node...", raftNode.getLocalEndpoint().getId());
        try {
            raftNode.start().join();
        } catch (Throwable t) {
            throw new AfloatDBException(raftNode.getLocalEndpoint().getId() + " could not start Raft node!", t);
        }
    }

    @PreDestroy
    public void shutdown() {
        if (processTerminationReporter.isCurrentProcessTerminating()) {
            System.err.println(raftNode.getLocalEndpoint().getId() + " terminating Raft node...");
        } else {
            LOGGER.info("{} terminating Raft node...", raftNode.getLocalEndpoint().getId());
        }

        try {
            raftNode.terminate().join();

            if (processTerminationReporter.isCurrentProcessTerminating()) {
                System.err.println(raftNode.getLocalEndpoint().getId() + " RaftNode is terminated.");
            } else {
                LOGGER.warn("{} Raft node is terminated.", raftNode.getLocalEndpoint().getId());
            }
        } catch (Throwable t) {
            String message = raftNode.getLocalEndpoint().getId() + " failure during termination of Raft node";
            if (processTerminationReporter.isCurrentProcessTerminating()) {
                System.err.println(message);
                t.printStackTrace(System.err);
            } else {
                LOGGER.error(message, t);
            }
        }
    }

    @Override
    public RaftNode get() {
        return raftNode;
    }

}
