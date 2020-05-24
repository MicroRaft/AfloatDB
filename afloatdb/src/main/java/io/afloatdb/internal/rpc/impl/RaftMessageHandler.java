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
import io.afloatdb.raft.proto.RaftMessageHandlerGrpc.RaftMessageHandlerImplBase;
import io.afloatdb.raft.proto.RaftMessageRequest;
import io.afloatdb.raft.proto.RaftMessageResponse;
import io.grpc.stub.StreamObserver;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.model.message.RaftMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static io.afloatdb.internal.di.AfloatDBModule.RAFT_NODE_SUPPLIER_KEY;
import static io.afloatdb.internal.utils.Serialization.unwrap;

@Singleton
public class RaftMessageHandler
        extends RaftMessageHandlerImplBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftMessageHandler.class);

    private final RaftNode raftNode;
    private final RaftEndpoint localEndpoint;
    private final ProcessTerminationLogger processTerminationLogger;
    private final Set<RaftMessageStreamObserver> streamObservers = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Inject
    public RaftMessageHandler(@Named(RAFT_NODE_SUPPLIER_KEY) Supplier<RaftNode> raftNodeSupplier,
                              ProcessTerminationLogger processTerminationLogger) {
        this.raftNode = raftNodeSupplier.get();
        this.localEndpoint = this.raftNode.getLocalEndpoint();
        this.processTerminationLogger = processTerminationLogger;
    }

    @PreDestroy
    public void shutdown() {
        streamObservers.forEach(RaftMessageStreamObserver::onCompleted);
        streamObservers.clear();

        processTerminationLogger.logInfo(LOGGER, localEndpoint.getId() + " RaftMessageHandler is shut down.");
    }

    @Override
    public StreamObserver<RaftMessageRequest> handle(StreamObserver<RaftMessageResponse> responseObserver) {
        RaftMessageStreamObserver observer = new RaftMessageStreamObserver();
        streamObservers.add(observer);
        return observer;
    }

    private class RaftMessageStreamObserver
            implements StreamObserver<RaftMessageRequest> {

        private volatile RaftEndpoint sender;

        @Override
        public void onNext(RaftMessageRequest proto) {
            RaftMessage message = unwrap(proto);
            if (sender == null) {
                sender = message.getSender();
            }

            LOGGER.debug("{} received {}.", localEndpoint.getId(), message);

            raftNode.handle(message);
        }

        @Override
        public void onError(Throwable t) {
            if (sender != null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.error(localEndpoint.getId() + " failure on Raft RPC stream of " + sender.getId(), t);
                } else {
                    LOGGER.error("{} failure on Raft RPC stream of {}. Exception: {} Message: {}", localEndpoint.getId(),
                            sender.getId(), t.getClass().getSimpleName(), t.getMessage());
                }
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.error(localEndpoint.getId() + " failure on Raft RPC stream.", t);
                } else {
                    LOGGER.error("{} failure on Raft RPC stream. Exception: {} Message: {}", localEndpoint.getId(),
                            t.getClass().getSimpleName(), t.getMessage());
                }
            }

        }

        @Override
        public void onCompleted() {
            LOGGER.debug("{} Raft RPC stream of {} completed.", localEndpoint.getId(), sender != null ? sender.getId() : null);
            streamObservers.remove(this);
        }

    }

}
