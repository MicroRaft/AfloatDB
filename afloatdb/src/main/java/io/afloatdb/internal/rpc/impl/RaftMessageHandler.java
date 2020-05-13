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
import io.afloatdb.kv.proto.ClearResponse;
import io.afloatdb.kv.proto.ContainsResponse;
import io.afloatdb.kv.proto.DeleteResponse;
import io.afloatdb.kv.proto.GetResponse;
import io.afloatdb.kv.proto.PutResponse;
import io.afloatdb.kv.proto.RemoveResponse;
import io.afloatdb.kv.proto.ReplaceResponse;
import io.afloatdb.kv.proto.SetResponse;
import io.afloatdb.kv.proto.SizeResponse;
import io.afloatdb.raft.proto.OperationResponse;
import io.afloatdb.raft.proto.QueryRequest;
import io.afloatdb.raft.proto.RaftMessageProto;
import io.afloatdb.raft.proto.RaftMessageServiceGrpc.RaftMessageServiceImplBase;
import io.afloatdb.raft.proto.RaftResponse;
import io.afloatdb.raft.proto.ReplicateRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.microraft.Ordered;
import io.microraft.QueryPolicy;
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
import static io.afloatdb.internal.utils.Exceptions.wrap;
import static io.afloatdb.internal.utils.Serialization.fromProto;
import static io.afloatdb.internal.utils.Serialization.unwrap;

@Singleton
public class RaftMessageHandler
        extends RaftMessageServiceImplBase {

    private static Logger LOGGER = LoggerFactory.getLogger(RaftMessageHandler.class);

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
    public void replicate(ReplicateRequest request, StreamObserver<OperationResponse> responseObserver) {
        raftNode.replicate(request.getOperation()).whenComplete((response, throwable) -> {
            if (throwable == null) {
                responseObserver.onNext(createProtoOperationResponse(response));
            } else {
                responseObserver.onError(wrap(throwable));
            }
            responseObserver.onCompleted();
        });
    }

    @Override
    public void query(QueryRequest request, StreamObserver<OperationResponse> responseObserver) {
        QueryPolicy queryPolicy = fromProto(request.getQueryPolicy());
        if (queryPolicy == null) {
            responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
            responseObserver.onCompleted();
            return;
        }

        raftNode.query(request.getOperation(), queryPolicy, request.getMinCommitIndex()).whenComplete((response, throwable) -> {
            if (throwable == null) {
                responseObserver.onNext(createProtoOperationResponse(response));
            } else {
                responseObserver.onError(wrap(throwable));
            }
            responseObserver.onCompleted();
        });
    }

    private OperationResponse createProtoOperationResponse(Ordered<Object> ordered) {
        OperationResponse.Builder builder = OperationResponse.newBuilder();
        builder.setCommitIndex(ordered.getCommitIndex());

        Object result = ordered.getResult();
        if (result instanceof PutResponse) {
            builder.setPutResponse((PutResponse) result);
        } else if (result instanceof SetResponse) {
            builder.setSetResponse((SetResponse) result);
        } else if (result instanceof GetResponse) {
            builder.setGetResponse((GetResponse) result);
        } else if (result instanceof ContainsResponse) {
            builder.setContainsResponse((ContainsResponse) result);
        } else if (result instanceof DeleteResponse) {
            builder.setDeleteResponse((DeleteResponse) result);
        } else if (result instanceof RemoveResponse) {
            builder.setRemoveResponse((RemoveResponse) result);
        } else if (result instanceof ReplaceResponse) {
            builder.setReplaceResponse((ReplaceResponse) result);
        } else if (result instanceof SizeResponse) {
            builder.setSizeResponse((SizeResponse) result);
        } else if (result instanceof ClearResponse) {
            builder.setClearResponse((ClearResponse) result);
        } else {
            throw new IllegalArgumentException("Invalid result: " + ordered);
        }

        return builder.build();
    }

    @Override
    public StreamObserver<RaftMessageProto> handle(StreamObserver<RaftResponse> responseObserver) {
        RaftMessageStreamObserver observer = new RaftMessageStreamObserver();
        streamObservers.add(observer);
        return observer;
    }

    private class RaftMessageStreamObserver
            implements StreamObserver<RaftMessageProto> {

        private volatile RaftEndpoint sender;

        @Override
        public void onNext(RaftMessageProto proto) {
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
