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

import io.afloatdb.raft.proto.PingRequest;
import io.afloatdb.raft.proto.PingResponse;
import io.afloatdb.raft.proto.ProtoRaftMessage;
import io.afloatdb.raft.proto.ProtoRaftResponse;
import io.afloatdb.raft.proto.RaftMessageServiceGrpc.RaftMessageServiceImplBase;
import io.grpc.stub.StreamObserver;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.model.message.RaftMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.function.Supplier;

import static io.afloatdb.internal.di.AfloatDBModule.RAFT_NODE_SUPPLIER_KEY;
import static io.afloatdb.internal.raft.impl.RaftMessages.extract;

@Singleton
public class RaftMessageHandler
        extends RaftMessageServiceImplBase {

    private static Logger LOGGER = LoggerFactory.getLogger(RaftMessageHandler.class);

    private final RaftNode raftNode;
    private final RaftEndpoint localMember;

    @Inject
    public RaftMessageHandler(@Named(RAFT_NODE_SUPPLIER_KEY) Supplier<RaftNode> raftNodeSupplier) {
        this.raftNode = raftNodeSupplier.get();
        this.localMember = this.raftNode.getLocalEndpoint();
    }

    @Override
    public StreamObserver<ProtoRaftMessage> handle(StreamObserver<ProtoRaftResponse> responseObserver) {
        return new RaftMessageStreamObserver();
    }

    @Override
    public void ping(PingRequest request, StreamObserver<PingResponse> responseObserver) {
        responseObserver.onNext(PingResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    private class RaftMessageStreamObserver
            implements StreamObserver<ProtoRaftMessage> {

        private volatile RaftEndpoint sender;

        @Override
        public void onNext(ProtoRaftMessage proto) {
            RaftMessage message = extract(proto);
            if (sender == null) {
                sender = message.getSender();
            }

            LOGGER.debug("{} received {}", localMember.getId(), message);

            raftNode.handle(message);
        }

        @Override
        public void onError(Throwable t) {
            if (sender != null) {
                LOGGER.error(localMember.getId() + " failure on Raft RPC stream of " + sender.getId(), t);
            } else {
                LOGGER.error(localMember.getId() + " failure on Raft RPC stream.", t);
            }

        }

        @Override
        public void onCompleted() {
            if (sender != null) {
                LOGGER.debug("{} Raft RPC stream of {} completed.", localMember.getId(), sender.getId());
            } else {
                LOGGER.debug("{} Raft RPC stream completed.", localMember.getId());
            }
        }

    }

}
