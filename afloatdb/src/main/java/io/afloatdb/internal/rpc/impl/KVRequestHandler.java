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

import io.afloatdb.kv.proto.ClearRequest;
import io.afloatdb.kv.proto.ClearResponse;
import io.afloatdb.kv.proto.ContainsRequest;
import io.afloatdb.kv.proto.ContainsResponse;
import io.afloatdb.kv.proto.DeleteRequest;
import io.afloatdb.kv.proto.DeleteResponse;
import io.afloatdb.kv.proto.GetRequest;
import io.afloatdb.kv.proto.GetResponse;
import io.afloatdb.kv.proto.KVServiceGrpc.KVServiceImplBase;
import io.afloatdb.kv.proto.PutRequest;
import io.afloatdb.kv.proto.PutResponse;
import io.afloatdb.kv.proto.RemoveRequest;
import io.afloatdb.kv.proto.RemoveResponse;
import io.afloatdb.kv.proto.ReplaceRequest;
import io.afloatdb.kv.proto.ReplaceResponse;
import io.afloatdb.kv.proto.SetRequest;
import io.afloatdb.kv.proto.SetResponse;
import io.afloatdb.kv.proto.SizeRequest;
import io.afloatdb.kv.proto.SizeResponse;
import io.afloatdb.raft.proto.ProtoOperation;
import io.grpc.stub.StreamObserver;
import io.microraft.Ordered;
import io.microraft.RaftNode;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static io.afloatdb.internal.di.AfloatDBModule.RAFT_NODE_SUPPLIER_KEY;
import static io.afloatdb.internal.rpc.impl.RaftExceptionUtils.wrap;
import static io.microraft.QueryPolicy.LINEARIZABLE;

@Singleton
public class KVRequestHandler
        extends KVServiceImplBase {

    private final RaftNode raftNode;

    @Inject
    public KVRequestHandler(@Named(RAFT_NODE_SUPPLIER_KEY) Supplier<RaftNode> raftNodeSupplier) {
        this.raftNode = raftNodeSupplier.get();
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        replicate(ProtoOperation.newBuilder().setPutRequest(request).build(), responseObserver);
    }

    @Override
    public void set(SetRequest request, StreamObserver<SetResponse> responseObserver) {
        replicate(ProtoOperation.newBuilder().setSetRequest(request).build(), responseObserver);
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        query(ProtoOperation.newBuilder().setGetRequest(request).build(), responseObserver);
    }

    @Override
    public void contains(ContainsRequest request, StreamObserver<ContainsResponse> responseObserver) {
        query(ProtoOperation.newBuilder().setContainsRequest(request).build(), responseObserver);
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        replicate(ProtoOperation.newBuilder().setDeleteRequest(request).build(), responseObserver);
    }

    @Override
    public void remove(RemoveRequest request, StreamObserver<RemoveResponse> responseObserver) {
        replicate(ProtoOperation.newBuilder().setRemoveRequest(request).build(), responseObserver);
    }

    @Override
    public void replace(ReplaceRequest request, StreamObserver<ReplaceResponse> responseObserver) {
        replicate(ProtoOperation.newBuilder().setReplaceRequest(request).build(), responseObserver);
    }

    @Override
    public void size(SizeRequest request, StreamObserver<SizeResponse> responseObserver) {
        query(ProtoOperation.newBuilder().setSizeRequest(request).build(), responseObserver);
    }

    @Override
    public void clear(ClearRequest request, StreamObserver<ClearResponse> responseObserver) {
        replicate(ProtoOperation.newBuilder().setClearRequest(request).build(), responseObserver);
    }

    private <T> void query(Object request, StreamObserver<T> responseObserver) {
        // TODO [basri] add timeout
        raftNode.<T>query(request, LINEARIZABLE, 0).whenComplete(new RaftResultConsumer<>(responseObserver));
    }

    private <T> void replicate(Object request, StreamObserver<T> responseObserver) {
        // TODO [basri] add timeout
        raftNode.<T>replicate(request).whenComplete(new RaftResultConsumer<>(responseObserver));
    }

    static class RaftResultConsumer<T>
            implements BiConsumer<Ordered<T>, Throwable> {

        final StreamObserver<T> responseObserver;

        RaftResultConsumer(StreamObserver<T> responseObserver) {
            this.responseObserver = responseObserver;
        }

        @Override
        public void accept(Ordered<T> response, Throwable throwable) {
            if (throwable == null) {
                responseObserver.onNext(response.getResult());
            } else {
                responseObserver.onError(wrap(throwable));
            }
            responseObserver.onCompleted();
        }

    }

}
