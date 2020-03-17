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

import io.afloatdb.internal.invocation.RaftInvocationManager;
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
import io.microraft.QueryPolicy;

import javax.inject.Inject;
import javax.inject.Singleton;

import static io.afloatdb.internal.utils.Exceptions.wrap;

@Singleton
public class KVRequestHandler
        extends KVServiceImplBase {

    private final RaftInvocationManager raftInvocationManager;

    @Inject
    public KVRequestHandler(RaftInvocationManager raftInvocationManager) {
        this.raftInvocationManager = raftInvocationManager;
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

    private <T> void replicate(ProtoOperation request, StreamObserver<T> responseObserver) {
        raftInvocationManager.<T>invoke(request).whenComplete((response, throwable) -> {
            if (throwable == null) {
                responseObserver.onNext(response.getResult());
            } else {
                responseObserver.onError(wrap(throwable));
            }
            responseObserver.onCompleted();
        });
    }

    private <T> void query(ProtoOperation request, StreamObserver<T> responseObserver) {
        raftInvocationManager.<T>query(request, QueryPolicy.LINEARIZABLE, 0).whenComplete((response, throwable) -> {
            if (throwable == null) {
                responseObserver.onNext(response.getResult());
            } else {
                responseObserver.onError(wrap(throwable));
            }
            responseObserver.onCompleted();
        });
    }

}
