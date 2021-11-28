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

import io.afloatdb.internal.invocation.InvocationService;
import io.afloatdb.kv.proto.ClearRequest;
import io.afloatdb.kv.proto.ContainsRequest;
import io.afloatdb.kv.proto.DeleteRequest;
import io.afloatdb.kv.proto.GetRequest;
import io.afloatdb.kv.proto.KVRequestHandlerGrpc.KVRequestHandlerImplBase;
import io.afloatdb.kv.proto.KVResponse;
import io.afloatdb.kv.proto.PutRequest;
import io.afloatdb.kv.proto.RemoveRequest;
import io.afloatdb.kv.proto.ReplaceRequest;
import io.afloatdb.kv.proto.SetRequest;
import io.afloatdb.kv.proto.SizeRequest;
import io.afloatdb.raft.proto.Operation;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;
import javax.inject.Singleton;

import static io.afloatdb.internal.utils.Exceptions.wrap;
import static io.microraft.QueryPolicy.EVENTUAL_CONSISTENCY;
import static io.microraft.QueryPolicy.LINEARIZABLE;

@Singleton
public class KVRequestHandler extends KVRequestHandlerImplBase {

    private final InvocationService invocationService;

    @Inject
    public KVRequestHandler(InvocationService invocationService) {
        this.invocationService = invocationService;
    }

    @Override
    public void put(PutRequest request, StreamObserver<KVResponse> responseObserver) {
        replicate(Operation.newBuilder().setPutRequest(request).build(), responseObserver);
    }

    @Override
    public void set(SetRequest request, StreamObserver<KVResponse> responseObserver) {
        replicate(Operation.newBuilder().setSetRequest(request).build(), responseObserver);
    }

    @Override
    public void get(GetRequest request, StreamObserver<KVResponse> responseObserver) {
        query(Operation.newBuilder().setGetRequest(request).build(), request.getMinCommitIndex(), responseObserver);
    }

    @Override
    public void contains(ContainsRequest request, StreamObserver<KVResponse> responseObserver) {
        query(Operation.newBuilder().setContainsRequest(request).build(), request.getMinCommitIndex(),
                responseObserver);
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<KVResponse> responseObserver) {
        replicate(Operation.newBuilder().setDeleteRequest(request).build(), responseObserver);
    }

    @Override
    public void remove(RemoveRequest request, StreamObserver<KVResponse> responseObserver) {
        replicate(Operation.newBuilder().setRemoveRequest(request).build(), responseObserver);
    }

    @Override
    public void replace(ReplaceRequest request, StreamObserver<KVResponse> responseObserver) {
        replicate(Operation.newBuilder().setReplaceRequest(request).build(), responseObserver);
    }

    @Override
    public void size(SizeRequest request, StreamObserver<KVResponse> responseObserver) {
        query(Operation.newBuilder().setSizeRequest(request).build(), request.getMinCommitIndex(), responseObserver);
    }

    @Override
    public void clear(ClearRequest request, StreamObserver<KVResponse> responseObserver) {
        replicate(Operation.newBuilder().setClearRequest(request).build(), responseObserver);
    }

    private void replicate(Operation request, StreamObserver<KVResponse> responseObserver) {
        invocationService.invoke(request).whenComplete((response, throwable) -> {
            // TODO [basri] bottleneck. offload to IO thread...
            if (throwable == null) {
                responseObserver.onNext(response.getResult());
            } else {
                responseObserver.onError(wrap(throwable));
            }
            responseObserver.onCompleted();
        });
    }

    private void query(Operation request, long minCommitIndex, StreamObserver<KVResponse> responseObserver) {
        invocationService.query(request, minCommitIndex == -1 ? LINEARIZABLE : EVENTUAL_CONSISTENCY, Math.max(0, minCommitIndex))
                .whenComplete((response, throwable) -> {
                    // TODO [basri] bottleneck. offload to IO thread...
                    if (throwable == null) {
                        responseObserver.onNext(response.getResult());
                    } else {
                        responseObserver.onError(wrap(throwable));
                    }
                    responseObserver.onCompleted();
                });
    }

}
