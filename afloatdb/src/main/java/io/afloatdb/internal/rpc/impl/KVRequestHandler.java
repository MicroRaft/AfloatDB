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
import io.afloatdb.kv.proto.ClearResponse;
import io.afloatdb.kv.proto.ContainsRequest;
import io.afloatdb.kv.proto.ContainsResponse;
import io.afloatdb.kv.proto.DeleteRequest;
import io.afloatdb.kv.proto.DeleteResponse;
import io.afloatdb.kv.proto.GetRequest;
import io.afloatdb.kv.proto.GetResponse;
import io.afloatdb.kv.proto.KVRequestHandlerGrpc.KVRequestHandlerImplBase;
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
import io.afloatdb.raft.proto.Operation;
import io.afloatdb.raft.proto.OperationResponse;
import io.grpc.stub.StreamObserver;
import io.microraft.QueryPolicy;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.function.Function;

import static io.afloatdb.internal.utils.Exceptions.wrap;

@Singleton
public class KVRequestHandler
        extends KVRequestHandlerImplBase {

    private final InvocationService invocationService;

    @Inject
    public KVRequestHandler(InvocationService invocationService) {
        this.invocationService = invocationService;
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        replicate(Operation.newBuilder().setPutRequest(request).build(), OperationResponse::getPutResponse, responseObserver);
    }

    @Override
    public void set(SetRequest request, StreamObserver<SetResponse> responseObserver) {
        replicate(Operation.newBuilder().setSetRequest(request).build(), OperationResponse::getSetResponse, responseObserver);
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        query(Operation.newBuilder().setGetRequest(request).build(), OperationResponse::getGetResponse, responseObserver);
    }

    @Override
    public void contains(ContainsRequest request, StreamObserver<ContainsResponse> responseObserver) {
        query(Operation.newBuilder().setContainsRequest(request).build(), OperationResponse::getContainsResponse,
              responseObserver);
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        replicate(Operation.newBuilder().setDeleteRequest(request).build(), OperationResponse::getDeleteResponse,
                  responseObserver);
    }

    @Override
    public void remove(RemoveRequest request, StreamObserver<RemoveResponse> responseObserver) {
        replicate(Operation.newBuilder().setRemoveRequest(request).build(), OperationResponse::getRemoveResponse,
                  responseObserver);
    }

    @Override
    public void replace(ReplaceRequest request, StreamObserver<ReplaceResponse> responseObserver) {
        replicate(Operation.newBuilder().setReplaceRequest(request).build(), OperationResponse::getReplaceResponse,
                  responseObserver);
    }

    @Override
    public void size(SizeRequest request, StreamObserver<SizeResponse> responseObserver) {
        query(Operation.newBuilder().setSizeRequest(request).build(), OperationResponse::getSizeResponse, responseObserver);
    }

    @Override
    public void clear(ClearRequest request, StreamObserver<ClearResponse> responseObserver) {
        replicate(Operation.newBuilder().setClearRequest(request).build(), OperationResponse::getClearResponse, responseObserver);
    }

    private <T> void replicate(Operation request, Function<OperationResponse, T> responseExtractor,
                               StreamObserver<T> responseObserver) {
        invocationService.invoke(request).whenComplete((response, throwable) -> {
            // TODO [basri] bottleneck. offload to IO thread...
            if (throwable == null) {
                responseObserver.onNext(responseExtractor.apply(response.getResult()));
            } else {
                responseObserver.onError(wrap(throwable));
            }
            responseObserver.onCompleted();
        });
    }

    private <T> void query(Operation request, Function<OperationResponse, T> responseExtractor,
                           StreamObserver<T> responseObserver) {
        invocationService.query(request, QueryPolicy.LINEARIZABLE, 0).whenComplete((response, throwable) -> {
            // TODO [basri] bottleneck. offload to IO thread...
            if (throwable == null) {
                responseObserver.onNext(responseExtractor.apply(response.getResult()));
            } else {
                responseObserver.onError(wrap(throwable));
            }
            responseObserver.onCompleted();
        });
    }

}
