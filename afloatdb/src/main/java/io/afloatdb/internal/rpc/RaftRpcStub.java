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

package io.afloatdb.internal.rpc;

import io.afloatdb.raft.proto.OperationResponse;
import io.afloatdb.raft.proto.QueryRequest;
import io.afloatdb.raft.proto.ReplicateRequest;
import io.grpc.stub.StreamObserver;
import io.microraft.model.message.RaftMessage;

import javax.annotation.Nonnull;

public interface RaftRpcStub {

    void send(@Nonnull RaftMessage message);

    void replicate(ReplicateRequest request, StreamObserver<OperationResponse> responseObserver);

    void query(QueryRequest request, StreamObserver<OperationResponse> responseObserver);

}
