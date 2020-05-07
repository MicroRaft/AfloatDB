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

import io.afloatdb.internal.rpc.RaftRpcStub;
import io.afloatdb.internal.rpc.RaftRpcStubManager;
import io.microraft.RaftEndpoint;
import io.microraft.model.message.RaftMessage;
import io.microraft.transport.Transport;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class GrpcTransport
        implements Transport {

    private final RaftRpcStubManager raftRpcStubManager;

    @Inject
    public GrpcTransport(RaftRpcStubManager raftRpcStubManager) {
        this.raftRpcStubManager = raftRpcStubManager;
    }

    @Override
    public void send(@Nonnull RaftEndpoint target, @Nonnull RaftMessage message) {
        RaftRpcStub stub = raftRpcStubManager.getRpcStub(target);
        if (stub != null) {
            stub.send(message);
        }
    }

    @Override
    public boolean isReachable(@Nonnull RaftEndpoint endpoint) {
        return raftRpcStubManager.getRpcStub(endpoint) != null;
    }

}
