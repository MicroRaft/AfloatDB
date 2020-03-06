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

package io.afloatdb.internal.raft.impl.model.log;

import io.afloatdb.internal.raft.impl.model.groupop.GrpcTerminateRaftGroupOpOrBuilder;
import io.afloatdb.internal.raft.impl.model.groupop.GrpcUpdateRaftGroupMembersOpOrBuilder;
import io.afloatdb.raft.proto.ProtoOperation;

public final class ProtoOperations {

    private ProtoOperations() {
    }

    public static Object extract(ProtoOperation operation) {
        switch (operation.getOperationCase()) {
            case TERMINATERAFTGROUPOP:
                return new GrpcTerminateRaftGroupOpOrBuilder(operation.getTerminateRaftGroupOp());
            case UPDATERAFTGROUPMEMBERSOP:
                return new GrpcUpdateRaftGroupMembersOpOrBuilder(operation.getUpdateRaftGroupMembersOp());
            default:
                return operation;
        }
    }

    public static ProtoOperation wrap(Object operation) {
        if (operation instanceof ProtoOperation) {
            return (ProtoOperation) operation;
        }

        ProtoOperation.Builder protoBuilder = ProtoOperation.newBuilder();
        if (operation instanceof GrpcTerminateRaftGroupOpOrBuilder) {
            protoBuilder.setTerminateRaftGroupOp(((GrpcTerminateRaftGroupOpOrBuilder) operation).getOp());
        } else if (operation instanceof GrpcUpdateRaftGroupMembersOpOrBuilder) {
            protoBuilder.setUpdateRaftGroupMembersOp(((GrpcUpdateRaftGroupMembersOpOrBuilder) operation).getOp());
        } else {
            throw new IllegalArgumentException("Invalid operation: " + operation);
        }

        return protoBuilder.build();
    }

}
