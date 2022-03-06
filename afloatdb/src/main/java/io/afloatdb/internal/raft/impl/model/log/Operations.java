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

package io.afloatdb.internal.raft.impl.model.log;

import io.afloatdb.internal.raft.impl.model.groupop.UpdateRaftGroupMembersOpOrBuilder;
import io.afloatdb.raft.proto.Operation;

public final class Operations {

    private Operations() {
    }

    public static Object unwrap(Operation operation) {
        if (operation.getOperationCase() == Operation.OperationCase.UPDATERAFTGROUPMEMBERSOP) {
            return new UpdateRaftGroupMembersOpOrBuilder(operation.getUpdateRaftGroupMembersOp());
        }

        return operation;
    }

    public static Operation wrap(Object operation) {
        if (operation instanceof Operation) {
            return (Operation) operation;
        }

        Operation.Builder builder = Operation.newBuilder();
        if (operation instanceof UpdateRaftGroupMembersOpOrBuilder) {
            builder.setUpdateRaftGroupMembersOp(((UpdateRaftGroupMembersOpOrBuilder) operation).getOp());
        } else {
            throw new IllegalArgumentException("Invalid operation: " + operation);
        }

        return builder.build();
    }

}
