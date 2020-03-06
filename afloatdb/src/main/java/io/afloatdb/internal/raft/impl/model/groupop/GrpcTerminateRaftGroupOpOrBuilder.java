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

package io.afloatdb.internal.raft.impl.model.groupop;

import io.afloatdb.raft.proto.ProtoTerminateRaftGroupOp;
import io.microraft.model.groupop.TerminateRaftGroupOp;

import javax.annotation.Nonnull;

public class GrpcTerminateRaftGroupOpOrBuilder
        implements TerminateRaftGroupOp, TerminateRaftGroupOp.TerminateRaftGroupOpBuilder {

    private final ProtoTerminateRaftGroupOp op;

    public GrpcTerminateRaftGroupOpOrBuilder() {
        this(ProtoTerminateRaftGroupOp.getDefaultInstance());
    }

    public GrpcTerminateRaftGroupOpOrBuilder(ProtoTerminateRaftGroupOp op) {
        this.op = op;
    }

    public ProtoTerminateRaftGroupOp getOp() {
        return op;
    }

    @Nonnull
    @Override
    public TerminateRaftGroupOp build() {
        return this;
    }

    @Override
    public String toString() {
        return "GrpcTerminateRaftGroupOp{}";
    }

}
