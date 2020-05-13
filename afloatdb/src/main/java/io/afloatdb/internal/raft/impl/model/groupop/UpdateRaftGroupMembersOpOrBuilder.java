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

package io.afloatdb.internal.raft.impl.model.groupop;

import io.afloatdb.AfloatDBException;
import io.afloatdb.internal.raft.impl.model.AfloatDBEndpoint;
import io.afloatdb.raft.proto.UpdateRaftGroupMembersOpProto;
import io.afloatdb.raft.proto.UpdateRaftGroupMembersOpProto.MembershipChangeModeProto;
import io.microraft.MembershipChangeMode;
import io.microraft.RaftEndpoint;
import io.microraft.model.groupop.UpdateRaftGroupMembersOp;
import io.microraft.model.groupop.UpdateRaftGroupMembersOp.UpdateRaftGroupMembersOpBuilder;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

public class UpdateRaftGroupMembersOpOrBuilder
        implements UpdateRaftGroupMembersOp, UpdateRaftGroupMembersOpBuilder {

    private UpdateRaftGroupMembersOpProto.Builder builder;
    private UpdateRaftGroupMembersOpProto op;
    private Collection<RaftEndpoint> members;
    private RaftEndpoint endpoint;

    public UpdateRaftGroupMembersOpOrBuilder() {
        this.builder = UpdateRaftGroupMembersOpProto.newBuilder();
    }

    public UpdateRaftGroupMembersOpOrBuilder(UpdateRaftGroupMembersOpProto op) {
        this.op = op;
        this.members = new LinkedHashSet<>();
        op.getGroupMemberList().stream().map(AfloatDBEndpoint::wrap).forEach(members::add);
        this.endpoint = AfloatDBEndpoint.wrap(op.getEndpoint());
    }

    public UpdateRaftGroupMembersOpProto getOp() {
        return op;
    }

    @Nonnull
    @Override
    public Collection<RaftEndpoint> getMembers() {
        return members;
    }

    @Nonnull
    @Override
    public RaftEndpoint getEndpoint() {
        return endpoint;
    }

    @Nonnull
    @Override
    public MembershipChangeMode getMode() {
        if (op.getMode() == MembershipChangeModeProto.ADD) {
            return MembershipChangeMode.ADD;
        } else if (op.getMode() == MembershipChangeModeProto.REMOVE) {
            return MembershipChangeMode.REMOVE;
        }
        throw new IllegalStateException();
    }

    @Nonnull
    @Override
    public UpdateRaftGroupMembersOpBuilder setMembers(@Nonnull Collection<RaftEndpoint> members) {
        members.stream().map(AfloatDBEndpoint::extract).forEach(builder::addGroupMember);
        this.members = members;
        return this;
    }

    @Nonnull
    @Override
    public UpdateRaftGroupMembersOpBuilder setEndpoint(@Nonnull RaftEndpoint endpoint) {
        builder.setEndpoint(AfloatDBEndpoint.extract(endpoint));
        this.endpoint = endpoint;
        return this;
    }

    @Nonnull
    @Override
    public UpdateRaftGroupMembersOpBuilder setMode(@Nonnull MembershipChangeMode mode) {
        if (mode == MembershipChangeMode.ADD) {
            builder.setMode(MembershipChangeModeProto.ADD);
            return this;
        } else if (mode == MembershipChangeMode.REMOVE) {
            builder.setMode(MembershipChangeModeProto.REMOVE);
            return this;
        }

        throw new AfloatDBException("Invalid mode: " + mode);
    }

    @Nonnull
    @Override
    public UpdateRaftGroupMembersOp build() {
        op = builder.build();
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "GrpcUpdateRaftGroupMembersOpBuilder{builder=" + builder + "}";
        }

        List<Object> memberIds = members.stream().map(RaftEndpoint::getId).collect(Collectors.toList());
        return "GrpcUpdateRaftGroupMembersOp{" + "members=" + memberIds + ", endpoint=" + endpoint.getId() + ", " + "modee="
                + getMode() + '}';
    }

}
