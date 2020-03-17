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

package io.afloatdb.internal.raft.impl.model.message;

import io.afloatdb.internal.raft.impl.model.AfloatDBEndpoint;
import io.afloatdb.raft.proto.ProtoAppendEntriesSuccessResponse;
import io.afloatdb.raft.proto.ProtoRaftMessage;
import io.microraft.RaftEndpoint;
import io.microraft.model.message.AppendEntriesSuccessResponse;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

public class GrpcAppendEntriesSuccessResponseOrBuilder
        implements AppendEntriesSuccessResponse, AppendEntriesSuccessResponse.AppendEntriesSuccessResponseBuilder,
                   GrpcRaftMessage {

    private ProtoAppendEntriesSuccessResponse.Builder builder;
    private ProtoAppendEntriesSuccessResponse response;
    private RaftEndpoint sender;

    public GrpcAppendEntriesSuccessResponseOrBuilder() {
        this.builder = ProtoAppendEntriesSuccessResponse.newBuilder();
    }

    public GrpcAppendEntriesSuccessResponseOrBuilder(ProtoAppendEntriesSuccessResponse response) {
        this.response = response;
        this.sender = AfloatDBEndpoint.wrap(response.getSender());
    }

    public ProtoAppendEntriesSuccessResponse getResponse() {
        return response;
    }

    @Nonnull
    @Override
    public AppendEntriesSuccessResponseBuilder setGroupId(@Nonnull Object groupId) {
        requireNonNull(groupId);
        builder.setGroupId((String) groupId);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesSuccessResponseBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.setSender(AfloatDBEndpoint.extract(sender));
        this.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesSuccessResponseBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesSuccessResponseBuilder setLastLogIndex(long lastLogIndex) {
        builder.setLastLogIndex(lastLogIndex);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesSuccessResponseBuilder setQueryRound(long queryRound) {
        builder.setQueryRound(queryRound);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesSuccessResponse build() {
        response = builder.build();
        builder = null;
        return this;
    }

    @Override
    public void populate(ProtoRaftMessage.Builder builder) {
        builder.setAppendEntriesSuccessResponse(response);
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "GrpcAppendEntriesFailureResponseBuilder{builder=" + builder + "}";
        }

        return "GrpcAppendEntriesSuccessResponse{" + "groupId=" + getGroupId() + ", sender=" + sender.getId() + ", " + "term="
                + getTerm() + ", lastLogIndex=" + getLastLogIndex() + ", queryRound=" + getQueryRound() + '}';
    }

    @Override
    public long getLastLogIndex() {
        return response.getLastLogIndex();
    }

    @Override
    public long getQueryRound() {
        return response.getQueryRound();
    }

    @Override
    public Object getGroupId() {
        return response.getGroupId();
    }

    @Nonnull
    @Override
    public RaftEndpoint getSender() {
        return sender;
    }

    @Override
    public int getTerm() {
        return response.getTerm();
    }

}
