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
import io.afloatdb.raft.proto.ProtoRaftMessage;
import io.afloatdb.raft.proto.ProtoVoteResponse;
import io.microraft.RaftEndpoint;
import io.microraft.model.message.VoteResponse;
import io.microraft.model.message.VoteResponse.VoteResponseBuilder;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

public class GrpcVoteResponseOrBuilder
        implements VoteResponse, VoteResponseBuilder, GrpcRaftMessage {

    private ProtoVoteResponse.Builder builder;
    private ProtoVoteResponse response;
    private RaftEndpoint sender;

    public GrpcVoteResponseOrBuilder() {
        this.builder = ProtoVoteResponse.newBuilder();
    }

    public GrpcVoteResponseOrBuilder(ProtoVoteResponse response) {
        this.response = response;
        this.sender = AfloatDBEndpoint.wrap(response.getSender());
    }

    public ProtoVoteResponse getResponse() {
        return response;
    }

    @Nonnull
    @Override
    public VoteResponseBuilder setGroupId(@Nonnull Object groupId) {
        requireNonNull(groupId);
        builder.setGroupId((String) groupId);
        return this;
    }

    @Nonnull
    @Override
    public VoteResponseBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.setSender(AfloatDBEndpoint.extract(sender));
        this.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public VoteResponseBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public VoteResponseBuilder setGranted(boolean granted) {
        builder.setGranted(granted);
        return this;
    }

    @Nonnull
    @Override
    public VoteResponse build() {
        response = builder.build();
        builder = null;
        return this;
    }

    @Override
    public void populate(ProtoRaftMessage.Builder builder) {
        builder.setVoteResponse(response);
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "GrpcVoteResponseBuilder{builder=" + builder + "}";
        }

        return "GrpcVoteResponse{" + "groupId=" + getGroupId() + ", sender=" + sender.getId() + ", term=" + getTerm()
                + ", granted=" + isGranted() + '}';
    }

    @Override
    public boolean isGranted() {
        return response.getGranted();
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
