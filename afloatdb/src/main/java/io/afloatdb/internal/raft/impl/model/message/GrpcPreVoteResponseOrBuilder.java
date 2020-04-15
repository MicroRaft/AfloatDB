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
import io.afloatdb.raft.proto.ProtoPreVoteResponse;
import io.afloatdb.raft.proto.ProtoRaftMessage;
import io.microraft.RaftEndpoint;
import io.microraft.model.message.PreVoteResponse;
import io.microraft.model.message.PreVoteResponse.PreVoteResponseBuilder;

import javax.annotation.Nonnull;

public class GrpcPreVoteResponseOrBuilder
        implements PreVoteResponse, PreVoteResponseBuilder, GrpcRaftMessage {

    private ProtoPreVoteResponse.Builder builder;
    private ProtoPreVoteResponse response;
    private RaftEndpoint sender;

    public GrpcPreVoteResponseOrBuilder() {
        this.builder = ProtoPreVoteResponse.newBuilder();
    }

    public GrpcPreVoteResponseOrBuilder(ProtoPreVoteResponse response) {
        this.response = response;
        this.sender = AfloatDBEndpoint.wrap(response.getSender());
    }

    public ProtoPreVoteResponse getResponse() {
        return response;
    }

    @Nonnull
    @Override
    public PreVoteResponseBuilder setGroupId(@Nonnull Object groupId) {
        builder.setGroupId((String) groupId);
        return this;
    }

    @Nonnull
    @Override
    public PreVoteResponseBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.setSender(AfloatDBEndpoint.extract(sender));
        this.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public PreVoteResponseBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public PreVoteResponseBuilder setGranted(boolean granted) {
        builder.setGranted(granted);
        return this;
    }

    @Nonnull
    @Override
    public PreVoteResponse build() {
        response = builder.build();
        builder = null;
        return this;
    }

    @Override
    public void populate(ProtoRaftMessage.Builder builder) {
        builder.setPreVoteResponse(response);
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "GrpcPreVoteResponseBuilder{builder=" + builder + "}";
        }

        return "GrpcPreVoteResponse{" + "groupId=" + getGroupId() + ", sender=" + sender.getId() + ", term=" + getTerm()
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
