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
import io.afloatdb.raft.proto.ProtoAppendEntriesFailureResponse;
import io.afloatdb.raft.proto.ProtoRaftMessage;
import io.microraft.RaftEndpoint;
import io.microraft.model.message.AppendEntriesFailureResponse;
import io.microraft.model.message.AppendEntriesFailureResponse.AppendEntriesFailureResponseBuilder;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

public class GrpcAppendEntriesFailureResponseOrBuilder
        implements AppendEntriesFailureResponse, AppendEntriesFailureResponseBuilder, GrpcRaftMessage {

    private ProtoAppendEntriesFailureResponse.Builder builder;
    private ProtoAppendEntriesFailureResponse response;
    private RaftEndpoint sender;

    public GrpcAppendEntriesFailureResponseOrBuilder() {
        this.builder = ProtoAppendEntriesFailureResponse.newBuilder();
    }

    public GrpcAppendEntriesFailureResponseOrBuilder(ProtoAppendEntriesFailureResponse response) {
        this.response = response;
        this.sender = AfloatDBEndpoint.wrap(response.getSender());
    }

    public ProtoAppendEntriesFailureResponse getResponse() {
        return response;
    }

    @Override
    public void populate(ProtoRaftMessage.Builder builder) {
        builder.setAppendEntriesFailureResponse(response);
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setGroupId(@Nonnull Object groupId) {
        requireNonNull(groupId);
        builder.setGroupId((String) groupId);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.setSender(AfloatDBEndpoint.extract(sender));
        this.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setExpectedNextIndex(long expectedNextIndex) {
        builder.setExpectedNextIndex(expectedNextIndex);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setQuerySeqNo(long querySeqNo) {
        builder.setQuerySeqNo(querySeqNo);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setFlowControlSeqNo(long flowControlSeqNo) {
        builder.setFlowControlSeqNo(flowControlSeqNo);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponse build() {
        response = builder.build();
        builder = null;
        return this;
    }

    @Override
    public long getExpectedNextIndex() {
        return response.getExpectedNextIndex();
    }

    @Override
    public long getQuerySeqNo() {
        return response.getQuerySeqNo();
    }

    @Override
    public long getFlowControlSeqNo() {
        return response.getFlowControlSeqNo();
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

    @Override
    public String toString() {
        if (builder != null) {
            return "GrpcAppendEntriesFailureResponseBuilder{builder=" + builder + "}";
        }

        return "GrpcAppendEntriesFailureResponse{" + "groupId=" + getGroupId() + ", sender=" + sender.getId() + ", " + "term="
                + getTerm() + ", expectedNextIndex=" + getExpectedNextIndex() + ", querySeqNo=" + getQuerySeqNo()
                + ", flowControlSeqNo=" + getFlowControlSeqNo() + '}';
    }

}
