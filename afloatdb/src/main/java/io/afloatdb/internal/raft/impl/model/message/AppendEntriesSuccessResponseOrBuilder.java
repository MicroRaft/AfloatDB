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
import io.afloatdb.raft.proto.AppendEntriesSuccessResponseProto;
import io.afloatdb.raft.proto.RaftMessageProto;
import io.microraft.RaftEndpoint;
import io.microraft.model.message.AppendEntriesSuccessResponse;

import javax.annotation.Nonnull;

public class AppendEntriesSuccessResponseOrBuilder
        implements AppendEntriesSuccessResponse, AppendEntriesSuccessResponse.AppendEntriesSuccessResponseBuilder,
                   RaftMessageProtoAware {

    private AppendEntriesSuccessResponseProto.Builder builder;
    private AppendEntriesSuccessResponseProto response;
    private RaftEndpoint sender;

    public AppendEntriesSuccessResponseOrBuilder() {
        this.builder = AppendEntriesSuccessResponseProto.newBuilder();
    }

    public AppendEntriesSuccessResponseOrBuilder(AppendEntriesSuccessResponseProto response) {
        this.response = response;
        this.sender = AfloatDBEndpoint.wrap(response.getSender());
    }

    public AppendEntriesSuccessResponseProto getResponse() {
        return response;
    }

    @Nonnull
    @Override
    public AppendEntriesSuccessResponseBuilder setGroupId(@Nonnull Object groupId) {
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
    public AppendEntriesSuccessResponseBuilder setQuerySeqNo(long querySeqNo) {
        builder.setQuerySeqNo(querySeqNo);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesSuccessResponseBuilder setFlowControlSeqNo(long flowControlSeqNo) {
        builder.setFlowControlSeqNo(flowControlSeqNo);
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
    public void populate(RaftMessageProto.Builder builder) {
        builder.setAppendEntriesSuccessResponse(response);
    }

    @Override
    public long getLastLogIndex() {
        return response.getLastLogIndex();
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

        return "GrpcAppendEntriesSuccessResponse{" + "groupId=" + getGroupId() + ", sender=" + sender.getId() + ", " + "term="
                + getTerm() + ", lastLogIndex=" + getLastLogIndex() + ", querySeqNo=" + getQuerySeqNo() + ", flowControlSeqNo="
                + getFlowControlSeqNo() + '}';
    }

}
