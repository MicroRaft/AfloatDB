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
import io.afloatdb.internal.raft.impl.model.log.GrpcLogEntryOrBuilder;
import io.afloatdb.raft.proto.ProtoAppendEntriesRequest;
import io.afloatdb.raft.proto.ProtoRaftMessage;
import io.microraft.RaftEndpoint;
import io.microraft.model.log.LogEntry;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.model.message.AppendEntriesRequest.AppendEntriesRequestBuilder;

import javax.annotation.Nonnull;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class GrpcAppendEntriesRequestOrBuilder
        implements AppendEntriesRequest, AppendEntriesRequestBuilder, GrpcRaftMessage {

    private ProtoAppendEntriesRequest.Builder builder;
    private ProtoAppendEntriesRequest request;
    private RaftEndpoint sender;
    private List<LogEntry> logEntries;

    public GrpcAppendEntriesRequestOrBuilder() {
        builder = ProtoAppendEntriesRequest.newBuilder();
    }

    public GrpcAppendEntriesRequestOrBuilder(ProtoAppendEntriesRequest request) {
        this.request = request;
        this.sender = AfloatDBEndpoint.wrap(request.getSender());
        this.logEntries = request.getEntryList().stream().map(GrpcLogEntryOrBuilder::new).collect(toList());
    }

    public ProtoAppendEntriesRequest getRequest() {
        return request;
    }

    @Override
    public void populate(ProtoRaftMessage.Builder builder) {
        builder.setAppendEntriesRequest(request);
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setGroupId(@Nonnull Object groupId) {
        requireNonNull(groupId);
        builder.setGroupId((String) groupId);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.setSender(AfloatDBEndpoint.extract(sender));
        this.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setPreviousLogTerm(int previousLogTerm) {
        builder.setPrevLogTerm(previousLogTerm);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setPreviousLogIndex(long previousLogIndex) {
        builder.setPrevLogIndex(previousLogIndex);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setCommitIndex(long commitIndex) {
        builder.setCommitIndex(commitIndex);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setLogEntries(@Nonnull List<LogEntry> logEntries) {
        for (LogEntry entry : logEntries) {
            builder.addEntry(((GrpcLogEntryOrBuilder) entry).getEntry());
        }

        this.logEntries = logEntries;

        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setQueryRound(long queryRound) {
        builder.setQueryRound(queryRound);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequest build() {
        request = builder.build();
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "GrpcAppendEntriesFailureResponseBuilder{builder=" + builder + "}";
        }

        return "GrpcAppendEntriesRequest{" + "groupId=" + getGroupId() + ", sender=" + sender.getId() + ", term=" + getTerm()
                + ", commitIndex=" + getCommitIndex() + ", queryRound=" + getQueryRound() + ", " + "prevLogIndex="
                + getPreviousLogIndex() + ", prevLogTerm=" + getPreviousLogTerm() + ", entries=" + getLogEntries() + '}';
    }

    @Override
    public int getPreviousLogTerm() {
        return request.getPrevLogTerm();
    }

    @Override
    public long getPreviousLogIndex() {
        return request.getPrevLogIndex();
    }

    @Override
    public long getCommitIndex() {
        return request.getCommitIndex();
    }

    @Nonnull
    @Override
    public List<LogEntry> getLogEntries() {
        return logEntries;
    }

    @Override
    public long getQueryRound() {
        return request.getQueryRound();
    }

    @Override
    public Object getGroupId() {
        return request.getGroupId();
    }

    @Nonnull
    @Override
    public RaftEndpoint getSender() {
        return sender;
    }

    @Override
    public int getTerm() {
        return request.getTerm();
    }

}
