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
import io.afloatdb.internal.raft.impl.model.log.GrpcSnapshotChunkOrBuilder;
import io.afloatdb.raft.proto.ProtoInstallSnapshotRequest;
import io.afloatdb.raft.proto.ProtoRaftMessage;
import io.microraft.RaftEndpoint;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.message.InstallSnapshotRequest;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class GrpcInstallSnapshotRequestOrBuilder
        implements InstallSnapshotRequest, InstallSnapshotRequest.InstallSnapshotRequestBuilder, GrpcRaftMessage {

    private ProtoInstallSnapshotRequest.Builder builder;
    private ProtoInstallSnapshotRequest request;
    private RaftEndpoint sender;
    private List<SnapshotChunk> snapshotChunks = new ArrayList<>();
    private Collection<RaftEndpoint> groupMembers = new LinkedHashSet<>();

    public GrpcInstallSnapshotRequestOrBuilder() {
        this.builder = ProtoInstallSnapshotRequest.newBuilder();
    }

    public GrpcInstallSnapshotRequestOrBuilder(ProtoInstallSnapshotRequest request) {
        this.request = request;
        this.sender = AfloatDBEndpoint.wrap(request.getSender());
        this.snapshotChunks = request.getSnapshotChunkList().stream().map(GrpcSnapshotChunkOrBuilder::new).collect(toList());
        this.groupMembers = request.getGroupMemberList().stream().map(AfloatDBEndpoint::wrap).collect(toList());
    }

    public ProtoInstallSnapshotRequest getRequest() {
        return request;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setGroupId(@Nonnull Object groupId) {
        requireNonNull(groupId);
        builder.setGroupId((String) groupId);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.setSender(AfloatDBEndpoint.extract(sender));
        this.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setSenderLeader(boolean leader) {
        builder.setSenderLeader(leader);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setSnapshotTerm(int snapshotTerm) {
        builder.setSnapshotTerm(snapshotTerm);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setSnapshotIndex(long snapshotIndex) {
        builder.setSnapshotIndex(snapshotIndex);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setTotalSnapshotChunkCount(int totalSnapshotChunkCount) {
        builder.setTotalSnapshotChunkCount(totalSnapshotChunkCount);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setSnapshotChunks(@Nonnull List<SnapshotChunk> snapshotChunks) {
        requireNonNull(snapshotChunks);
        snapshotChunks.stream().map(snapshotChunk -> ((GrpcSnapshotChunkOrBuilder) snapshotChunk).getSnapshotChunk())
                      .forEach(builder::addSnapshotChunk);
        this.snapshotChunks = snapshotChunks;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setGroupMembersLogIndex(long groupMembersLogIndex) {
        builder.setGroupMembersLogIndex(groupMembersLogIndex);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setGroupMembers(@Nonnull Collection<RaftEndpoint> groupMembers) {
        requireNonNull(groupMembers);
        groupMembers.stream().map(AfloatDBEndpoint::extract).forEach(builder::addGroupMember);
        this.groupMembers = groupMembers;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setQuerySeqNo(long querySeqNo) {
        builder.setQuerySeqNo(querySeqNo);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setFlowControlSeqNo(long flowControlSeqNo) {
        builder.setFlowControlSeqNo(flowControlSeqNo);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequest build() {
        request = builder.build();
        builder = null;
        return this;
    }

    @Override
    public void populate(ProtoRaftMessage.Builder builder) {
        builder.setInstallSnapshotRequest(request);
    }

    @Override
    public boolean isSenderLeader() {
        return request.getSenderLeader();
    }

    @Override
    public int getSnapshotTerm() {
        return request.getSnapshotTerm();
    }

    @Override
    public long getSnapshotIndex() {
        return request.getSnapshotIndex();
    }

    @Override
    public int getTotalSnapshotChunkCount() {
        return request.getTotalSnapshotChunkCount();
    }

    @Nonnull
    @Override
    public List<SnapshotChunk> getSnapshotChunks() {
        return snapshotChunks;
    }

    @Override
    public long getGroupMembersLogIndex() {
        return request.getGroupMembersLogIndex();
    }

    @Nonnull
    @Override
    public Collection<RaftEndpoint> getGroupMembers() {
        return groupMembers;
    }

    @Override
    public long getQuerySeqNo() {
        return request.getQuerySeqNo();
    }

    @Override
    public long getFlowControlSeqNo() {
        return request.getFlowControlSeqNo();
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

    @Override
    public String toString() {
        if (builder != null) {
            return "GrpcInstallSnapshotRequestBuilder{builder=" + builder + "}";
        }

        return "GrpcInstallSnapshotRequest{" + "groupId=" + getGroupId() + ", sender=" + sender.getId() + ", term=" + getTerm()
                + ", senderLeader=" + isSenderLeader() + ", snapshotTerm=" + getSnapshotTerm() + ", snapshotIndex="
                + getSnapshotIndex() + ", chunkCount=" + getTotalSnapshotChunkCount() + ", snapshotChunks=" + getSnapshotChunks()
                + ", groupMembers=" + getGroupMembers() + ", groupMembersLogIndex=" + getGroupMembersLogIndex() + ", querySeqNo="
                + getQuerySeqNo() + ", flowControlSeqNo=" + getFlowControlSeqNo() + '}';
    }

}
